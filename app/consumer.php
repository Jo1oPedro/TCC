<?php

/**
 * CDC Consumer CNES - Streaming ETL Pipeline
 *
 * Este é o componente central do pipeline near real-time.
 *
 * Fluxo:
 *   Kafka (tópicos CDC CNES) → Transformação → Bronze → Silver → Métricas
 *
 * Uso:
 *   php consumer.php
 *   php consumer.php --verbose
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\DatabaseConnection;
use TCC\Pipeline\EventTransformer;
use TCC\Pipeline\GoldProcessor;
use TCC\Pipeline\LakehouseWriter;
use TCC\Pipeline\MetricsCollector;

// Configuração

$config = require __DIR__ . '/config.php';
$verbose = in_array('--verbose', $argv ?? []);
$goldInterval = 50; // processa gold a cada N eventos

echo "============================================================\n";
echo "TCC Lakehouse - CDC Consumer CNES (Streaming ETL)\n";
echo "Início: " . date('Y-m-d H:i:s') . "\n";
echo "============================================================\n\n";

// Inicializa componentes

// Conexão PostgreSQL (para métricas e camada gold)
$pg = DatabaseConnection::get('postgres', $config['postgres']);

// Writer do lakehouse (filesystem local)
$writer = new LakehouseWriter($config['lakehouse']['base_path']);

// Transformer de eventos CDC
$transformer = new EventTransformer();

// Coletor de métricas
$metrics = new MetricsCollector($pg, $config['metrics']['enabled']);

// Processador Gold (agrega silver → gold + PostgreSQL)
$goldProcessor = new GoldProcessor($pg, $writer, $config['lakehouse']['base_path']);

//  consumidor Kafka
$kafkaConf = new RdKafka\Conf();
$kafkaConf->set('group.id', $config['kafka']['group_id']);
$kafkaConf->set('metadata.broker.list', $config['kafka']['brokers']);
$kafkaConf->set('auto.offset.reset', $config['kafka']['auto_offset_reset']);
$kafkaConf->set('enable.auto.commit', 'true');
$kafkaConf->set('auto.commit.interval.ms', '1000');

// Callback de rebalanceamento (útil para logs)
$kafkaConf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "[Kafka] Partições atribuídas: " . count($partitions) . "\n";
            $kafka->assign($partitions);
            break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "[Kafka] Partições revogadas.\n";
            $kafka->assign(null);
            break;
        default:
            echo "[Kafka] Erro de rebalanceamento: {$err}\n";
    }
});

$consumer = new RdKafka\KafkaConsumer($kafkaConf);
$consumer->subscribe($config['kafka']['topics']);

echo "[Kafka] Inscrito nos tópicos:\n";
foreach ($config['kafka']['topics'] as $topic) {
    echo "- {$topic}\n";
}
echo "\n[Pipeline] Aguardando eventos CDC...\n\n";


// Contadores
$stats = [
    'total' => 0,
    'inserts' => 0,
    'snapshots' => 0,
    'updates' => 0,
    'deletes' => 0,
    'errors' => 0,
    'start_time' => microtime(true),
];

// Loop principal de consumo
while (true) {
    $message = $consumer->consume(5000); // timeout 5 segundos

    if ($message === null) continue;

    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            processEvent($message, $transformer, $writer, $metrics, $stats, $verbose);

            // Dispara processamento Gold a cada N eventos
            if ($stats['total'] > 0 && $stats['total'] % $goldInterval === 0) {
                echo "[Gold] Atualizando camada gold ({$stats['total']} eventos acumulados)...\n";
                try {
                    $results = $goldProcessor->processAll();
                    $stats['last_gold'] = $stats['total'];
                    echo "[Gold] estab_summary={$results['estab_summary']} | municipal_health={$results['municipal_health']} | workforce={$results['workforce']}\n";
                } catch (\Exception $e) {
                    echo "[Gold] ERRO: {$e->getMessage()}\n";
                }
            }
            break;

        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            // Fim da partição — normal, não é erro
            if ($verbose) echo "[Kafka] Fim da partição {$message->partition}\n";
            break;

        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            // Timeout — nenhum evento novo, imprime status periódico
            $elapsed = round(microtime(true) - $stats['start_time'], 1);
            if ($stats['total'] > 0) {
                echo "[Status] {$stats['total']} eventos processados em {$elapsed}s "
                   . "(I:{$stats['inserts']} S:{$stats['snapshots']} U:{$stats['updates']} D:{$stats['deletes']} E:{$stats['errors']})\n";

                // Dispara camada gold no período de inatividade se há eventos pedentes
                if(!isset($stats["last_gold"]) || $stats["total"] > $stats["last_gold"]) {
                    // flush ANTES do gold para garantir que metricas sejam persistidas
                    // mesmo que o gold falhe (problema pre-existente nao deve afetar metricas)
                    $metrics->flush();
                    echo "[Gold] Atualizando camada gold \n";
                    try {
                        $results = $goldProcessor->processAll();
                        echo "[Gold] estab_summary={$results['estab_summary']} | municipal_health={$results['municipal_health']} | workforce={$results['workforce']}\n";
                        $stats['last_gold'] = $stats['total'];
                    } catch (\Exception $e) {
                        echo "[Gold] ERRO: {$e->getMessage()}\n";
                    }
                }
            }
            break;

        default:
            echo "[Kafka] ERRO: {$message->errstr()} (code: {$message->err})\n";
            $stats['errors']++;
            break;
    }
}

// Processamento de evento individual
function processEvent(
    RdKafka\Message $message,
    EventTransformer $transformer,
    LakehouseWriter $writer,
    MetricsCollector $metrics,
    array &$stats,
    bool $verbose,
): void {
    $payload = json_decode($message->payload, true);

    if ($payload === null) {
        echo "[WARN] Payload inválido no offset {$message->offset}\n";
        $stats['errors']++;
        return;
    }

    // Extrai nome da tabela a partir do tópico
    $table = $transformer->extractTableName($message->topic_name);
    $operation = $payload['op'] ?? '?';

    // CAMADA BRONZE
    $bronze = $writer->writeBronze($table, $payload);

    // TRANSFORMAÇÃO + CAMADA SILVER
    $cleaned = $transformer->transform($table, $payload);

    $silverTs = null;
    if ($cleaned !== null) {
        $silver = $writer->writeSilver($table, $cleaned);
        $silverTs = $silver['timestamp'];
    }

    // MÉTRICAS
    $recordId = match($table) {
        'estabelecimentos' => $payload['after']['co_unidade'] ?? $payload['before']['co_unidade'] ?? null,
        'profissionais' => $payload['after']['co_profissional_sus'] ?? $payload['before']['co_profissional_sus'] ?? null,
        default => $payload['after']['id'] ?? $payload['before']['id'] ?? null,
    };

    // Decomposicao da latencia CDC (timestamps que o Debezium ja embute):
    //  - tsSource: commit no MySQL (binlog)                       - payload.source.ts_ms
    //  - tsDbz: processamento interno do conector Debezium     - payload.ts_ms
    //  - tsKc: publicacao no Kafka via SMT InsertField        - payload.ts_kafka_connect
    //  (pode vir null se a SMT nao preencher; tratado como ausente)
    $tsSource = $payload['source']['ts_ms'] ?? null;
    $tsDbz    = $payload['ts_ms'] ?? null;
    $tsKc     = $payload['ts_kafka_connect'] ?? null;
    if (is_string($tsKc)) {
        // SMT pode serializar como ISO; converte para ms desde epoch
        $tsKcInt = strtotime($tsKc);
        $tsKc = $tsKcInt !== false ? $tsKcInt * 1000 : null;
    }

    $metrics->record(
        table: $table,
        operation: $cleaned['_operation'] ?? 'UNKNOWN',
        recordId: $recordId,
        tsSource: $tsSource,
        tsDbz: $tsDbz,
        tsKc: $tsKc,
        tsBronze: $bronze['timestamp'],
        tsSilver: $silverTs,
    );

    // CONTADORES
    $stats['total']++;
    match ($operation) {
        'c' => $stats['inserts']++,
        'r' => $stats['snapshots']++,
        'u' => $stats['updates']++,
        'd' => $stats['deletes']++,
        default => null,
    };

    // LOG
    if ($verbose) {
        $opName = match($operation) {
            'c' => 'INSERT', 'r' => 'SNAPSHOT', 'u' => 'UPDATE', 'd' => 'DELETE',
            default => $operation,
        };
        echo sprintf(
            "[%s] %s.%s | op=%s | id=%s | latência_estimada=%sms\n",
            date('H:i:s'),
            $message->topic_name,
            $message->offset,
            $opName,
            $recordId ?? '?',
            $tsSource ? (int)(microtime(true) * 1000) - $tsSource : '?'
        );
    }
}
