<?php

/**
 * Executor de Experimentos do TCC - CNES
 *
 * Automatiza a execução dos cenários de teste:
 *   1. Limpa métricas anteriores
 *   2. Roda o cenário de carga por N segundos
 *   3. Aguarda o consumer processar tudo
 *   4. Dispara o processamento gold
 *   5. Coleta métricas e salva resultado
 *   6. Repete para cada cenário/repetição
 *
 * Uso:
 *   php run_experiment.php                          # Roda todos os cenários
 *   php run_experiment.php --scenario=low           # Roda só um cenário
 *   php run_experiment.php --repetitions=5          # 5 repetições (padrão; recomendação do orientador)
 *   php run_experiment.php --duration=120           # 120s por cenário
 *   php run_experiment.php --skip-reset             # Não limpa métricas entre cenários
 *
 * O consumer.php deve estar rodando em outro terminal
 *
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\DatabaseConnection;
use TCC\Pipeline\LakehouseWriter;
use TCC\Pipeline\GoldProcessor;

$config = require __DIR__ . '/config.php';

// Parse de argumentos
$targetScenario = null;
$repetitions = 5; // orientador: aumentar de 3 para 5 para reportar dispersao
$duration = 60;
$skipReset = in_array('--skip-reset', $argv ?? []);

foreach ($argv as $arg) {
    if (str_starts_with($arg, '--scenario=')) {
        $targetScenario = substr($arg, strlen('--scenario='));
    }
    if (str_starts_with($arg, '--repetitions=')) {
        $repetitions = (int) substr($arg, strlen('--repetitions='));
    }
    if (str_starts_with($arg, '--duration=')) {
        $duration = (int) substr($arg, strlen('--duration='));
    }
}

// Ordenados por taxa nominal crescente (op/s):
// low=1, mixed=5, moderate=10, rate25=25, rate40=40, rate60=60, rate80=80, burst=100
$scenarios = ['low', 'mixed', 'moderate', 'rate25', 'rate40', 'rate60', 'rate80', 'burst'];
if ($targetScenario !== null) {
    if (!in_array($targetScenario, $scenarios)) {
        echo "Cenário inválido: {$targetScenario}\n";
        echo "Disponíveis: " . implode(', ', $scenarios) . "\n";
        exit(1);
    }
    $scenarios = [$targetScenario];
}

echo "===\n";
echo "  TCC Lakehouse - Executor de Experimentos CNES\n";
echo "  Cenários: " . implode(', ', $scenarios) . "\n";
echo "  Repetições: {$repetitions}\n";
echo "  Duração por cenário: {$duration}s\n";
echo "  Início: " . date('Y-m-d H:i:s') . "\n";
echo "===\n\n";
echo " Certificar de que o consumer.php está rodando!\n\n";

// Conexões
$pg = DatabaseConnection::get('postgres', $config['postgres']);
$writer = new LakehouseWriter($config['lakehouse']['base_path']);
$goldProcessor = new GoldProcessor($pg, $writer, $config['lakehouse']['base_path']);

// Diretório de resultados
$resultsDir = '/app/logs/experiments';
if (!is_dir($resultsDir)) {
    mkdir($resultsDir, 0755, true);
}

$experimentId = date('Ymd_His');
$summaryFile = "{$resultsDir}/experiment_{$experimentId}_summary.csv";

// Cabeçalho do CSV de resumo
$csvHeader = [
    'experiment_id', 'scenario', 'repetition', 'duration_s',
    'total_events', 'events_per_second',
    'avg_latency_ms', 'p50_latency_ms', 'p95_latency_ms', 'p99_latency_ms',
    'min_latency_ms', 'max_latency_ms',
    'avg_dbz_ms', 'avg_publish_ms', 'avg_kafka_ms', 'avg_cdc_ms', 'avg_etl_ms',
    'inserts', 'updates', 'deletes',
    'gold_estab_summary', 'gold_municipal_health', 'gold_workforce',
    'timestamp'
];

file_put_contents($summaryFile, implode(',', $csvHeader) . "\n");

// Execução dos experimentos
$totalRuns = count($scenarios) * $repetitions;
$currentRun = 0;

foreach ($scenarios as $scenario) {
    for ($rep = 1; $rep <= $repetitions; $rep++) {
        $currentRun++;
        $runId = "{$scenario}_r{$rep}";

        echo "===\n";
        echo "  [{$currentRun}/{$totalRuns}] Cenário: {$scenario} | Repetição: {$rep}/{$repetitions}\n";
        echo "===\n";

        // 1. Reset de métricas
        if (!$skipReset) {
            echo "[1/5] Limpando métricas anteriores...\n";
            $pg->exec("DELETE FROM pipeline_metrics");
            echo "  ✅ Métricas limpas.\n";
        }

        // Marca o início no banco
        $startMark = microtime(true);
        $startTime = date('Y-m-d H:i:s');

        // 2. Executa gerador de carga
        echo "[2/5] Executando carga ({$scenario}, {$duration}s)...\n";

        // Roda o load_generator como processo separado
        $loadCmd = "php /app/load_generator.php --scenario={$scenario} --duration={$duration} 2>&1";
        $loadOutput = [];
        $loadReturn = 0;
        exec($loadCmd, $loadOutput, $loadReturn);

        // Imprime as últimas linhas do output
        $lastLines = array_slice($loadOutput, -5);
        foreach ($lastLines as $line) {
            echo "  {$line}\n";
        }

        if ($loadReturn !== 0) {
            echo " ❌ Erro na geração de carga!\n";
            continue;
        }

        // 3. Aguarda processamento do consumer
        echo "[3/5] Aguardando consumer processar eventos (15s)...\n";
        sleep(15);

        // 4. Processa camada gold
        echo "[4/5] Processando camada gold...\n";
        try {
            $goldResults = $goldProcessor->processAll();
            echo "  ✅ Gold: estab={$goldResults['estab_summary']} | municipal={$goldResults['municipal_health']} | workforce={$goldResults['workforce']}\n";
        } catch (\Exception $e) {
            echo "  ❌ Erro no gold: {$e->getMessage()}\n";
            $goldResults = ['estab_summary' => 0, 'municipal_health' => 0, 'workforce' => 0];
        }

        // 5. Coleta métricas
        echo "[5/5] Coletando métricas...\n";

        $metricsData = $pg->query("
            SELECT
                COUNT(*) as total_events,
                ROUND(AVG(latency_total_ms)) as avg_latency_ms,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_total_ms)) as p50_latency_ms,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_total_ms)) as p95_latency_ms,
                ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_total_ms)) as p99_latency_ms,
                MIN(latency_total_ms) as min_latency_ms,
                MAX(latency_total_ms) as max_latency_ms,
                ROUND(AVG(latency_dbz_ms)) as avg_dbz_ms,
                ROUND(AVG(latency_publish_ms)) as avg_publish_ms,
                ROUND(AVG(latency_kafka_ms)) as avg_kafka_ms,
                ROUND(AVG(latency_cdc_ms)) as avg_cdc_ms,
                ROUND(AVG(latency_etl_ms)) as avg_etl_ms
            FROM pipeline_metrics
            WHERE latency_total_ms IS NOT NULL
        ")->fetch();

        $opCounts = $pg->query("
            SELECT
                event_operation,
                COUNT(*) as cnt
            FROM pipeline_metrics
            GROUP BY event_operation
        ")->fetchAll(\PDO::FETCH_KEY_PAIR);

        $elapsedTime = round(microtime(true) - $startMark, 1);
        $totalEvents = (int) ($metricsData['total_events'] ?? 0);
        $eventsPerSecond = $elapsedTime > 0 ? round($totalEvents / $elapsedTime, 2) : 0;

        // Imprime resumo
        echo "\n  --- Resultado: {$runId} ---\n";
        echo "  Total eventos: {$totalEvents}\n";
        echo "  Eventos/segundo: {$eventsPerSecond}\n";
        echo "  Latência média: {$metricsData['avg_latency_ms']}ms\n";
        echo "  Latência P50: {$metricsData['p50_latency_ms']}ms\n";
        echo "  Latência P95: {$metricsData['p95_latency_ms']}ms\n";
        echo "  Latência P99: {$metricsData['p99_latency_ms']}ms\n";
        echo "  Latência máxima: {$metricsData['max_latency_ms']}ms\n";
        echo "  ---\n\n";

        // Salva no CSV
        $csvRow = [
            $experimentId,
            $scenario,
            $rep,
            $duration,
            $totalEvents,
            $eventsPerSecond,
            $metricsData['avg_latency_ms'] ?? 0,
            $metricsData['p50_latency_ms'] ?? 0,
            $metricsData['p95_latency_ms'] ?? 0,
            $metricsData['p99_latency_ms'] ?? 0,
            $metricsData['min_latency_ms'] ?? 0,
            $metricsData['max_latency_ms'] ?? 0,
            $metricsData['avg_dbz_ms'] ?? 0,
            $metricsData['avg_publish_ms'] ?? 0,
            $metricsData['avg_kafka_ms'] ?? 0,
            $metricsData['avg_cdc_ms'] ?? 0,
            $metricsData['avg_etl_ms'] ?? 0,
            $opCounts['INSERT'] ?? 0,
            $opCounts['UPDATE'] ?? 0,
            $opCounts['DELETE'] ?? 0,
            $goldResults['estab_summary'],
            $goldResults['municipal_health'],
            $goldResults['workforce'],
            $startTime,
        ];
        file_put_contents($summaryFile, implode(',', $csvRow) . "\n", FILE_APPEND);

        // Salva métricas detalhadas desta repetição
        $detailFile = "{$resultsDir}/experiment_{$experimentId}_{$runId}_detail.csv";
        exportDetailedMetrics($pg, $detailFile);

        echo "  📊 Resultados salvos em:\n";
        echo " {$summaryFile}\n";
        echo " {$detailFile}\n\n";

        // Pausa entre repetições
        if ($rep < $repetitions || $scenario !== end($scenarios)) {
            echo "  Pausa de 10s antes do próximo cenário...\n\n";
            sleep(10);
        }
    }
}

// Relatório final consolidado
echo "\n===\n";
echo "  EXPERIMENTO CONCLUÍDO\n";
echo "===\n";
echo "  ID: {$experimentId}\n";
echo "  Cenários executados: " . implode(', ', $scenarios) . "\n";
echo "  Repetições por cenário: {$repetitions}\n";
echo "  Arquivo de resumo: {$summaryFile}\n";
echo "===\n\n";

// Imprime tabela consolidada (com dispersao entre repeticoes)
echo "--- Resultados Consolidados (mean +/- std, [min..max] das repeticoes) ---\n\n";
echo sprintf(
    "  %-10s %10s %22s %22s %22s\n",
    'Cenário', 'EvtsMean', 'Avg(ms) mean+/-sd[min,max]', 'P95(ms) mean+/-sd[min,max]', 'Max(ms) mean+/-sd[min,max]'
);
echo "  " . str_repeat('-', 100) . "\n";

$csvContent = file_get_contents($summaryFile);
$lines = explode("\n", trim($csvContent));
array_shift($lines); // remove header

$grouped = [];
foreach ($lines as $line) {
    $cols = str_getcsv($line);
    if (count($cols) < 12) continue;
    $scn = $cols[1];
    $grouped[$scn][] = $cols;
}

// helpers de estatistica amostral
$mean = static fn(array $x): float => count($x) > 0 ? array_sum($x) / count($x) : 0.0;
$std  = static function(array $x) use ($mean): float {
    $n = count($x);
    if ($n < 2) return 0.0;
    $m = $mean($x);
    $v = 0.0;
    foreach ($x as $v_i) { $v += ($v_i - $m) ** 2; }
    return sqrt($v / ($n - 1));
};

foreach ($grouped as $scn => $runs) {
    $events = array_map('floatval', array_column($runs, 4));
    $avgs   = array_map('floatval', array_column($runs, 6));
    $p95s   = array_map('floatval', array_column($runs, 8));
    $maxs   = array_map('floatval', array_column($runs, 11));

    echo sprintf(
        "  %-10s %10d %6d +/- %4d [%5d,%5d] %6d +/- %4d [%5d,%5d] %6d +/- %4d [%5d,%5d]\n",
        $scn,
        round($mean($events)),
        round($mean($avgs)), round($std($avgs)), (int)min($avgs), (int)max($avgs),
        round($mean($p95s)), round($std($p95s)), (int)min($p95s), (int)max($p95s),
        round($mean($maxs)), round($std($maxs)), (int)min($maxs), (int)max($maxs),
    );
}

echo "\n  Dispersao entre repeticoes (n={$repetitions}). Dados utilizaveis no Capitulo 6 do TCC.\n";
echo "===\n";

function exportDetailedMetrics(PDO $pg, string $path): void
{
    $rows = $pg->query("
        SELECT
            id, event_table, event_operation, record_id,
            ts_mysql, ts_source_ms, ts_dbz_ms, ts_kc_ms,
            ts_bronze, ts_silver, ts_gold,
            latency_dbz_ms, latency_publish_ms, latency_kafka_ms,
            latency_cdc_ms, latency_etl_ms, latency_total_ms,
            created_at
        FROM pipeline_metrics
        ORDER BY created_at
    ")->fetchAll();

    $fp = fopen($path, 'w');
    if (!empty($rows)) {
        fputcsv($fp, array_keys($rows[0]));
        foreach ($rows as $row) {
            fputcsv($fp, $row);
        }
    }
    fclose($fp);
}
