<?php

namespace TCC\Pipeline;

use PDO;

/**
 * Coleta e armazena métricas de latência do pipeline.
 *
 * Decomposição de latência:
 * - latency_dbz_ms:     ts_dbz - ts_source          (binlog read + processamento Debezium)
 * - latency_publish_ms: ts_kc  - ts_dbz             (publicação no Kafka)
 * - latency_kafka_ms:   ts_bronze - ts_kc           (Kafka trânsito + consumo PHP + escrita bronze)
 * - latency_cdc_ms:     ts_bronze - ts_source       (captura total MySQL → bronze)
 * - latency_etl_ms:     ts_silver - ts_bronze       (transformação bronze → silver)
 * - latency_total_ms:   now - ts_source             (MySQL → silver + registro de métrica)
 */
class MetricsCollector
{
    private PDO $pg;
    private bool $enabled;
    private array $buffer = [];
    private int $bufferLimit = 50; // flush a cada 50 registros

    public function __construct(PDO $postgres, bool $enabled = true)
    {
        $this->pg = $postgres;
        $this->enabled = $enabled;
    }

    /**
     * Registra uma métrica de evento processado, com decomposição da latência CDC.
     *
     * @param string    $table       Tabela de origem
     * @param string    $operation   Tipo de operação (INSERT/UPDATE/DELETE)
     * @param string|int|null $recordId ID do registro
     * @param int|null  $tsSource    source.ts_ms do Debezium (commit no MySQL/binlog)
     * @param int|null  $tsDbz       payload.ts_ms do Debezium (processamento interno do conector)
     * @param int|null  $tsKc        payload.ts_kafka_connect (SMT InsertField — publicação no Kafka)
     * @param string    $tsBronze    Timestamp ISO de gravação no bronze
     * @param string|null $tsSilver  Timestamp ISO de gravação no silver
     * @param string|null $tsGold    Timestamp ISO de gravação no gold
     */
    public function record(
        string  $table,
        string  $operation,
        string|int|null $recordId,
        ?int    $tsSource,
        ?int    $tsDbz,
        ?int    $tsKc,
        string  $tsBronze,
        ?string $tsSilver = null,
        ?string $tsGold = null,
    ): void {
        if (!$this->enabled) return;

        $nowMs = (int) (microtime(true) * 1000);
        $bronzeMs = self::isoToMillis($tsBronze);
        $silverMs = $tsSilver !== null ? self::isoToMillis($tsSilver) : null;

        // Decomposicao
        $latencyDbz     = ($tsSource !== null && $tsDbz !== null) ? $tsDbz - $tsSource : null;
        $latencyPublish = ($tsDbz !== null && $tsKc !== null) ? $tsKc - $tsDbz : null;
        $latencyKafka   = ($tsKc !== null && $bronzeMs !== null) ? $bronzeMs - $tsKc : null;
        $latencyCdc     = ($tsSource !== null && $bronzeMs !== null) ? $bronzeMs - $tsSource : null;
        $latencyEtl     = ($bronzeMs !== null && $silverMs !== null) ? $silverMs - $bronzeMs : null;
        $latencyTotal   = $tsSource !== null ? $nowMs - $tsSource : null;

        $this->buffer[] = [
            'table' => $table,
            'operation' => $operation,
            'record_id' => $recordId,
            'ts_mysql' => $tsSource ? date('Y-m-d H:i:s', (int)($tsSource / 1000)) : null,
            'ts_source_ms' => $tsSource,
            'ts_dbz_ms' => $tsDbz,
            'ts_kc_ms' => $tsKc,
            'ts_bronze' => $tsBronze,
            'ts_silver' => $tsSilver,
            'ts_gold' => $tsGold,
            'latency_dbz_ms' => $latencyDbz,
            'latency_publish_ms' => $latencyPublish,
            'latency_kafka_ms' => $latencyKafka,
            'latency_cdc_ms' => $latencyCdc,
            'latency_etl_ms' => $latencyEtl,
            'latency_total_ms' => $latencyTotal,
        ];

        if (count($this->buffer) >= $this->bufferLimit) {
            $this->flush();
        }
    }

    /**
     * Persiste métricas buffered no PostgreSQL.
     */
    public function flush(): void
    {
        if (empty($this->buffer)) return;

        try {
            $stmt = $this->pg->prepare("
                INSERT INTO pipeline_metrics
                    (event_table, event_operation, record_id,
                     ts_mysql, ts_source_ms, ts_dbz_ms, ts_kc_ms,
                     ts_bronze, ts_silver, ts_gold,
                     latency_dbz_ms, latency_publish_ms, latency_kafka_ms,
                     latency_cdc_ms, latency_etl_ms, latency_total_ms)
                VALUES
                    (:table, :operation, :record_id,
                     :ts_mysql, :ts_source_ms, :ts_dbz_ms, :ts_kc_ms,
                     :ts_bronze, :ts_silver, :ts_gold,
                     :latency_dbz_ms, :latency_publish_ms, :latency_kafka_ms,
                     :latency_cdc_ms, :latency_etl_ms, :latency_total_ms)
            ");

            foreach ($this->buffer as $m) {
                $stmt->execute([
                    ':table' => $m['table'],
                    ':operation' => $m['operation'],
                    ':record_id' => $m['record_id'],
                    ':ts_mysql' => $m['ts_mysql'],
                    ':ts_source_ms' => $m['ts_source_ms'],
                    ':ts_dbz_ms' => $m['ts_dbz_ms'],
                    ':ts_kc_ms' => $m['ts_kc_ms'],
                    ':ts_bronze' => $m['ts_bronze'],
                    ':ts_silver' => $m['ts_silver'],
                    ':ts_gold' => $m['ts_gold'],
                    ':latency_dbz_ms' => $m['latency_dbz_ms'],
                    ':latency_publish_ms' => $m['latency_publish_ms'],
                    ':latency_kafka_ms' => $m['latency_kafka_ms'],
                    ':latency_cdc_ms' => $m['latency_cdc_ms'],
                    ':latency_etl_ms' => $m['latency_etl_ms'],
                    ':latency_total_ms' => $m['latency_total_ms'],
                ]);
            }

            $count = count($this->buffer);
            $this->buffer = [];
            echo "[Metrics] {$count} métricas persistidas no PostgreSQL.\n";

        } catch (\Exception $e) {
            echo "[Metrics] ERRO ao persistir: {$e->getMessage()}\n";
        }
    }

    /**
     * Retorna um resumo das métricas coletadas.
     */
    public function getSummary(): array
    {
        $this->flush();

        $result = $this->pg->query("
            SELECT 
                event_table,
                event_operation,
                COUNT(*) as total_events,
                ROUND(AVG(latency_cdc_ms)) as avg_cdc_ms,
                ROUND(AVG(latency_total_ms)) as avg_total_ms,
                MIN(latency_total_ms) as min_total_ms,
                MAX(latency_total_ms) as max_total_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_total_ms) as p95_total_ms
            FROM pipeline_metrics
            WHERE latency_total_ms IS NOT NULL
            GROUP BY event_table, event_operation
            ORDER BY event_table, event_operation
        ")->fetchAll();

        return $result;
    }

    public function __destruct()
    {
        $this->flush();
    }

    /**
     * Converte timestamp ISO-8601 com microssegundos (Y-m-d\TH:i:s.u)
     * para milissegundos desde epoch, preservando a precisao.
     */
    private static function isoToMillis(string $iso): ?int
    {
        $dt = \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.u', $iso);
        if ($dt === false) {
            // fallback para formatos sem microssegundos
            $dt = \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s', $iso);
            if ($dt === false) {
                $ts = strtotime($iso);
                return $ts === false ? null : $ts * 1000;
            }
        }
        return (int) $dt->format('Uv');
    }
}
