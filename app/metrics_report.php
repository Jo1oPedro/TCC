<?php

/**
 * Relatório de Métricas do Pipeline
 *
 * Gera um relatório detalhado das métricas coletadas durante
 * os experimentos do TCC.
 * 
 * Uso:
 *   php metrics_report.php
 *   php metrics_report.php --csv    (exporta CSV)
 * 
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\DatabaseConnection;

$config = require __DIR__ . '/config.php';
$exportCsv = in_array('--csv', $argv ?? []);

$pg = DatabaseConnection::get('postgres', $config['postgres']);

echo "===\n";
echo "  Relatório de Métricas do Pipeline\n";
echo "  " . date('Y-m-d H:i:s') . "\n";
echo "===\n\n";

// 1. Resumo geral
$summary = $pg->query("
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT event_table) as tables_monitored,
        MIN(created_at) as first_event,
        MAX(created_at) as last_event,
        EXTRACT(EPOCH FROM MAX(created_at) - MIN(created_at)) as duration_seconds
    FROM pipeline_metrics
")->fetch();

echo "--- Resumo Geral ---\n";
echo "  Total de eventos: {$summary['total_events']}\n";
echo "  Tabelas monitoradas: {$summary['tables_monitored']}\n";
echo "  Primeiro evento: {$summary['first_event']}\n";
echo "  Último evento: {$summary['last_event']}\n";
echo "  Duração: " . round($summary['duration_seconds'] ?? 0) . "s\n\n";

// 2. Latência por tabela e operação
$latency = $pg->query("
    SELECT
        event_table,
        event_operation,
        COUNT(*) as events,
        ROUND(AVG(latency_cdc_ms)) as avg_cdc_ms,
        ROUND(AVG(latency_total_ms)) as avg_total_ms,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_total_ms)) as p50_ms,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_total_ms)) as p95_ms,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_total_ms)) as p99_ms,
        ROUND(MAX(latency_total_ms)) as max_total_ms
    FROM pipeline_metrics
    WHERE latency_total_ms IS NOT NULL
    GROUP BY event_table, event_operation
    ORDER BY event_table, event_operation
")->fetchAll();

echo "--- Latência por Tabela/Operação ---\n";
echo sprintf(
    "  %-15s %-10s %6s %8s %8s %8s %8s %8s\n",
    'Tabela', 'Operação', 'Qtd', 'Avg(ms)', 'P50(ms)', 'P95(ms)', 'P99(ms)', 'Max(ms)'
);
echo "  " . str_repeat('-', 80) . "\n";

foreach ($latency as $row) {
    echo sprintf(
        "  %-15s %-10s %6d %8d %8d %8d %8d %8d\n",
        $row['event_table'],
        $row['event_operation'],
        $row['events'],
        $row['avg_total_ms'],
        $row['p50_ms'],
        $row['p95_ms'],
        $row['p99_ms'],
        $row['max_total_ms']
    );
}

// 2b. Decomposicao da latencia CDC
$breakdown = $pg->query("
    SELECT
        COUNT(*) AS events,
        ROUND(AVG(latency_dbz_ms))     AS avg_dbz_ms,
        ROUND(AVG(latency_publish_ms)) AS avg_publish_ms,
        ROUND(AVG(latency_kafka_ms))   AS avg_kafka_ms,
        ROUND(AVG(latency_cdc_ms))     AS avg_cdc_ms,
        ROUND(AVG(latency_etl_ms))     AS avg_etl_ms,
        ROUND(AVG(latency_total_ms))   AS avg_total_ms
    FROM pipeline_metrics
    WHERE latency_total_ms IS NOT NULL
")->fetch();

echo "\n--- Decomposicao da Latencia (medias em ms) ---\n";
echo "  Eventos analisados: {$breakdown['events']}\n";
echo "  ts_source -> ts_dbz (Debezium interno) : {$breakdown['avg_dbz_ms']} ms\n";
echo "  ts_dbz -> ts_kc (publicacao Kafka) : {$breakdown['avg_publish_ms']} ms\n";
echo "  ts_kc -> bronze (Kafka + consumo PHP) : {$breakdown['avg_kafka_ms']} ms\n";
echo "  ---\n";
echo "  CDC total (MySQL -> bronze) : {$breakdown['avg_cdc_ms']} ms\n";
echo "  ETL (bronze -> silver) : {$breakdown['avg_etl_ms']} ms\n";
echo "  Total (MySQL -> silver + flush) : {$breakdown['avg_total_ms']} ms\n";

// 3. Throughput ao longo do tempo
echo "\n--- Throughput (eventos/minuto) ---\n";

$throughput = $pg->query("
    SELECT 
        DATE_TRUNC('minute', created_at) as minute,
        COUNT(*) as events_per_minute,
        ROUND(AVG(latency_total_ms)) as avg_latency_ms
    FROM pipeline_metrics
    GROUP BY DATE_TRUNC('minute', created_at)
    ORDER BY minute
    LIMIT 30
")->fetchAll();

foreach ($throughput as $row) {
    $bar = str_repeat('█', min(50, (int)($row['events_per_minute'] / 2)));
    echo sprintf(
        "  %s | %4d eventos | avg %5dms | %s\n",
        substr($row['minute'], 11, 5),
        $row['events_per_minute'],
        $row['avg_latency_ms'],
        $bar
    );
}

// 4. Consistência (comparação MySQL vs Lakehouse)
echo "\n--- Verificação de Consistência ---\n";

$consistency = $pg->query("
    SELECT 
        event_table,
        event_operation,
        COUNT(*) as total,
        COUNT(CASE WHEN latency_total_ms IS NOT NULL THEN 1 END) as with_metrics,
        COUNT(CASE WHEN latency_total_ms IS NULL THEN 1 END) as without_metrics
    FROM pipeline_metrics
    GROUP BY event_table, event_operation
    ORDER BY event_table
")->fetchAll();

foreach ($consistency as $row) {
    $pct = $row['total'] > 0 ? round($row['with_metrics'] / $row['total'] * 100, 1) : 0;
    echo "  {$row['event_table']}.{$row['event_operation']}: {$row['total']} eventos ({$pct}% com métricas)\n";
}

// 5. Export CSV (opcional)
if ($exportCsv) {
    $csvPath = '/app/logs/metrics_export_' . date('Ymd_His') . '.csv';

    $all = $pg->query("
        SELECT * FROM pipeline_metrics ORDER BY created_at
    ")->fetchAll();

    $fp = fopen($csvPath, 'w');
    if (!empty($all)) {
        fputcsv($fp, array_keys($all[0]));
        foreach ($all as $row) {
            fputcsv($fp, $row);
        }
    }
    fclose($fp);

    echo "\n  📊 CSV exportado: {$csvPath}\n";
}

echo "\n===\n";
