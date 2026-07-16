<?php
/**
 * Le o CSV de resumo do experimento mais recente e produz:
 *   - tabela LaTeX de latencia (avg+/-std [min,max])
 *   - tabela LaTeX de throughput
 *   - tabela LaTeX de decomposicao da latencia
 *   - coordenadas para o grafico latencia x throughput
 *
 * Uso: php compute_dispersion.php [path/para/summary.csv]
 */

$csvPath = $argv[1] ?? null;
if ($csvPath === null) {
    $dir = '/app/logs/experiments';
    $files = glob("$dir/experiment_*_summary.csv");
    if (empty($files)) { fwrite(STDERR, "Nenhum CSV encontrado em $dir\n"); exit(1); }
    rsort($files);
    $csvPath = $files[0];
}

fwrite(STDERR, "Lendo: $csvPath\n");

$rows = [];
$fp = fopen($csvPath, 'r');
$header = fgetcsv($fp);
while (($r = fgetcsv($fp)) !== false) {
    $rows[] = array_combine($header, $r);
}
fclose($fp);

// agrupa por cenario
$by = [];
foreach ($rows as $r) { $by[$r['scenario']][] = $r; }

function mean(array $x): float { return count($x) ? array_sum($x)/count($x) : 0.0; }
function std(array $x): float {
    $n = count($x); if ($n < 2) return 0.0;
    $m = mean($x); $v = 0;
    foreach ($x as $i) $v += ($i - $m) ** 2;
    return sqrt($v / ($n - 1));
}

// Ordem desejada
$order = ['low', 'mixed', 'moderate', 'rate25', 'rate40', 'rate60', 'rate80', 'burst'];

echo "\n%% ===\n";
echo "%% Tabela de latencia (gerada por compute_dispersion.php)\n";
echo "%% ===\n";
foreach ($order as $scn) {
    if (!isset($by[$scn])) continue;
    $runs = $by[$scn];
    $avg = array_map('floatval', array_column($runs, 'avg_latency_ms'));
    $p95 = array_map('floatval', array_column($runs, 'p95_latency_ms'));
    $p99 = array_map('floatval', array_column($runs, 'p99_latency_ms'));
    $max = array_map('floatval', array_column($runs, 'max_latency_ms'));
    printf("%-9s & %d $\\pm$ %d [%d, %d] & %d $\\pm$ %d [%d, %d] & %d $\\pm$ %d [%d, %d] & %d $\\pm$ %d [%d, %d] \\\\\n",
        ucfirst($scn),
        round(mean($avg)), round(std($avg)), (int)min($avg), (int)max($avg),
        round(mean($p95)), round(std($p95)), (int)min($p95), (int)max($p95),
        round(mean($p99)), round(std($p99)), (int)min($p99), (int)max($p99),
        round(mean($max)), round(std($max)), (int)min($max), (int)max($max),
    );
}

echo "\n%% Tabela de throughput\n";
foreach ($order as $scn) {
    if (!isset($by[$scn])) continue;
    $runs = $by[$scn];
    $events = array_map('floatval', array_column($runs, 'total_events'));
    $nominal = match($scn) {
        'low'=>1, 'mixed'=>5, 'moderate'=>10, 'rate25'=>25, 'rate40'=>40,
        'rate60'=>60, 'rate80'=>80, 'burst'=>100, default=>0,
    };
    $effRate = mean($events) / 30.0; // duration=30s
    printf("%-9s & %d & %d & %.1f \\\\\n",
        ucfirst($scn), $nominal, round(mean($events)), $effRate);
}

echo "\n%% Tabela de decomposicao da latencia (medias)\n";
foreach ($order as $scn) {
    if (!isset($by[$scn])) continue;
    $runs = $by[$scn];
    $dbz   = array_map('floatval', array_column($runs, 'avg_dbz_ms'));
    $pub   = array_map('floatval', array_column($runs, 'avg_publish_ms'));
    $kfk   = array_map('floatval', array_column($runs, 'avg_kafka_ms'));
    $cdc   = array_map('floatval', array_column($runs, 'avg_cdc_ms'));
    $etl   = array_map('floatval', array_column($runs, 'avg_etl_ms'));
    $tot   = array_map('floatval', array_column($runs, 'avg_latency_ms'));
    printf("%-9s & %d & %s & %s & %d & %d & %d \\\\\n",
        ucfirst($scn),
        round(mean($dbz)),
        round(mean($pub)) ?: 'n/d',
        round(mean($kfk)) ?: 'n/d',
        round(mean($cdc)),
        round(mean($etl)),
        round(mean($tot)),
    );
}

echo "\n%% Coordenadas (taxa_nominal, lat_media, std) para pgfplots\n";
$rateMap = ['low'=>1,'mixed'=>5,'moderate'=>10,'rate25'=>25,'rate40'=>40,'rate60'=>60,'rate80'=>80,'burst'=>100];
echo "%% Pontos com erro:\n";
foreach ($order as $scn) {
    if (!isset($by[$scn])) continue;
    $runs = $by[$scn];
    $avg = array_map('floatval', array_column($runs, 'avg_latency_ms'));
    printf("  (%d, %d) +- (0, %d)\n", $rateMap[$scn], round(mean($avg)), round(std($avg)));
}

echo "\n";
