<?php

/**
 * Gerador de Carga CNES
 *
 * Simula operações de cadastro e atualização de estabelecimentos
 * de saúde, profissionais e equipes no MySQL, gerando
 * INSERTs e UPDATEs para alimentar o pipeline CDC.
 *
 * Uso:
 *   php load_generator.php --scenario=low (1 op/seg)
 *   php load_generator.php --scenario=mixed (5 op/seg, inserts + updates)
 *   php load_generator.php --scenario=moderate (10 op/seg)
 *   php load_generator.php --scenario=rate25 (25 op/seg)
 *   php load_generator.php --scenario=rate40 (40 op/seg)
 *   php load_generator.php --scenario=rate60 (60 op/seg)
 *   php load_generator.php --scenario=rate80 (80 op/seg)
 *   php load_generator.php --scenario=burst (100 op/seg, rajada)
 *   php load_generator.php --duration=300 (duração em segundos, padrão: 60)
 *   php load_generator.php --scenario=moderate --duration=120
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\LoadGenerator;
use TCC\Pipeline\ScenarioConfig;

$config = require __DIR__ . '/config.php';

// Parse de argumentos
$scenario = 'low';
$duration = 60;

foreach ($argv as $arg) {
    if (str_starts_with($arg, '--scenario=')) {
        $scenario = substr($arg, strlen('--scenario='));
    }
    if (str_starts_with($arg, '--duration=')) {
        $duration = (int) substr($arg, strlen('--duration='));
    }
}

echo "  TCC Lakehouse - Gerador de Carga CNES\n";
echo "  Cenário: {$scenario}\n";
echo "  Duração: {$duration}s\n";
echo "  Início: " . date('Y-m-d H:i:s') . "\n";

$scenarioConfig = new ScenarioConfig();

// Execução
$startTime = microtime(true);
$endTime = $startTime + $duration;
$totalOps = 0;

$loadGenerator = new LoadGenerator($config, $scenarioConfig);

$scenario = $scenarioConfig->getScenario($scenario);

while (microtime(true) < $endTime) {
    // Seleciona operação baseada nos pesos
    $op = weightedRandom($scenario['operations']);

    try {
        match ($op) {
            'insert_estabelecimento' => $loadGenerator->insertEstabelecimento(),
            'update_estabelecimento' => $loadGenerator->updateEstabelecimento(),
            'insert_vinculo' => $loadGenerator->insertVinculo(),
            'update_vinculo' => $loadGenerator->updateVinculo(),
            'manage_equipe' => $loadGenerator->manageEquipe(),
        };
        $totalOps++;
    } catch (\Exception $e) {
        echo "[ERRO] {$op}: {$e->getMessage()}\n";
    }

    // Log periódico a cada 100 operações
    if ($totalOps % 100 === 0 && $totalOps > 0) {
        $elapsed = round(microtime(true) - $startTime, 1);
        $rate = round($totalOps / $elapsed, 1);
        echo "[Carga] {$totalOps} operações em {$elapsed}s ({$rate} ops/s)\n";
    }

    // Intervalo entre operações
    usleep($scenario['interval_ms'] * 1000);
}

// Relatório final
$elapsed = round(microtime(true) - $startTime, 1);
$rate = round($totalOps / $elapsed, 1);

echo "\n===\n";
echo "  Relatório de Carga\n";
echo "===\n";
echo "  Duração: {$elapsed}s\n";
echo "  Total de operações: {$totalOps}\n";
echo "  Taxa média: {$rate} ops/s\n";
echo "  ---\n";
foreach ($loadGenerator->counts as $op => $count) {
    echo "  {$op}: {$count}\n";
}
echo "===\n";

// Função auxiliar
function weightedRandom(array $weights): string
{
    $total = array_sum($weights);
    $rand = random_int(1, $total);
    $cumulative = 0;

    foreach ($weights as $key => $weight) {
        $cumulative += $weight;
        if ($rand <= $cumulative) {
            return $key;
        }
    }

    return array_key_first($weights);
}
