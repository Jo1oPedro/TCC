<?php

/**
 * Processador da Camada Gold - CNES
 *
 * Lê dados da camada SILVER do lakehouse e materializa
 * agregações na camada Gold:
 *   - Filesystem (lakehouse-data/gold/)
 *   - PostgreSQL (tabelas gold_*)
 *
 * Todo dado vem do pipeline:
 *   MySQL → Debezium → Kafka → Consumer → Bronze → Silver → [AQUI] → Gold
 *
 * Modos de execução:
 *
 *   # Execução única (snapshot gold)
 *   php gold_processor.php
 *
 *   # Execução contínua (atualiza gold a cada N segundos)
 *   php gold_processor.php --loop --interval=30
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\DatabaseConnection;
use TCC\Pipeline\LakehouseWriter;
use TCC\Pipeline\GoldProcessor;

$config = require __DIR__ . '/config.php';

// Parse argumentos
$loop = in_array('--loop', $argv ?? []);
$interval = 30;
foreach ($argv as $arg) {
    if (str_starts_with($arg, '--interval=')) {
        $interval = (int) substr($arg, strlen('--interval='));
    }
}

echo "===\n";
echo "  TCC Lakehouse - Processador Gold CNES\n";
echo "  Fonte: camada Silver do lakehouse (NÃO consulta MySQL)\n";
echo "  Modo: " . ($loop ? "contínuo (a cada {$interval}s)" : "execução única") . "\n";
echo "  Início: " . date('Y-m-d H:i:s') . "\n";
echo "===\n\n";

// Conexões
$postgres = DatabaseConnection::get('postgres', $config['postgres']);
$writer = new LakehouseWriter($config['lakehouse']['base_path']);

$processor = new GoldProcessor($postgres, $writer, $config['lakehouse']['base_path']);

do {
    echo "[Gold] Processando agregações a partir do Silver... (" . date('H:i:s') . ")\n";

    try {
        $results = $processor->processAll();

        echo "[Gold] Resultados:\n";
        echo "  - estab_summary:    {$results['estab_summary']} registros\n";
        echo "  - municipal_health: {$results['municipal_health']} registros\n";
        echo "  - workforce:        {$results['workforce']} registros\n";
        echo "[Gold] Concluído.\n\n";
    } catch (\Exception $e) {
        echo "[Gold] ERRO: {$e->getMessage()}\n\n";
    }

    if ($loop) {
        echo "[Gold] Próxima execução em {$interval}s...\n";
        sleep($interval);
    }
} while ($loop);
