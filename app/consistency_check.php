<?php

/**
 * Verificação de Consistência - CNES
 *
 * Compara o estado do MySQL (origem) com o PostgreSQL (destino gold)
 * e com os arquivos do lakehouse para validar que o pipeline
 * propagou corretamente os dados.
 */

require_once __DIR__ . '/vendor/autoload.php';

use TCC\Pipeline\ConsistencyChecker;

$config = require __DIR__ . '/config.php';

echo "  TCC Lakehouse - Verificação de Consistência CNES\n";
echo "  " . date('Y-m-d H:i:s') . "\n";

$consistencyChecker = new ConsistencyChecker($config);

$consistencyChecker->checkRecordCounts();
$consistencyChecker->checkEquipmentTotals();
$consistencyChecker->checkWorkforceHours();
$consistencyChecker->checkLakehouseFiles();
$consistencyChecker->checkMetricsCoverage();

// Resultado final
echo "\n===\n";
if ($consistencyChecker->consistencyIsOk()) {
    echo "  ✅ CONSISTÊNCIA OK — Dados do CNES no MySQL batem com o Gold no PostgreSQL.\n";
} else {
    echo "  ⚠️  DIVERGÊNCIAS ENCONTRADAS\n";
    echo "  Isso pode indicar que o pipeline ainda está processando\n";
    echo "  ou que o gold_processor precisa ser executado novamente.\n";
    echo "  Rode: php gold_processor.php\n";
}
echo "===\n";
