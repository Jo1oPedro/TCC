<?php

namespace TCC\Pipeline;

use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

class ConsistencyChecker
{
    private bool $allOk = true;
    private \PDO $mysql;
    private \PDO $pg;
    private string $basePath;

    public function __construct(
        private array $config
    ) {
        $this->mysql = DatabaseConnection::get('mysql', $config['mysql']);
        $this->pg = DatabaseConnection::get('postgres', $config['postgres']);
        $this->basePath = $config['lakehouse']['base_path'];
    }

    /**
     * 1. Contagem de registros: MySQL vs PostgreSQL Gold
     */
    public function checkRecordCounts(): bool
    {
        echo "--- 1. MySQL (origem) vs PostgreSQL (gold) ---\n\n";

        // Estabelecimentos vs gold_estab_summary
        $mysqlEstab = $this->mysql->query("SELECT COUNT(*) as cnt FROM estabelecimentos")->fetch()['cnt'];
        $pgEstab = $this->pg->query("SELECT COUNT(*) as cnt FROM gold_estab_summary")->fetch()['cnt'];
        $estabMatch = ($mysqlEstab == $pgEstab);
        $this->allOk = $this->allOk && $estabMatch;

        echo sprintf(
            "  Estabelecimentos: MySQL=%d | PG Gold=%d | %s\n",
            $mysqlEstab, $pgEstab,
            $estabMatch ? 'OK' : 'DIVERGENTE'
        );

        // Municipios com estabelecimentos vs gold_municipal_health
        $mysqlMun = $this->mysql->query("SELECT COUNT(DISTINCT co_municipio_gestor) as cnt FROM estabelecimentos")->fetch()['cnt'];
        $pgMun = $this->pg->query("SELECT COUNT(*) as cnt FROM gold_municipal_health")->fetch()['cnt'];
        $munMatch = ($mysqlMun == $pgMun);
        $this->allOk = $this->allOk && $munMatch;

        echo sprintf(
            "  Municipios:       MySQL=%d | PG Gold=%d | %s\n",
            $mysqlMun, $pgMun,
            $munMatch ? 'OK' : 'DIVERGENTE'
        );

        // Profissionais com carga_horaria vs gold_workforce
        $mysqlProf = $this->mysql->query("SELECT COUNT(DISTINCT co_profissional_sus) as cnt FROM carga_horaria")->fetch()['cnt'];
        $pgProf = $this->pg->query("SELECT COUNT(*) as cnt FROM gold_workforce")->fetch()['cnt'];
        $profMatch = ($mysqlProf == $pgProf);
        $this->allOk = $this->allOk && $profMatch;

        echo sprintf(
            "  Profissionais:    MySQL=%d | PG Gold=%d | %s\n",
            $mysqlProf, $pgProf,
            $profMatch ? 'OK' : 'DIVERGENTE'
        );

        return $this->allOk;
    }

    /**
     * 2. Verificacao de totais de equipamentos
     * Compara MySQL SUM(qt_existente) vs PG SUM(total_equipamentos)
     */
    public function checkEquipmentTotals(): bool
    {
        echo "\n--- 2. Totais de equipamentos ---\n\n";

        $mysqlTotal = $this->mysql->query("SELECT COALESCE(SUM(qt_existente), 0) as total FROM estab_equipamentos")->fetch()['total'];
        $pgTotal = $this->pg->query("SELECT COALESCE(SUM(total_equipamentos), 0) as total FROM gold_estab_summary")->fetch()['total'];

        $diff = abs($mysqlTotal - $pgTotal);
        $match = ($diff == 0);
        $this->allOk = $this->allOk && $match;

        echo sprintf(
            "  MySQL SUM(qt_existente):           %d\n",
            $mysqlTotal
        );
        echo sprintf(
            "  PG SUM(total_equipamentos):        %d\n",
            $pgTotal
        );
        echo sprintf(
            "  Diferenca: %d | %s\n",
            $diff,
            $match ? 'OK' : 'DIVERGENTE'
        );

        return $this->allOk;
    }

    /**
     * 3. Verificacao de horas da forca de trabalho
     * Compara MySQL SUM(qt_carga_horaria_ambulatorial) vs PG SUM(horas_ambulatorial)
     */
    public function checkWorkforceHours(): bool
    {
        echo "\n--- 3. Horas da forca de trabalho ---\n\n";

        $mysqlHoras = $this->mysql->query("SELECT COALESCE(SUM(qt_carga_horaria_ambulatorial), 0) as total FROM carga_horaria")->fetch()['total'];
        $pgHoras = $this->pg->query("SELECT COALESCE(SUM(horas_ambulatorial), 0) as total FROM gold_workforce")->fetch()['total'];

        $diff = abs($mysqlHoras - $pgHoras);
        $match = ($diff == 0);
        $this->allOk = $this->allOk && $match;

        echo sprintf(
            "  MySQL SUM(qt_carga_horaria_ambulatorial): %d\n",
            $mysqlHoras
        );
        echo sprintf(
            "  PG SUM(horas_ambulatorial):               %d\n",
            $pgHoras
        );
        echo sprintf(
            "  Diferenca: %d | %s\n",
            $diff,
            $match ? 'OK' : 'DIVERGENTE'
        );

        return $this->allOk;
    }

    /**
     * 4. Verificacao do lakehouse (filesystem)
     */
    public function checkLakehouseFiles(): bool
    {
        echo "\n--- 4. Arquivos no Lakehouse ---\n\n";

        $layers = ['bronze', 'silver', 'gold'];

        foreach ($layers as $layer) {
            $path = "{$this->basePath}/{$layer}";
            $fileCount = 0;
            $totalSize = 0;
            $tables = [];

            if (is_dir($path)) {
                $iterator = new RecursiveIteratorIterator(
                    new RecursiveDirectoryIterator($path, RecursiveDirectoryIterator::SKIP_DOTS)
                );
                foreach ($iterator as $file) {
                    if ($file->isFile() && $file->getExtension() === 'jsonl') {
                        $fileCount++;
                        $totalSize += $file->getSize();
                        // Extrai nome da tabela/dataset do path
                        $relative = str_replace($path . '/', '', $file->getPathname());
                        $parts = explode('/', $relative);
                        if (isset($parts[0])) {
                            $tables[$parts[0]] = ($tables[$parts[0]] ?? 0) + 1;
                        }
                    }
                }
            }

            $sizeKb = round($totalSize / 1024, 1);
            $hasFiles = $fileCount > 0;
            if ($layer !== 'gold') {
                $this->allOk = $this->allOk && $hasFiles;
            }

            echo sprintf(
                "  %-8s %4d arquivos | %6s KB | %s\n",
                ucfirst($layer) . ':',
                $fileCount,
                $sizeKb,
                $hasFiles ? 'OK' : 'VAZIO'
            );

            if (!empty($tables)) {
                foreach ($tables as $table => $count) {
                    echo "           -- {$table}: {$count} arquivos\n";
                }
            }
        }

        return $this->allOk;
    }

    /**
     * 5. Verificacao do pipeline de metricas
     */
    public function checkMetricsCoverage(): void
    {
        echo "\n--- 5. Pipeline de Metricas ---\n\n";
        $metricsCount = $this->pg->query("SELECT COUNT(*) as cnt FROM pipeline_metrics")->fetch()['cnt'];
        $metricsWithLatency = $this->pg->query("SELECT COUNT(*) as cnt FROM pipeline_metrics WHERE latency_total_ms IS NOT NULL")->fetch()['cnt'];

        $metricsPct = $metricsCount > 0 ? round($metricsWithLatency / $metricsCount * 100, 1) : 0;

        echo "  Total de metricas:     {$metricsCount}\n";
        echo "  Com latencia calculada: {$metricsWithLatency} ({$metricsPct}%)\n";

        if ($metricsCount > 0) {
            $avgLatency = $this->pg->query("SELECT ROUND(AVG(latency_total_ms)) as avg FROM pipeline_metrics WHERE latency_total_ms IS NOT NULL")->fetch()['avg'];
            echo "  Latencia media:        {$avgLatency}ms\n";
        }
    }

    public function consistencyIsOk(): bool
    {
        return $this->allOk;
    }
}
