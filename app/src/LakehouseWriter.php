<?php

namespace TCC\Pipeline;

use RuntimeException;

/**
 * Responsável pela escrita nas camadas do lakehouse (bronze/silver/gold).
 *
 * Bronze: dados brutos do CDC, com metadados do Debezium
 * Silver: dados limpos e normalizados
 * Gold:   dados agregados prontos para consulta analítica
 */
class LakehouseWriter
{
    private string $basePath;

    public function __construct(string $basePath)
    {
        $this->basePath = rtrim($basePath, '/');
    }

    /**
     * Grava um evento na camada Bronze.
     * Armazena o evento CDC completo, com todos os metadados do Debezium.
     *
     * @param string $table    Nome da tabela de origem
     * @param array  $event    Evento CDC completo
     * @return array{path: string, timestamp: string}
     */
    public function writeBronze(string $table, array $event): array
    {
        $now = new \DateTimeImmutable();
        $dateDir = $now->format('Y-m-d');
        $dir = "{$this->basePath}/bronze/{$table}/{$dateDir}";

        $this->ensureDir($dir);

        // Enriquece com metadados de ingestão
        $record = [
            '_ingested_at' => $now->format('Y-m-d\TH:i:s.u'),
            '_source_table' => $table,
            '_operation' => $event['op'] ?? 'unknown',
            'before' => $event['before'] ?? null,
            'after' => $event['after'] ?? null,
            'source' => $event['source'] ?? null,
            'ts_ms' => $event['ts_ms'] ?? null,
        ];

        $filename = sprintf(
            '%s-%06d.jsonl',
            $now->format('His'),
            random_int(0, 999999)
        );
        $path = "{$dir}/{$filename}";

        file_put_contents($path, json_encode($record, JSON_UNESCAPED_UNICODE) . "\n", FILE_APPEND);

        return [
            'path' => $path,
            'timestamp' => $record['_ingested_at'],
        ];
    }

    /**
     * Grava um registro na camada Silver.
     * Dados já limpos, normalizados e com tipagem correta.
     *
     * @param string $table    Nome da tabela
     * @param array  $record   Registro limpo (após transformação)
     * @return array{path: string, timestamp: string}
     */
    public function writeSilver(string $table, array $record): array
    {
        $now = new \DateTimeImmutable();
        $dateDir = $now->format('Y-m-d');
        $dir = "{$this->basePath}/silver/{$table}/{$dateDir}";

        $this->ensureDir($dir);

        $record['_processed_at'] = $now->format('Y-m-d\TH:i:s.u');

        $filename = sprintf(
            '%s-%06d.jsonl',
            $now->format('His'),
            random_int(0, 999999)
        );
        $path = "{$dir}/{$filename}";

        file_put_contents($path, json_encode($record, JSON_UNESCAPED_UNICODE) . "\n", FILE_APPEND);

        return [
            'path' => $path,
            'timestamp' => $record['_processed_at'],
        ];
    }

    /**
     * Grava um registro agregado na camada Gold.
     *
     * @param string $dataset  Nome do dataset agregado (ex: 'daily_sales')
     * @param array  $record   Registro agregado
     * @return array{path: string, timestamp: string}
     */
    public function writeGold(string $dataset, array $record): array
    {
        $now = new \DateTimeImmutable();
        $dir = "{$this->basePath}/gold/{$dataset}";

        $this->ensureDir($dir);

        $record['_aggregated_at'] = $now->format('Y-m-d\TH:i:s.u');

        $filename = $now->format('Y-m-d') . '.jsonl';
        $path = "{$dir}/{$filename}";

        file_put_contents($path, json_encode($record, JSON_UNESCAPED_UNICODE) . "\n", FILE_APPEND);

        return [
            'path' => $path,
            'timestamp' => $record['_aggregated_at'],
        ];
    }

    private function ensureDir(string $dir): void
    {
        if (!is_dir($dir) && !mkdir($dir, 0755, true) && !is_dir($dir)) {
            throw new RuntimeException("Não foi possível criar diretório: {$dir}");
        }
    }
}
