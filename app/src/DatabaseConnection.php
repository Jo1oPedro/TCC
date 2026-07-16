<?php

namespace TCC\Pipeline;

use PDO;
use PDOException;
use RuntimeException;

/**
 * Gerencia conexões com bancos de dados (MySQL e PostgreSQL).
 */
class DatabaseConnection
{
    private static array $connections = [];

    /**
     * Obtém uma conexão PDO para o banco especificado.
     *
     * @param string $name Nome da conexão ('mysql' ou 'postgres')
     * @param array  $config Configuração de conexão
     * @return PDO
     */
    public static function get(string $name, array $config): PDO
    {
        if (!isset(self::$connections[$name])) {
            self::$connections[$name] = self::create($name, $config);
        }

        return self::$connections[$name];
    }

    private static function create(string $name, array $config): PDO
    {
        try {
            $driver = match ($name) {
                'mysql' => 'mysql',
                'postgres', 'pg' => 'pgsql',
                default => throw new RuntimeException("Driver desconhecido: {$name}"),
            };

            $dsn = match ($driver) {
                'mysql' => "mysql:host={$config['host']};port={$config['port']};dbname={$config['database']};charset=utf8mb4",
                'pgsql' => "pgsql:host={$config['host']};port={$config['port']};dbname={$config['database']}",
            };

            $pdo = new PDO($dsn, $config['username'], $config['password'], [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ]);

            echo "[DB] Conexão '{$name}' estabelecida com sucesso.\n";
            return $pdo;

        } catch (PDOException $e) {
            throw new RuntimeException("[DB] Falha ao conectar '{$name}': " . $e->getMessage());
        }
    }
}
