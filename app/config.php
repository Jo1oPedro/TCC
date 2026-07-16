<?php

/**
 * Configuracoes centralizadas do pipeline CNES.
 */

// Timezone explicito para garantir consistencia em latencias e timestamps.
// Debezium envia ts_ms em UTC; mantemos o pipeline em UTC para evitar drift.
date_default_timezone_set('UTC');

return [
    'kafka' => [
        'brokers' => getenv('KAFKA_BROKERS') ?: 'kafka:29092',
        'group_id' => getenv('KAFKA_GROUP_ID') ?: 'lakehouse-consumer-group',
        'auto_offset_reset' => 'earliest',
        'topics' => [
            'cdc.cnes_db.estabelecimentos',
            'cdc.cnes_db.profissionais',
            'cdc.cnes_db.carga_horaria',
            'cdc.cnes_db.equipes',
            'cdc.cnes_db.equipe_profissionais',
            'cdc.cnes_db.estab_equipamentos',
            'cdc.cnes_db.estab_servicos',
        ],
    ],

    'mysql' => [
        'host' => getenv('MYSQL_HOST') ?: 'mysql',
        'port' => getenv('MYSQL_PORT') ?: '3306',
        'database' => 'cnes_db',
        'username' => getenv('MYSQL_USER') ?: 'appuser',
        'password' => getenv('MYSQL_PASS') ?: 'appuser123',
    ],

    'postgres' => [
        'host' => getenv('PG_HOST') ?: 'postgres',
        'port' => getenv('PG_PORT') ?: '5432',
        'database' => 'analytics_db',
        'username' => getenv('PG_USER') ?: 'analyst',
        'password' => getenv('PG_PASS') ?: 'analyst123',
    ],

    'lakehouse' => [
        'base_path' => getenv('LAKEHOUSE_PATH') ?: '/lakehouse-data',
        'format' => 'jsonl',
    ],

    'metrics' => [
        'enabled' => true,
        'log_file' => '/app/logs/pipeline.log',
    ],
];
