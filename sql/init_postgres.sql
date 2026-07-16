-- Schema: analytics_db
-- Proposito: Camada analitica do Data Lakehouse para dados CNES
-- Recebe dados processados pelo pipeline CDC -> Kafka -> PHP

-- Visao consolidada do estabelecimento de saude
CREATE TABLE IF NOT EXISTS gold_estab_summary (
    co_unidade VARCHAR(20) PRIMARY KEY,
    co_cnes VARCHAR(7),
    razao_social VARCHAR(200),
    nome_fantasia VARCHAR(200),
    tipo_estabelecimento VARCHAR(200),
    tipo_unidade VARCHAR(200),
    natureza_juridica VARCHAR(200),
    municipio VARCHAR(100),
    uf VARCHAR(2),
    gestao VARCHAR(100),
    turno_atendimento VARCHAR(200),
    total_profissionais INT DEFAULT 0,
    total_equipes INT DEFAULT 0,
    total_equipamentos INT DEFAULT 0,
    total_servicos INT DEFAULT 0,
    latitude DECIMAL(12,8),
    longitude DECIMAL(12,8),
    dt_atualizacao DATE,
    dt_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Panorama de saude por municipio
CREATE TABLE IF NOT EXISTS gold_municipal_health (
    co_municipio VARCHAR(6) PRIMARY KEY,
    nome_municipio VARCHAR(100),
    uf VARCHAR(2),
    total_estabelecimentos INT DEFAULT 0,
    total_profissionais INT DEFAULT 0,
    total_equipes_esf INT DEFAULT 0,
    total_equipes_outras INT DEFAULT 0,
    total_equipamentos_sus INT DEFAULT 0,
    qtd_ubs INT DEFAULT 0,
    qtd_hospitais INT DEFAULT 0,
    qtd_upas INT DEFAULT 0,
    qtd_outros INT DEFAULT 0,
    horas_ambulatoriais_total BIGINT DEFAULT 0,
    horas_hospitalares_total BIGINT DEFAULT 0,
    dt_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Forca de trabalho em saude
CREATE TABLE IF NOT EXISTS gold_workforce (
    co_profissional_sus VARCHAR(20) PRIMARY KEY,
    no_profissional VARCHAR(200),
    cbo_codigo VARCHAR(10),
    cbo_descricao VARCHAR(200),
    conselho_classe VARCHAR(200),
    total_vinculos INT DEFAULT 0,
    horas_ambulatorial BIGINT DEFAULT 0,
    horas_hospitalar BIGINT DEFAULT 0,
    horas_outros BIGINT DEFAULT 0,
    qtd_estabelecimentos INT DEFAULT 0,
    qtd_equipes INT DEFAULT 0,
    dt_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metricas do pipeline
-- Decomposicao de latencia (em ms desde epoch quando aplicavel):
--   ts_source: source.ts_ms do Debezium - momento do commit no MySQL (binlog)
--   ts_dbz:    payload.ts_ms do Debezium - momento em que o conector processou o evento
--   ts_kc:     payload.ts_kafka_connect adicionado pela SMT InsertField - momento da publicacao no Kafka
--   ts_bronze: instante de gravacao em bronze (escrita do consumer PHP)
--   ts_silver: instante de gravacao em silver
-- Latencias derivadas:
--   latency_dbz_ms     = ts_dbz - ts_source          (binlog read + processamento interno do Debezium)
--   latency_publish_ms = ts_kc  - ts_dbz             (publicacao no Kafka)
--   latency_kafka_ms   = ts_bronze_ms - ts_kc        (transito Kafka + consumo PHP + escrita bronze)
--   latency_cdc_ms     = ts_bronze_ms - ts_source    (captura total: MySQL -> bronze)
--   latency_etl_ms     = ts_silver_ms - ts_bronze_ms (transformacao bronze -> silver)
--   latency_total_ms   = now_ms - ts_source          (MySQL -> silver + registro de metrica)
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    event_table VARCHAR(100) NOT NULL,
    event_operation VARCHAR(10) NOT NULL,
    record_id VARCHAR(50),
    ts_mysql TIMESTAMP,
    ts_source_ms BIGINT,
    ts_dbz_ms BIGINT,
    ts_kc_ms BIGINT,
    ts_bronze TIMESTAMP,
    ts_silver TIMESTAMP,
    ts_gold TIMESTAMP,
    latency_dbz_ms INT,
    latency_publish_ms INT,
    latency_kafka_ms INT,
    latency_cdc_ms INT,
    latency_etl_ms INT,
    latency_total_ms INT,
    batch_size INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_metrics_table ON pipeline_metrics(event_table);
CREATE INDEX IF NOT EXISTS idx_metrics_created ON pipeline_metrics(created_at);
CREATE INDEX IF NOT EXISTS idx_estab_municipio ON gold_estab_summary(municipio);
CREATE INDEX IF NOT EXISTS idx_estab_tipo ON gold_estab_summary(tipo_estabelecimento);
