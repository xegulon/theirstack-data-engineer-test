-- Créer la base de données si elle n'existe pas
CREATE DATABASE IF NOT EXISTS mydb;

-- Utiliser la base de données
USE mydb;

-- Créer la table company_landing
CREATE TABLE IF NOT EXISTS company_landing
(
    data_provider_origin_id UInt32,
    data_provider_company_id String,
    name String,
    domain Nullable(String),
    linkedin_slug Nullable(String),
    info String, -- JSON stored as String in ClickHouse
    created_at DateTime64(3) DEFAULT now(),
    updated_at DateTime64(3) DEFAULT now(),
    host Nullable(String),
    url Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY (data_provider_origin_id, data_provider_company_id);

-- Inserts the 5k records dataset
-- INSERT INTO company_landing 
-- SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_5k.csv', 'CSV');

-- Inserts the 50k records dataset
-- INSERT INTO company_landing 
-- SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_50k.csv', 'CSV');

-- Inserts the 500k records dataset
INSERT INTO company_landing 
SELECT * FROM s3('https://media.theirstack.com/ts-data-engineer-test-2025/company_landing_500k.csv', 'CSV'); 