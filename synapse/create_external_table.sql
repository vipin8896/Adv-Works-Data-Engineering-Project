-- External table (CETAS) on the gold container.
-- Replace the placeholders with your own values before running.
-- Run each statement one by one.

-- 1) Master key (run once in your database)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your-strong-password>';

-- 2) Credential that uses the Synapse managed identity
CREATE DATABASE SCOPED CREDENTIAL cred_aw
WITH IDENTITY = 'Managed Identity';

-- 3) External data sources (silver and gold containers)
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION   = 'https://<your-storage-account>.dfs.core.windows.net/silver',
    CREDENTIAL = cred_aw
);

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION   = 'https://<your-storage-account>.dfs.core.windows.net/gold',
    CREDENTIAL = cred_aw
);

-- 4) External file format
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE      = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- 5) External table from the sales view (CETAS)
CREATE EXTERNAL TABLE gold.extsales
WITH (
    LOCATION    = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;

-- Check
SELECT * FROM gold.extsales;
