-- Create separate database for airflow
CREATE DATABASE airflow;

-- Connect to analytical warehouse database
\c warehouse;

-- Create staging schema for raw/silver tables ingested by Spark
CREATE SCHEMA IF NOT EXISTS raw_stage;

-- Create analytics schema for dbt production marts
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS dbt_staging;

-- Grant usage permissions
GRANT ALL ON SCHEMA raw_stage TO postgres;
GRANT ALL ON SCHEMA analytics TO postgres;
GRANT ALL ON SCHEMA dbt_staging TO postgres;
