-- Iceberg JDBC catalog V1: Apache Iceberg JdbcUtil's catalog/namespace DDL.
-- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/jdbc/JdbcUtil.java
BEGIN;
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;
CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);
CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);
-- Fail on incompatible existing catalogs, rather than silently changing their schema.
SELECT catalog_name, table_namespace, table_name, metadata_location,
       previous_metadata_location, iceberg_type
FROM iceberg_catalog.iceberg_tables LIMIT 0;
SELECT catalog_name, namespace, property_key, property_value
FROM iceberg_catalog.iceberg_namespace_properties LIMIT 0;
COMMIT;
