🚀 Azure Databricks & Unity Catalog Governance PlaybookA comprehensive reference guide and cheat sheet covering Azure Databricks Architecture, Unity Catalog Governance, Data Lake Lifecycle, Delta Lake Features, and Auto Loader Streaming Pipelines.📌 1. Databricks Core ArchitectureDatabricks operates on a split architecture model:Control Plane: Managed by Databricks in its own Azure subscription. Contains notebooks, UI, job scheduler, web application, and workspace metadata.Compute / Data Plane: Located in your own Azure subscription. Contains the virtual networks (VNet), clusters/compute nodes, and execution engines where data is processed.🏛️ 2. Unity Catalog & Storage Governance FlowTo securely access and govern data in Azure Data Lake Storage Gen2 (ADLS Gen2), Unity Catalog follows a 5-tier security model:$$\text{Metastore} \rightarrow \text{Storage Credential} \rightarrow \text{External Location} \rightarrow \text{3-Level Namespace} \rightarrow \text{ADLS Gen2 Container}$$Metastore: Top-level container in Unity Catalog that stores metadata and security policies across workspaces.Workspace: Collaborative environment attached to a Metastore.Storage Credential: Authenticates Databricks with Azure using Managed Identities (Access Connectors).External Location: Pairs a Storage Credential with a cloud storage path (abfss://...).3-Level Namespace (catalog.schema.table):Catalog: Top-level data container.Schema (Database): Logical grouping of tables and volumes inside a catalog.Table / Volume: Data objects holding tabular or unstructured data.🛠️ 3. Databricks SQL & Management Cheat SheetCatalog & Table OperationsSQL-- Create Catalog and Schema
CREATE CATALOG IF NOT EXISTS prod_catalog;
USE CATALOG prod_catalog;
CREATE SCHEMA IF NOT EXISTS sales_schema;

-- Drop Catalog with all nested objects (Cascade)
DROP CATALOG IF EXISTS legacy_catalog CASCADE;

-- Drop Table
DROP TABLE IF EXISTS prod_catalog.sales_schema.raw_sales;
Managed vs External TablesManaged Table: Databricks manages both metadata and physical storage. Dropping the table deletes underlying cloud files.External Table: Points to a custom ADLS Gen2 directory. Dropping the table removes metadata only; raw files remain intact.SQL-- External Table Example
CREATE TABLE prod_catalog.sales_schema.external_sales (
    id INT,
    amount DOUBLE,
    transaction_date DATE
)
LOCATION 'abfss://container@storage.dfs.core.windows.net/sales_data/';
Permanent ViewsSQLCREATE OR REPLACE VIEW prod_catalog.sales_schema.high_value_sales AS
SELECT * FROM prod_catalog.sales_schema.external_sales
WHERE amount > 1000;
📁 4. Volumes (Unstructured Data Management)Volumes govern non-tabular files (PDFs, Images, CSVs, Raw JSONs, Model artifacts) in Unity Catalog.Managed Volume: Physical files deleted when volume is dropped.External Volume: Physical files remain safe on ADLS Gen2 when volume is dropped.SQL-- Create External Volume
CREATE EXTERNAL VOLUME prod_catalog.sales_schema.landing_volume
LOCATION 'abfss://container@storage.dfs.core.windows.net/raw_landing/';

-- POSIX Path Access in Notebooks
-- Path format: /Volumes/catalog_name/schema_name/volume_name/file_name
💻 5. Databricks Utility (dbutils) CommandsExecute file operations directly on ADLS Gen2 or Volumes using PySpark:Python# Create Directory in ADLS Gen2
dbutils.fs.mkdirs('abfss://container@storage.dfs.core.windows.net/new_folder/')

# Copy Folder recursively (Source to Destination / Volume)
dbutils.fs.cp(
    'abfss://container@storage.dfs.core.windows.net/source_path/',
    '/Volumes/prod_catalog/sales_schema/landing_volume/destination_path/',
    recurse=True
)
⚡ 6. Delta Lake Advanced FeaturesDeletion Vectors (Write Optimization)true (Default): Instead of rewriting full Parquet files during UPDATE/DELETE, Databricks writes a lightweight deletion vector file. Boosts write speed significantly.false (Compatibility Mode): Disables deletion vectors for downstream engines (e.g., legacy Power BI, external connectors) that do not support them.SQL-- Disable Deletion Vectors for external tool compatibility
ALTER TABLE prod_catalog.sales_schema.my_table 
SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);
Deep Clone vs Shallow CloneFeatureShallow CloneDeep CloneData CopiedMetadata only (Pointers)Full Data Files + MetadataStorage CostNear ZeroFull Duplicate StorageSpeedSecondsDepends on data volumeSource DependencyDependent on SourceFully IndependentPrimary Use CaseTesting, Staging, Ad-hoc analysisProduction Backups, MigrationSQL-- Shallow Clone
CREATE TABLE prod_catalog.sales_schema.table_shallow 
SHALLOW CLONE prod_catalog.sales_schema.source_table;

-- Deep Clone
CREATE TABLE prod_catalog.sales_schema.table_deep 
DEEP CLONE prod_catalog.sales_schema.source_table;
🔄 7. Incremental Data Loading with Auto Loader (cloudFiles)Auto Loader incrementally processes new incoming files from Cloud Storage / Volumes without full folder scans.Python# Streaming Read with Auto Loader
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/Volumes/prod_catalog/sales_schema/landing_volume/_schemas/")
    .load("/Volumes/prod_catalog/sales_schema/landing_volume/raw_landing/"))

# Write Stream with Checkpointing to Delta Sink
query = (df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/prod_catalog/sales_schema/landing_volume/_checkpoints/sales_target/")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="10 seconds")  # Or trigger(availableNow=True) for Incremental Batch
    .toTable("prod_catalog.sales_schema.fact_sales"))
⚙️ 8. Orchestration (Jobs & Workflows)Databricks Workflows allow orchestrating end-to-end data pipelines:Tasks: Notebooks, Python scripts, SQL Queries, dbt models.Triggers: Schedule-based (CRON), Event-driven (File arrival via Storage Events), or API-driven.Cluster Management: Job Clusters are automatically provisioned and terminated to optimize compute costs.
