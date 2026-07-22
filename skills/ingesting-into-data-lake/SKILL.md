---
name: ingesting-into-data-lake
description: >-
  Move data into the data lake using the right AWS service per source type. Provisions
  Landing layer (DMS for databases, AppFlow for SaaS, Firehose for streaming, Transfer
  Family for SFTP, DataSync for on-prem) and generates Glue ETL jobs that read from
  Landing S3 only — never directly from the source. Implements the two-layer Landing+Raw
  architecture with DQDL quality rules, intelligent worker sizing, and pre-deployment
  validation. Triggers on: import data, load data, ingest, sync database, migrate table,
  set up pipeline, ETL, replicate, CDC, onboard source.
  Do NOT use for creating Glue connections (use connecting-to-data-source), creating
  empty tables (use creating-data-lake-table), running queries (use querying-data-lake).
version: 2
argument-hint: '[source-type|source-name|connection-name] [--tool dms|appflow|firehose|glue]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Ingest into Data Lake

Move data from a source into the data lake using the right AWS service. This skill
implements the two-layer architecture: provision the Landing layer ingestion service,
then generate a Glue ETL job that reads from Landing S3 to promote data to Raw/Bronze Iceberg.

## Philosophy

**Right tool per source. Glue reads from S3 only.**

- Databases → AWS DMS (CDC or full load) → S3 Landing → Glue Raw
- SaaS → Amazon AppFlow → S3 Landing → Glue Raw
- Streaming → Kinesis Firehose → S3 Landing → Glue Raw
- SFTP files → Transfer Family → S3 Landing → Glue Raw
- On-prem → DataSync → S3 Landing → Glue Raw
- S3 files (already landed) → EventBridge → Glue Raw directly

**Never default to Glue JDBC for database sources.** DMS is purpose-built and 4× cheaper.

## Integration with Self-Serve Data Onboarding

This skill handles **Steps 7-9** of the master onboarding workflow:
- Step 7: Provision Landing ingestion service (DMS/AppFlow/Firehose/Transfer Family)
- Step 8: Configure Landing → Raw EventBridge trigger
- Step 9: Generate Glue ETL job (reads from Landing S3, writes to Raw Iceberg)

## Workflow

### 1. Confirm Source Tool (from discovery)

By this point, Q1-Q7 discovery is complete and the user has confirmed the tool via cost comparison. Use the selected tool:

| Confirmed Tool | Action |
|---|---|
| AWS DMS | Provision replication instance + task → S3 Parquet Landing |
| Amazon AppFlow | Create connector profile + flow → S3 Parquet Landing |
| Kinesis Firehose | Create delivery stream (JSON→Parquet) → S3 Landing |
| Transfer Family | Create SFTP server + S3 mapping → S3 Landing |
| DataSync | Create task (on-prem → S3 Landing) |
| Aurora S3 Export | Configure export task → S3 Parquet Landing |
| Glue (S3 already landed) | Skip Landing provisioning — files already in S3 |

### 2. Pre-Deployment Validation

Run ALL checks from `steering/pre-deployment-validation.md` before generating code:
- Connectivity, credentials, networking, IAM, ASL, worker type
- MUST PASS before proceeding

### 3. Provision Landing Layer

Configure the selected ingestion service to deliver data to:
```
s3://{bucket}/landing/{source_name}/{table_name}/year=YYYY/month=MM/day=DD/
```

Format: Parquet (when service supports it), otherwise source-native.
See `steering/landing-layer.md` for service-specific configurations.

### 4. Configure EventBridge Trigger

Set up S3 event → EventBridge → Step Functions to trigger the Raw pipeline when new Landing files arrive.

### 5. Generate Glue Raw Job

Generate PySpark Glue job that:
1. Reads from Landing S3 path (Parquet or source format)
2. Adds audit columns: `_ingested_at`, `_source_file`, `_batch_id`, `_row_hash`, `_ingested_date`
3. Runs DQDL quality rules (compiled from data contract)
4. Routes failures to `{table}_quarantine` Iceberg table
5. Deduplicates via `_row_hash` (within-batch) and merge key (cross-batch)
6. Writes pass rows to Raw Iceberg table
7. Emits CloudWatch metrics

**Key:** The Glue job reads S3 files. It never reads from the source system via JDBC/API.

### 6. Intelligent Worker Sizing

Apply sizing from `steering/ingestion-standards.md`:
- Volume < 1 GB → G.1X, 2 workers, 15 min timeout
- Volume 1-10 GB → G.1X, 5 workers, 30 min timeout
- Volume 10-50 GB → G.1X, 10 workers, 60 min timeout
- Volume > 50 GB → G.2X, 10 workers, 120 min timeout
- SLA > 2 hours → use Flex execution class (35% cheaper)
- S3-sourced jobs need 50% fewer workers than JDBC

### 7. Validate and Test

After deploying:
1. Trigger Landing ingestion (initial load)
2. Verify files in `s3://{bucket}/landing/{source}/`
3. Verify EventBridge triggers Raw pipeline
4. Verify data in Raw Iceberg table via Athena
5. Verify DQDL results in Glue Console
6. Verify CloudWatch metrics emitted

## Gotchas

- S3 Tables requires Glue 5.1+ and `--datalake-formats iceberg`
- All `spark.sql.catalog.*` config MUST go in `--conf` args, never `spark.conf.set()`
- G.025X is ONLY valid for `gluestreaming` — auto-correct to G.1X for `glueetl`
- DMS S3 target uses date-partitioned folders natively — align with Landing convention
- AppFlow Parquet output requires explicit `prefixConfig` for date partitioning
- Firehose uses `!{timestamp:yyyy}` dynamic partitioning syntax
- DynamoDB does not need DMS or Glue connection — use native S3 Export

## Troubleshooting

| Error | Cause | Action |
|---|---|---|
| DMS task ERROR state | Source connectivity or grants | Check DMS endpoint, source permissions |
| AppFlow CONNECTOR_RUNTIME_ERROR | OAuth expired | Refresh connector profile |
| Firehose SUSPENDED | S3 delivery failures | Check bucket permissions, resume stream |
| Glue timeout | Volume larger than sized | Increase workers or timeout |
| DQDL quarantine > 5% | Data quality issue | Inspect quarantine table, check source |
| No files in Landing | Ingestion service not running | Check DMS/AppFlow/Firehose status |
- Step 12: Run Smoke Test in Dev (validate with limited data)

It also powers the runtime execution of generated pipelines. The pipeline generator (`steering/pipeline-generation.md`) produces config-driven code; this skill executes it.

**Platform conventions enforced:**
- All ingested data MUST include audit columns: `_ingested_at`, `_source_file`, `_batch_id`, `_pipeline_version`, `_row_hash`
- Invalid rows MUST be quarantined (never dropped) per `hooks/post-ingestion-checks.md`
- Incremental processing by default (bookmarks for S3, watermarks for JDBC)
- Data contracts MUST be validated post-ingestion per the contract engine

## Common Tasks

You MUST execute commands using AWS MCP server tools when connected — they provide validation, sandboxed execution, and audit logging. Fall back to AWS CLI only if MCP is unavailable. You MUST explain each step before executing.

## Workflow

### 1. Verify Dependencies and Context

- You MUST check whether AWS MCP tools or AWS CLI are available and inform the user if missing
- You MUST confirm target AWS region and verify credentials with `aws sts get-caller-identity`

### 2. Classify the Source

| User says... | Source type |
|---|---|
| "upload my file", "local CSV", "move to S3" | Local file |
| "load from S3", "import CSV/JSON/Parquet from s3://" | S3 files |
| "import from Oracle/Postgres/MySQL/SQL Server/Redshift/RDS/Aurora" | JDBC |
| "pull from Snowflake", "Snowflake table to S3" | Snowflake |
| "import from BigQuery", "GCP analytics to S3" | BigQuery |
| "export DynamoDB", "DynamoDB to data lake" | DynamoDB |
| "migrate Glue table", "convert Hive to Iceberg" | Catalog migration |

If the user names Salesforce, ServiceNow, SAP, MongoDB, or Kafka, redirect to `steering/saas-streaming-ingestion.md` — those use the platform's REST API / streaming templates.

If the source table is referenced by a fuzzy or business name, delegate to `finding-data-lake-assets` to resolve before proceeding.

### 3. Confirm Connection Exists (if applicable)

For JDBC, Snowflake, and BigQuery sources, a Glue connection is required:

```bash
aws glue get-connection --name <CONNECTION_NAME> --region <REGION>
```

If the connection does not exist, delegate to `connecting-to-data-source` to create and test it. Do not proceed until verified.

Local files, S3 files, DynamoDB, and catalog migration do not need a Glue connection.

### 4. Clarify the Target

You MUST confirm before creating or writing to any table:

- **Database/namespace**: Does a specific target database exist?
- **Table**: Existing table (append/merge) or new table (delegate to `creating-data-lake-table`)?
- **Format**: S3 Tables (default), standard Iceberg, or raw Parquet?

**Platform defaults:**
- Target format: Iceberg (via S3 Tables or general-purpose bucket)
- Compression: ZSTD for Parquet
- Partitioning: by `_ingested_date` (derived from `_ingested_at`)
- Write mode: APPEND for bronze layer; MERGE for silver/curated

### 5. Execute Source Workflow

Execute the ingestion based on source type. Common configuration:

**Glue job arguments (Glue 5.1+):**
```bash
--conf spark.sql.catalog.s3tablescatalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.s3tablescatalog.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
--conf spark.sql.catalog.s3tablescatalog.warehouse=arn:aws:s3tables:<REGION>:<ACCOUNT>:bucket/<BUCKET>
--datalake-formats iceberg
```

**Platform requirements for every ingestion job:**
- Add audit columns (`_ingested_at`, `_source_file`, `_batch_id`, `_pipeline_version`, `_row_hash`)
- Validate against data contract quality rules
- Route invalid rows to quarantine table
- Emit CloudWatch metrics (rows_read, rows_valid, rows_quarantined, duration_ms)

### 6. Validate

Run all three checks (do not skip):

1. Row count matches expected (source vs target)
2. Null check on critical columns
3. Spot-check 3-5 sample rows via Athena

Post-ingestion validation is also handled by the `hooks/post-ingestion-checks.md` hook which runs automatically.

### 7. Schedule (if recurring)

For recurring pipelines:
- Simple single-step: Glue Triggers with cron schedule via EventBridge
- Multi-step with branching: AWS Step Functions (per `steering/pipeline-generation.md`)
- Schedule defined in `pipeline-config.yaml` under the `schedule` section

## Argument Routing

| User provides | Action |
|---|---|
| S3 path only | Infer one-time load, start at Step 2 with S3 files |
| Connection name | Start at Step 3 with the named connection |
| Table name | Start at Step 4, ask whether this is source or target |
| `--target` flag | Pre-fill the target format in Step 4 |
| No args | Walk through interactively |

## Gotchas

- S3 Tables requires Glue 5.1+ and `--datalake-formats iceberg` job argument
- All `spark.sql.catalog.*` config MUST go in `--conf` job arguments, never in `spark.conf.set()`. Glue 5.x throws `AnalysisException: Cannot modify the value of a static config` otherwise.
- The `warehouse` parameter is required in S3 Tables catalog config.
- Table and column names in S3 Tables MUST be all lowercase
- Standard Iceberg targets MUST include a LOCATION clause; S3 Tables MUST NOT
- DynamoDB does not need a Glue connection — do not attempt to create one
- Connection failures during ingest delegate back to `connecting-to-data-source`

## Troubleshooting

| Error | Likely cause | Action |
|---|---|---|
| Access Denied on S3 | Missing IAM permissions | Check Glue role has s3:GetObject, s3:PutObject |
| Access Denied on S3 Tables | Missing s3tables:* permissions | Add S3 Tables inline policy to Glue role |
| CTAS timeout | Dataset too large for Athena | Switch to Glue ETL or batch with WHERE filters |
| JDBC connection timeout/auth failure | Connection-level issue | Delegate to `connecting-to-data-source` |
| Throughput exceeded (DynamoDB) | Read percent too high | Lower `read.percent` or use native export |
| Quarantine rate > 5% | Data quality issue | Check `steering/troubleshooting.md` Step 2c |
