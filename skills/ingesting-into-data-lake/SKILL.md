---
name: ingesting-into-data-lake
description: >-
  Import data into the AWS data lake from S3 files, local uploads, JDBC databases
  (Oracle, SQL Server, PostgreSQL, MySQL, RDS, Aurora), Amazon Redshift, Snowflake,
  BigQuery, DynamoDB, or existing Glue catalog tables (migration). Default target
  is S3 Tables; standard Iceberg on a general purpose bucket is supported where S3
  Tables is not adopted. Handles one-time loads, recurring pipelines, migrations.
  Triggers on: import data, load data, ingest, sync database, migrate table, move
  data to AWS, set up pipeline, ETL, pull from Snowflake, query BigQuery into S3,
  export DynamoDB, CTAS, convert to Iceberg. Do NOT use for setting up or troubleshooting
  Glue connections (use connecting-to-data-source), creating empty tables (use
  creating-data-lake-table), running queries (use querying-data-lake), finding tables
  by fuzzy name (use finding-data-lake-assets), catalog audit (use exploring-data-catalog),
  or SaaS platforms like Salesforce, ServiceNow, SAP, MongoDB, Kafka.
version: 1
argument-hint: '[source-path|connection-name|table-name] [--target s3-tables|iceberg|parquet]'
---

# Ingest into Data Lake

Move data from a source into a queryable table in the data lake. This skill assumes the source connection (if one is needed) already exists. For Glue connection setup or troubleshooting, delegate to `connecting-to-data-source`.

## Philosophy

**Default to S3 Tables unless the environment says otherwise.** S3 Tables is the recommended target for new data lake work. If the user's catalog inventory shows they haven't adopted S3 Tables, recommend standard Iceberg on their existing general-purpose bucket instead of forcing them to change posture.

## Integration with Self-Serve Data Onboarding

This skill handles **Steps 11-12** of the master onboarding workflow defined in `steering/onboarding-workflow.md`:
- Step 11: Generate Pipeline Code (Glue ETL job execution)
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
