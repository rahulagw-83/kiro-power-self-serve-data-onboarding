---
name: creating-data-lake-table
description: >-
  Create managed Iceberg tables using Amazon S3 Tables (s3tables API namespace) with
  automatic compaction and snapshot management. Sets up table bucket, namespace, table,
  schema, Glue catalog registration, partitioning, IAM access control. Triggers on:
  create table, data lake table, analytics table, structured data storage, S3 Tables,
  Iceberg, Athena table, partitioning strategy, access permissions. Do NOT use for:
  importing files (use ingesting-into-data-lake), vector storage (use storing-and-querying-vectors),
  querying existing tables (use querying-data-lake), or locating existing table (use
  finding-data-lake-assets).
version: 1
argument-hint: '[table-description|schema-spec]'
---

# Create Data Lake Tables with Amazon S3 Tables

## Overview

Amazon S3 Tables provides managed Iceberg tables with automatic compaction and snapshot management. Queryable via Athena and Iceberg-compatible engines.

## Integration with Self-Serve Data Onboarding

This skill is invoked during **Step 11 (Generate Pipeline Code)** when the target Iceberg table does not yet exist. It creates the bronze-layer landing table that pipelines write to.

**Platform conventions enforced:**
- Table names: all lowercase, snake_case (e.g., `orders_raw`, `customers_bronze`)
- Namespace: defaults to the source's `domain` from `onboarding-request.yaml`
- Partition: by `_ingested_date` (date type, derived from `_ingested_at` timestamp)
- Audit columns MUST be included in every table schema: `_ingested_at`, `_source_file`, `_batch_id`, `_pipeline_version`, `_row_hash`, `_ingested_date`
- Quarantine table: `{table_name}_quarantine` with additional `_error_reason` and `_quarantined_at` columns

## Common Tasks

You MUST use AWS MCP server tools when connected. Fall back to AWS CLI if MCP unavailable.

## Decision Guide

**Before creating, check what exists:**

```bash
aws glue get-tables --database-name <NAME>
```

| What you find | Action |
|---|---|
| Fuzzy database name | STOP. Delegate to `finding-data-lake-assets` to resolve. |
| Existing table with matching name | Check schema match. Reuse if compatible, recreate only if user confirms. |
| No matching tables | Proceed with creation. |

## Workflow

### 1. Verify Dependencies

- You MUST confirm target AWS region and verify credentials with `aws sts get-caller-identity`

### 2. Understand the Schema

Sources of schema:
- **From data contract** (preferred): Read `data-contract.yaml` for column definitions
- **From schema inference** (Step 6 of onboarding): Use inferred schema + sensitivity classification
- **Explicit from user**: Validate Iceberg types
- **From existing S3 data**: Infer schema from file headers

**Platform schema requirements:**
Every bronze table MUST include these audit columns in addition to source columns:

```json
{"name": "_ingested_at", "type": "timestamp", "required": true},
{"name": "_source_file", "type": "string", "required": true},
{"name": "_batch_id", "type": "string", "required": true},
{"name": "_pipeline_version", "type": "string", "required": true},
{"name": "_row_hash", "type": "string", "required": true},
{"name": "_ingested_date", "type": "date", "required": true}
```

### 3. Create Table Bucket (if needed)

```bash
aws s3tables list-table-buckets
# If no bucket exists:
aws s3tables create-table-bucket --name <BUCKET_NAME> --region <REGION>
```

### 4. Create Namespace

```bash
aws s3tables create-namespace --table-bucket-arn <ARN> --namespace <NAMESPACE>
```

Namespace defaults to the `domain` field from the onboarding request (e.g., `sales`, `finance`, `marketing`).

### 5. Create Glue Data Catalog Integration

Check if `s3tablescatalog` exists:

```bash
aws glue get-catalog --catalog-id s3tablescatalog
```

If not found, create the federated catalog:

```bash
aws glue create-catalog --name "s3tablescatalog" --catalog-input '{
  "FederatedCatalog": {
    "Identifier": "arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/*",
    "ConnectionName": "aws:s3tables"
  },
  "CreateDatabaseDefaultPermissions": [{"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}],
  "CreateTableDefaultPermissions": [{"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}],
  "AllowFullTableExternalDataAccess": "True"
}'
```

### 6. Configure Access Control

S3 Tables uses `s3tables:*` IAM namespace (not `s3:*`).

Minimum permissions for the Glue ETL role:
- `s3tables:GetTableBucket`, `s3tables:GetNamespace`, `s3tables:GetTable`
- `s3tables:GetTableMetadataLocation`, `s3tables:GetTableData`, `s3tables:PutTableData`
- `glue:GetCatalog`, `glue:GetDatabase`, `glue:GetTable`

### 7. Create the Table

```bash
aws s3tables create-table \
  --table-bucket-arn <ARN> \
  --namespace <NAMESPACE> \
  --name <TABLE_NAME> \
  --format ICEBERG \
  --metadata '<METADATA_JSON>'
```

Also create the quarantine table:

```bash
aws s3tables create-table \
  --table-bucket-arn <ARN> \
  --namespace <NAMESPACE> \
  --name <TABLE_NAME>_quarantine \
  --format ICEBERG \
  --metadata '<QUARANTINE_METADATA_JSON>'
```

### 8. Verify

```bash
aws s3tables get-table --table-bucket-arn <ARN> --namespace <NS> --name <TABLE>
```

Confirm queryability via Athena with `DESCRIBE` using the S3 Tables catalog context.

## Gotchas

- Table names MUST be all lowercase with no hyphens
- S3 Tables MUST NOT have a LOCATION clause (storage is managed automatically)
- `partitionSpec.sourceId` MUST reference a valid schema field ID
- S3 Tables uses `s3tables:*` IAM namespace, not `s3:*`
- Encryption is immutable after bucket creation

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| "Table location can not be specified" | LOCATION in CREATE TABLE | Remove LOCATION clause |
| `AccessDeniedException` with `s3:*` policy | Using wrong IAM namespace | Use `s3tables:*` permissions |
| `GENERIC_INTERNAL_ERROR` | Mixed case in names | Use all lowercase |
