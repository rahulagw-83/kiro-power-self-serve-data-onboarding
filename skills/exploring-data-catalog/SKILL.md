---
name: exploring-data-catalog
description: >-
  Full inventory and audit of AWS Glue Data Catalog assets across S3 Tables, Redshift-federated,
  and remote Iceberg catalogs. Triggers on: inventory the catalog, audit databases,
  list all tables, catalog overview, data landscape, enumerate catalogs, data inventory,
  search the catalog. Do NOT use for finding specific data (use finding-data-lake-assets),
  running queries (use querying-data-lake), or creating tables (use creating-data-lake-table).
version: 2
argument-hint: '[search-term|catalog-name|database-name|s3://bucket-path|table-name]'
---

# Explore Data Catalog

Structured inventory and cataloging across your AWS data landscape: Glue Data Catalog with S3 Tables, Redshift-federated, and remote Iceberg catalogs.

## Overview

Maps data in an AWS account. Starts with catalog landscape (Glue, S3 Tables, federated), then drills into databases and tables. Read-only — no query execution.

## Integration with Self-Serve Data Onboarding

This skill supports:
- **Pre-onboarding assessment**: Understanding what already exists before bringing in new sources
- **Duplicate detection**: Verifying a source isn't already registered under a different name
- **Target selection**: Helping users choose the right database/namespace for their new pipeline
- **Platform health**: Auditing catalog completeness, stale tables, missing descriptions

**Cross-reference with Source Registry:**

```bash
aws dynamodb scan --table-name SourceRegistry \
  --projection-expression "source_name, source_type, #s, domain, target_table" \
  --expression-attribute-names '{"#s": "status"}'
```

Compare catalog assets against the registry to identify:
- Tables without a registered pipeline (orphaned)
- Registered sources without a corresponding table (failed onboarding)
- Tables in unexpected formats (should be Iceberg)

## Common Tasks

You MUST execute commands using AWS MCP server tools when connected. Fall back to AWS CLI only if MCP is unavailable.

### 1. Verify Dependencies

- You MUST verify AWS MCP server tools are available; fall back to AWS CLI if not
- You MUST confirm credentials with `aws sts get-caller-identity`

### 2. Discover Catalogs

List catalogs in account:

```bash
aws glue get-catalogs --recursive --include-root
```

Classify each catalog:

| Field Present | Catalog Type | Contents |
|---|---|---|
| Neither `TargetRedshiftCatalog` nor `FederatedCatalog` | **Default (Glue)** | Standard Glue databases and tables |
| `FederatedCatalog.ConnectionName` = `aws:s3tables` | **S3 Tables** | Managed Iceberg table buckets |
| `TargetRedshiftCatalog` | **Redshift-federated** | Redshift databases as Glue catalogs |
| `FederatedCatalog` with other `ConnectionName` | **Remote Iceberg** | External catalogs (Snowflake, Databricks) |

### 3. Enumerate Databases and Tables

For each catalog (or user-specified one):

```bash
aws glue get-databases --catalog-id <catalog-id>
aws glue get-tables --database-name <db> --catalog-id <catalog-id>
```

For S3 Tables catalogs:

```bash
aws s3tables list-table-buckets
aws s3tables list-namespaces --table-bucket-arn <arn>
aws s3tables list-tables --table-bucket-arn <arn> --namespace <ns>
```

### 4. Capture Details and Analyze

For each database capture:
- Table count and formats (Parquet, CSV, JSON, Iceberg)
- Partitioning strategies
- S3 locations
- Last access time and staleness
- Missing descriptions

**Platform-specific analysis:**
- Flag tables not using Iceberg format (platform standard)
- Flag tables without `_ingested_date` partition (missing platform audit columns)
- Flag databases without corresponding Source Registry entries
- Identify tables with quarantine counterparts (`{table}_quarantine`)

## Argument Routing

| User provides | Action |
|---|---|
| Starts with `s3://` | Explore unregistered data at that path |
| Matches a known catalog | Deep dive into that catalog |
| Matches a known database | Deep dive into that database |
| Matches a known table | Detailed table analysis with schema |
| No match | Treat as search term via `search-tables` |
| No args | Full landscape discovery |

## Principles

- Start with catalog landscape, then narrow based on user interest
- Always report catalog types — users need to know where data lives
- Always report data formats — they drive cost and performance decisions
- Flag stale tables and missing descriptions
- Summary first, details on request
- You MUST NOT execute Athena queries during discovery; query execution belongs to `querying-data-lake`

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| Only sub-catalogs returned, default missing | `--include-root` omitted | Re-run with `--include-root` |
| S3 Tables not queryable via Athena | Tables not registered in Glue | Flag and suggest registration |
| `get-databases`/`get-tables` fails with catalog-id | Default catalog needs account ID or omit | Omit `--catalog-id` for default catalog |
