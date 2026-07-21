---
name: finding-data-lake-assets
description: >-
  Resolve data lake and lakehouse asset references across Glue Data Catalog, S3, S3
  Tables, and Redshift. Triggers on: find the table, where is our data, which table
  has, locate dataset, find data for, search catalog, what tables match, Redshift
  table, lakehouse table, data lake table, warehouse table, reverse lookup S3 path.
  Do NOT use for: full catalog audits (use exploring-data-catalog), running queries
  (use querying-data-lake), creating tables (use creating-data-lake-table).
version: 2
argument-hint: '[table-name|keyword|column-name|s3://path]'
---

# Find Data Lake Assets

## Overview

Resolves data lake asset references to concrete catalog entries. Acts as a resolver for other skills and direct user requests. Covers Glue, S3, S3 Tables, and Redshift. Optimized for low token usage — return the answer fast and get out of the way.

## Integration with Self-Serve Data Onboarding

This skill is used during:
- **Step 2 (Duplicate Check)**: Verify if a source is already registered before onboarding
- **Step 4 (Connection Setup)**: Find existing connections and candidate sources
- **Step 11 (Pipeline Generation)**: Resolve target table references
- **Any time** a user references a table by business name rather than exact catalog path

**Cross-reference with Source Registry:** After finding catalog assets, cross-check against the DynamoDB Source Registry to determine onboarding status:

```bash
aws dynamodb scan --table-name SourceRegistry \
  --filter-expression "source_name = :n" \
  --expression-attribute-values '{":n": {"S": "<source_name>"}}'
```

## Common Tasks

You MUST execute commands using AWS MCP server tools when connected. Fall back to AWS CLI only if MCP is unavailable.

### 1. Verify Dependencies

- You MUST verify AWS MCP server tools are available; fall back to AWS CLI if not
- You MUST confirm credentials with `aws sts get-caller-identity`

### 2. Classify the Request

- **Resolve** (most common): User/skill references something specific. Goal: find it, return the reference.
- **Search**: User is exploring. Goal: rank candidates, present top matches.

Default to Resolve mode when ambiguous.

### 3. Extract Search Terms

Parse the request into dimensions:
- **Name terms**: Table or database names mentioned
- **Domain terms**: Business concepts (billing, orders, churn)
- **Column terms**: Specific column names (customer_id, event_type)
- **Location terms**: S3 paths, bucket names, prefixes

### 4. Layered Search (stop early)

Search sources in order. Stop at the first layer that returns a high-confidence match.

**Layer 1: Glue Data Catalog** (always start here)

```bash
aws glue search-tables --search-text "orders"
aws glue get-tables --database-name <db> --expression "order.*"
```

**Layer 2: S3 Reverse Lookup** (S3 path provided)

```bash
aws glue search-tables --search-text "<path-keyword>"
aws s3api list-objects-v2 --bucket <bucket-name> --prefix <prefix>
```

**Layer 3: S3 Tables** (if Layer 1 returns nothing for expected S3 Tables asset)

```bash
aws s3tables list-table-buckets
aws s3tables list-namespaces --table-bucket-arn <arn>
aws s3tables list-tables --table-bucket-arn <arn> --namespace <ns>
```

**Layer 4: Redshift Catalog** (if user mentions Redshift, warehouse, or lakehouse)

Run via Athena federated query or direct Redshift Data API.

### 5. Apply Confidence Gate

- **High confidence** (exact name match, single result): Return immediately.
- **Medium confidence** (fuzzy match, 2-3 results): Present top matches, let user pick.
- **Low confidence** (many weak matches or none): Suggest refining or running `exploring-data-catalog`.

### 6. Return the Reference

```
Table: database_name.table_name
Catalog: default | s3tablescatalog/<bucket>
Format: Parquet | CSV | JSON | ORC | Iceberg
Location: s3://bucket/prefix/
Partition keys: [key1, key2] or none
Onboarding status: active | pending | not registered
Sources searched: Glue Data Catalog, S3 Tables
Sources skipped: Redshift (stopped early)
```

## Broad Scan Fallback

When `search-tables` returns nothing, use a boto3 paginator script to scan across databases:

```python
import boto3, sys, json

region = sys.argv[1]
term = sys.argv[2]
glue = boto3.client("glue", region_name=region)
matches = []

db_paginator = glue.get_paginator("get_databases")
for db_page in db_paginator.paginate():
    for db in db_page["DatabaseList"]:
        tbl_paginator = glue.get_paginator("get_tables")
        for tbl_page in tbl_paginator.paginate(
            DatabaseName=db["Name"], Expression=f".*{term}.*"
        ):
            for tbl in tbl_page["TableList"]:
                matches.append({
                    "database": db["Name"],
                    "table": tbl["Name"],
                    "format": tbl.get("Parameters", {}).get("classification", "unknown"),
                    "location": tbl.get("StorageDescriptor", {}).get("Location", ""),
                })

print(json.dumps(matches, indent=2) if matches else "No matches found.")
```

## Principles

- Prefer `search-tables` over iterating databases (one API call beats N)
- Resolve fast and stop early — every extra API call costs tokens
- Always report which sources were searched and which were skipped
- MUST pass an `Expression` filter when calling `get-tables`

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `search-tables` returns nothing for S3 Tables | Does not cover federated catalogs | Use `aws s3tables list-table-buckets` |
| `AccessDeniedException` on `search-tables` | Missing `glue:SearchTables` permission | Request permission or fall back to `get-tables` with known database |
| `get-tables` fails | Requires `--database-name` | Use `search-tables` for cross-database search |
