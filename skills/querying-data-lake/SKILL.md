---
name: querying-data-lake
description: >-
  Execute and manage Athena SQL queries across default and federated catalogs (Glue,
  S3 Tables, Redshift). Triggers on phrases like: query data, run SQL, athena query,
  analyze table, SQL query, workgroup status, profile table, query Redshift catalog,
  query S3 Tables. Do NOT use for finding specific data assets (use finding-data-lake-assets),
  full catalog audits (use exploring-data-catalog), importing data (use ingesting-into-data-lake).
version: 1
argument-hint: '[SQL-query|query-name|workgroup-name|catalog-name|''profile TABLE_NAME'']'
---

# Query Data Lake

Execute SQL queries on Amazon Athena across default and federated catalogs (Glue, S3 Tables, Redshift) with workgroup selection, statement classification, and error recovery.

## Integration with Self-Serve Data Onboarding

This skill is used during:
- **Step 12 (Smoke Test)**: Validate pipeline output by querying the target table
- **Step 14 (Integration Tests)**: Run full-volume validation queries
- **Post-ingestion checks**: Verify row counts, quarantine rates, and data freshness

**Platform query patterns:**

```sql
-- Verify pipeline output (smoke test)
SELECT COUNT(*) as row_count,
       MAX(_ingested_at) as latest_ingestion,
       COUNT(DISTINCT _batch_id) as batch_count
FROM {database}.{table}
WHERE _ingested_date = CURRENT_DATE;

-- Check quarantine rate
SELECT COUNT(*) as quarantined_rows, _error_reason
FROM {database}.{table}_quarantine
WHERE _quarantined_at > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY _error_reason;

-- Freshness SLA check
SELECT ROUND(
  DATE_DIFF('hour', MAX(_ingested_at), CURRENT_TIMESTAMP)
) as hours_since_last_ingestion
FROM {database}.{table};
```

## Common Tasks

### 1. Verify Dependencies

- You MUST verify AWS MCP server tools are available; fall back to AWS CLI if not
- You MUST confirm credentials with `aws sts get-caller-identity`

### 2. Resolve Workgroup

List available workgroups and select the appropriate one:

```bash
aws athena list-work-groups --query 'WorkGroups[].Name'
```

You MUST select a workgroup before submitting any query (prevents output-location errors).

### 3. Resolve the Target Asset

If the user refers to a table by business name, delegate to `finding-data-lake-assets` to resolve the concrete `database.table` reference.

### 4. Discover Schema

For analytical queries, profile the target table before building the final query:

```bash
aws athena start-query-execution \
  --work-group <WORKGROUP> \
  --query-string "DESCRIBE {database}.{table}" \
  --query-execution-context Database=<db>
```

### 5. Build Query

Table addressing depends on catalog type:
- Default Glue catalog: `database.table`
- S3 Tables: Use `--query-execution-context Catalog=s3tablescatalog/<BUCKET_NAME>,Database=<NAMESPACE>`
- Redshift federated: `datasource.database.table`

### 6. Classify and Execute

| Statement | Behavior |
|---|---|
| `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN` | Safe — execute |
| `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `MERGE` | Destructive — warn the user and require explicit confirmation |

```bash
aws athena start-query-execution \
  --work-group <WORKGROUP_NAME> \
  --query-string '<sql>' \
  --query-execution-context Database=<db>
```

### 7. Present Results

Present results with cost, data scanned, duration, and actionable insights.

```bash
aws athena get-query-results --query-execution-id <ID>
```

## Argument Routing

| User provides | Action |
|---|---|
| SQL keywords (SELECT, SHOW, etc.) | SQL text — execute directly |
| `profile TABLE_NAME` | Run comprehensive table profiling |
| Workgroup name | Show workgroup status and recent queries |
| Catalog name | Delegate to `exploring-data-catalog` |
| No args | Show recent query activity |

## Gotchas

- Redshift-federated: no partition pruning — every query scans full table. Warn user.
- Cross-catalog joins incur network overhead — warn user.
- S3 Tables catalog path: `s3tablescatalog/<BUCKET_NAME>` as the Catalog context parameter
- Do NOT put catalog in SQL — use `--query-execution-context Catalog=...`

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| Output-location error | Workgroup has no output location configured | Select different workgroup or configure one |
| Redshift identifier error with mixed case | Redshift names are lowercase only | Lowercase the identifier |
| Cross-catalog `information_schema` returns nothing | Missing catalog qualifier | Use `"catalog".information_schema.tables` |
| `HIVE_CANNOT_OPEN_SPLIT` | File missing or permissions issue | Check S3 path and Glue role permissions |
