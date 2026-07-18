---
inclusion: always
---

# Ingestion Standards

> Always-on standard. Defines the non-negotiable engineering requirements for
> every auto-generated or manually created ingestion pipeline. Every pipeline
> Kiro generates MUST satisfy these constraints.

## 1. Landing Layer Rules

| Rule | Rationale |
|------|-----------|
| **Append-only bronze** | Raw data is never mutated; enables reprocessing and audit |
| **Iceberg snapshots enabled** | Downstream consumers get incremental changes efficiently via snapshot-based reads |
| **S3 paths only** | `s3://<bucket>/<source>/raw/` — no local paths, no hardcoded prefixes |
| **No business logic in bronze** | Raw capture only; transformations happen in silver |
| **Partitioned by ingestion date** | `_ingested_date` partition for time-travel and cost efficiency |

## 2. Mandatory Audit Columns

Every bronze table MUST include:

| Column | Type | Source |
|--------|------|--------|
| `_ingested_at` | TIMESTAMP | Pipeline execution time (UTC) |
| `_source_file` | STRING | Source file path / API endpoint / table name |
| `_batch_id` | STRING | UUID for this batch run |
| `_pipeline_version` | STRING | Git SHA or version tag of the pipeline code |
| `_row_hash` | STRING | SHA-256 of the full row (for deduplication) |
| `_ingested_date` | DATE | Partition key derived from `_ingested_at` |

## 3. Error Handling (Fail-Fast + Quarantine)

- **MUST NOT** silently drop rows. Every rejected row goes to a `_quarantine` table.
- **MUST** raise a CloudWatch Alarm / SNS alert when quarantine volume exceeds threshold (default: 5% of batch).
- **MUST** include error reason column in quarantine: `_error_reason STRING`.
- **MUST** halt the pipeline (fail the Glue job) if critical errors exceed 5% of the batch.

Quarantine table naming: `<database>.<table>_quarantine` (in Glue Data Catalog)

## 4. Deduplication

- Every pipeline MUST deduplicate within a batch using `_row_hash`.
- Cross-batch deduplication strategy is source-dependent (merge key in the data contract).
- Use Iceberg `MERGE INTO` for CDC patterns; conditional INSERT for append patterns.

## 5. Idempotency

- All pipelines MUST be idempotent — re-running a batch with the same data produces the same output.
- Use Iceberg `MERGE INTO` or conditional INSERT for exactly-once semantics.
- Glue job bookmarks or S3 event-driven triggers ensure no reprocessing of already-consumed files.

## 6. Security Tagging

- **MUST** auto-tag columns with Lake Formation tags: `pii`, `phi`, `sensitive`, `public`.
- Tagging is driven by the data contract; schema inference applies default rules for unspecified columns.
- Tag propagation to downstream tables is managed via Lake Formation tag-based access control (TBAC).

## 7. Source-System Isolation

- Each source lands in its own Glue database: `<source_name>_bronze`
- No cross-source joins in the bronze layer.
- Credentials stored in AWS Secrets Manager; never in code or config files.
- Secret naming convention: `ingestion/<source_name>/<credential_type>`

## 8. Incremental by Default

- All pipelines use **Glue job bookmarks** or **S3 Event Notifications** for incremental processing.
- Full loads only permitted for small reference tables (< 100MB) or initial backfill.
- Config flag: `load_type: incremental | full | cdc`

## 9. Table Properties

Every bronze Iceberg table MUST have these properties:
```sql
CREATE TABLE glue_catalog.<database>.<table> (
    ...
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
)
PARTITIONED BY (_ingested_date)
LOCATION 's3://<bucket>/<source>/<table>/'
```

## 10. Documentation

- Every pipeline MUST have: source description, owner, SLA, schema version, and data contract reference in Glue Data Catalog table parameters.
- Table description format: `"Ingested from <source_name> via <connection_method>. Owner: <email>. SLA: <hours>h. Contract: <contract_id>."`

## 11. Observability (Mandatory)

Every pipeline MUST emit metrics on every run:

| Metric | Emitted At |
|--------|-----------|
| `pipeline_start` | Job start |
| `rows_read` | After read |
| `rows_valid` | After validation |
| `rows_quarantined` | After validation |
| `rows_written` | After write |
| `quarantine_rate` | After validation |
| `duration_seconds` | Job end |
| `freshness_hours` | Job end (JDBC) |
| `status` (1=success, 0=failure) | Job end |

Metrics emitted to: CloudWatch custom metrics namespace `IngestionPipeline/<SourceDisplayName>`

## 12. Structured Logging

All pipeline logs MUST use JSON format:
```json
{
  "timestamp": "2025-07-18T14:30:00Z",
  "level": "INFO",
  "source_id": "src-ecommerce-orders-001",
  "batch_id": "uuid",
  "message": "Pipeline completed",
  "rows_written": 50000,
  "duration_seconds": 45.2
}
```

Never log credentials, secrets, or full PII values.

## 13. Standard Pipeline Components (Generated In Order)

Every generated pipeline MUST include these components in this order:

1. **Initialisation** — Set up logging, metrics, batch_id. Read pipeline config.
2. **Source Read** — Connect using Secrets Manager credentials. Read data (incremental/full/cdc).
3. **Pre-Transform Hook** — Source-specific cleaning (whitespace, encoding, normalization).
4. **Schema Validation** — Compare incoming schema against data contract. Handle drift.
5. **Contract Validation (DQ)** — Run quality rules from contract. Route failures to quarantine.
6. **Post-Transform Hook** — Derived columns, computed fields.
7. **Audit Column Injection** — Add all mandatory audit columns.
8. **Deduplication** — Hash-based within-batch. Merge-key-based cross-batch.
9. **Write to Bronze** — Append/MERGE to Iceberg table (Parquet, ZSTD, partitioned).
10. **Emit Metrics** — Duration, rows_read, rows_valid, rows_quarantined, rows_written, status.
11. **Update Watermark** — (JDBC only) Persist new watermark to DynamoDB.
