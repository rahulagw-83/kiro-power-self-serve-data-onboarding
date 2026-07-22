---
inclusion: always
---

# Ingestion Standards

> Always-on standard. Defines the non-negotiable engineering requirements for
> every auto-generated or manually created ingestion pipeline. Every pipeline
> Kiro generates MUST satisfy these constraints.

## 1. Two-Layer Architecture Rules

| Rule | Rationale |
|------|-----------|
| **Landing = raw copy** | No transforms, no validation, no masking in Landing |
| **Raw = governed Iceberg** | Audit columns, DQDL quality, dedup, Lake Formation tags |
| **Glue reads from S3 only** | Never read directly from source via JDBC/API in Glue |
| **Partitioned Landing** | `year=YYYY/month=MM/day=DD/` for time-bounded reprocessing |
| **90-day Landing retention** | S3 lifecycle rule deletes Landing files after 90 days |
| **EventBridge trigger** | New Landing files → EventBridge → Step Functions → Glue |

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

## 5. Data Quality: DQDL (Glue Data Quality)

- **MUST** compile data contract quality rules to DQDL syntax at generation time.
- **MUST NOT** maintain custom Python validator classes — use `EvaluateDataQuality` transform.
- **MUST** enable CloudWatch metrics publishing for every DQDL evaluation.
- **MUST** route DQDL failures to `{table}_quarantine` Iceberg table.
- **MUST** halt pipeline if quarantine rate exceeds contract threshold (default 5%).
- See `steering/glue-data-quality.md` for full DQDL compilation reference.

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

## 14. Intelligent Worker Sizing

Replace static timeout/sizing values with calculated ones:

```
timeout_minutes = 10 (provisioning buffer)
               + (estimated_volume_gb / throughput_per_dpu_gb_per_min)
               × 1.2 (20% safety margin)

For initial full loads: timeout × 3 (first run reads everything)
For S3-sourced jobs (Landing → Raw): worker_count × 0.5
  (S3 reads are ~2× faster and more parallelizable than JDBC reads)
```

| Volume per Batch | Worker Type | Workers | Base Timeout |
|---|---|---|---|
| < 1 GB | G.1X | 2 | 15 min |
| 1–10 GB | G.1X | 5 | 30 min |
| 10–50 GB | G.1X | 10 | 60 min |
| > 50 GB | G.2X | 10 | 120 min |

**Glue Flex Execution Class:**
For non-time-sensitive batch jobs (SLA > 2 hours), default to Flex execution class for up to 35% cost reduction. Flex jobs start within 5 minutes (vs immediate for Standard).

**Worker Type Rules:**
- G.025X is ONLY valid for `gluestreaming` command type — auto-correct to G.1X for `glueetl`
- G.4X and G.8X are for memory-intensive workloads (large joins, complex aggregations)
- Validate worker type against job command type BEFORE creating the Glue job

## 15. Pre-Deployment Validation (Mandatory)

Before generating or deploying ANY pipeline, ALL checks in `steering/pre-deployment-validation.md` MUST pass:
1. Connectivity (can we reach the source?)
2. Credentials (secret exists, keys valid, not stale)
3. Networking (SG self-reference, subnet routes, S3 VPC endpoint)
4. IAM (roles have all required permissions)
5. ASL (state machine definition valid)
6. Worker Type (valid for this job command type)

If ANY check fails → STOP. Do NOT generate infrastructure. Fix first, then retry.
