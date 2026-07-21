# Hook — Post-Ingestion Checks

> Quality gate that runs AFTER data is written to the bronze table. Validates
> that the ingestion result meets expectations and contract requirements.

## When This Hook Runs

- After data is written to the bronze Iceberg table.
- Before the pipeline emits "end" metrics and exits.

## Checks Performed

### 1. Row Count Validation

```python
def check_row_counts(batch_metrics: BatchMetrics, contract: Contract) -> CheckResult:
    """
    Compare actual row counts against contract volume bounds.
    """
    rows_written = batch_metrics.rows_written
    min_expected = contract.volume.min_rows_per_batch
    max_expected = contract.volume.max_rows_per_batch
    
    if rows_written < min_expected:
        return CheckResult(status="warning", msg=f"Low volume: {rows_written} < {min_expected}")
    elif rows_written > max_expected:
        return CheckResult(status="warning", msg=f"High volume: {rows_written} > {max_expected}")
    else:
        return CheckResult(status="pass")
```

| Result | Action |
|--------|--------|
| Within bounds | Proceed |
| Below minimum | Alert (possible source issue) |
| Above maximum | Alert (possible data dump / backfill) |
| Zero rows written (unexpected) | Alert + investigate |

### 2. Quarantine Rate Check

```python
def check_quarantine_rate(batch_metrics: BatchMetrics, config: PipelineConfig) -> CheckResult:
    """
    Validate that quarantine rate is below threshold.
    """
    rate = batch_metrics.rows_quarantined / batch_metrics.rows_read
    threshold = config.observability.alert_rules.quarantine_rate_threshold_pct / 100
    
    if rate > threshold:
        return CheckResult(status="error", msg=f"Quarantine rate {rate:.1%} exceeds {threshold:.1%}")
    elif rate > threshold * 0.5:
        return CheckResult(status="warning", msg=f"Quarantine rate {rate:.1%} approaching threshold")
    else:
        return CheckResult(status="pass")
```

| Result | Action |
|--------|--------|
| Below threshold | Proceed |
| Approaching threshold | Alert (informational) |
| Exceeds threshold | FAIL pipeline + critical alert |

### 3. Audit Column Completeness

```python
def check_audit_columns(target_table: str, current_batch_id: str, glue_database: str) -> CheckResult:
    """
    Verify all mandatory audit columns are populated in the latest batch.
    Uses PySpark SQL within the Glue job context.
    """
    required = ["_ingested_at", "_source_file", "_batch_id", "_pipeline_version", "_row_hash"]
    
    # Check for NULLs in audit columns for this batch (PySpark SQL in Glue job)
    null_counts = spark.sql(f"""
        SELECT 
            {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as {col}_nulls' for col in required])}
        FROM glue_catalog.{glue_database}.{target_table}
        WHERE _batch_id = '{current_batch_id}'
    """)
```

| Result | Action |
|--------|--------|
| All populated | Proceed |
| Any audit column has NULLs | FAIL + critical alert (pipeline bug) |

### 4. Schema Consistency Check

```python
def check_schema_consistency(target_table: str, contract: Contract, glue_database: str) -> CheckResult:
    """
    Verify that the written data schema matches the contract.
    - All contract fields present in table (via Glue Catalog get_table or PySpark).
    - No unexpected type mismatches.
    """
    import boto3
    glue_client = boto3.client('glue')
    response = glue_client.get_table(DatabaseName=glue_database, Name=target_table)
    actual_columns = {col['Name']: col['Type'] for col in response['Table']['StorageDescriptor']['Columns']}
    
    missing = [f.name for f in contract.schema.fields if f.name not in actual_columns and not f.nullable]
    if missing:
        return CheckResult(status="error", msg=f"Missing required columns: {missing}")
    return CheckResult(status="pass")
```

| Result | Action |
|--------|--------|
| Schema matches | Proceed |
| Extra columns (error output) | Alert (schema drift detected) |
| Missing required columns | FAIL + critical alert |

### 5. Freshness Validation

```python
def check_freshness(batch_metrics: BatchMetrics, contract: Contract) -> CheckResult:
    """
    Check if the data we just ingested meets the freshness SLA.
    Compare: max(source_timestamp) vs. now()
    """
    max_source_ts = batch_metrics.max_source_timestamp
    now = datetime.utcnow()
    hours_stale = (now - max_source_ts).total_seconds() / 3600
    
    if hours_stale > contract.freshness.critical_after_hours:
        return CheckResult(status="error", msg=f"Data is {hours_stale:.1f}h stale (critical: {contract.freshness.critical_after_hours}h)")
    elif hours_stale > contract.freshness.alert_after_hours:
        return CheckResult(status="warning", msg=f"Data is {hours_stale:.1f}h stale (alert: {contract.freshness.alert_after_hours}h)")
    else:
        return CheckResult(status="pass")
```

| Result | Action |
|--------|--------|
| Within SLA | Proceed |
| Approaching SLA | Alert (informational) |
| Breaching SLA | Alert + escalate to source team |

### 6. Deduplication Verification

```python
def check_deduplication(target_table: str, merge_key: List[str], current_batch_id: str, glue_database: str) -> CheckResult:
    """
    Confirm no duplicates on the merge key in the latest batch.
    Uses PySpark SQL within the Glue job context.
    """
    dup_count = spark.sql(f"""
        SELECT COUNT(*) - COUNT(DISTINCT {', '.join(merge_key)}) as dup_count
        FROM glue_catalog.{glue_database}.{target_table}
        WHERE _batch_id = '{current_batch_id}'
    """).first().dup_count
```

| Result | Action |
|--------|--------|
| Zero duplicates | Proceed |
| < 0.1% duplicates | Alert (dedup edge case; log for review) |
| > 0.1% duplicates | Alert + investigate dedup logic |

## Hook Output

```python
@dataclass
class PostIngestionResult:
    passed: bool
    checks: List[CheckResult]
    metrics: Dict[str, Any]     # row_count, quarantine_rate, freshness_hours, etc.
    warnings: List[str]
    errors: List[str]
```

## Behaviour on Failure

- If quarantine rate OR audit columns OR schema check fails:
  - Pipeline status = FAILED.
  - Alert emitted immediately via CloudWatch Alarm + SNS.
  - Data already written is NOT rolled back (append-only bronze is auditable via Iceberg snapshots).
  - Next run will process new data; failed rows remain in quarantine.

- If volume bounds or freshness have warnings:
  - Pipeline status = SUCCESS (with warnings).
  - Alerts emitted for investigation.

## MUST Rules

- **MUST** run all post-ingestion checks after every write.
- **MUST** fail the pipeline if quarantine rate exceeds threshold.
- **MUST** verify audit column completeness (pipeline integrity check).
- **MUST** emit all check results as metrics to CloudWatch.
- **MUST** log detailed error information for failed checks.
- **MUST** complete all checks within 60 seconds (timeout and warn if longer).
