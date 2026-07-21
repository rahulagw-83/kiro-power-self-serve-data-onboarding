# Hook — Pre-Ingestion Validation

> Quality gate that runs BEFORE data is read from the source. Applied to every
> generated pipeline. Ensures preconditions are met before committing resources
> to a read operation.

## When This Hook Runs

- At pipeline start, BEFORE the source read operation.
- After initialisation (batch_id generated, metrics started).

## Checks Performed

### 1. Source Connectivity Test

```python
def check_connectivity(config: PipelineConfig) -> CheckResult:
    """
    Verify that the source is reachable and responsive.
    - JDBC: attempt connection via Glue Connection with timeout (30s)
    - API: send health check / lightweight GET with timeout (30s)
    - S3: list objects in path (confirm access via boto3)
    - Kafka: describe topic (confirm broker reachable)
    """
```

| Result | Action |
|--------|--------|
| Reachable | Proceed |
| Unreachable | Retry (3x with backoff) → if still down, FAIL pipeline + alert |

### 2. Credential Validity

```python
def check_credentials(config: PipelineConfig) -> CheckResult:
    """
    Verify that stored credentials are still valid.
    - Retrieve secret from Secrets Manager (boto3 secretsmanager get_secret_value)
    - Attempt authenticated operation (e.g., lightweight query, token introspection)
    - Do NOT log the credential value
    """
```

| Result | Action |
|--------|--------|
| Valid | Proceed |
| Expired/Invalid | FAIL pipeline + alert with actionable message ("OAuth token expired; rotate in Secrets Manager secret: ingestion/<source>/oauth_token") |

### 3. Source Freshness Check

```python
def check_source_freshness(config: PipelineConfig, registry: SourceRegistry) -> CheckResult:
    """
    For incremental pipelines: verify that source has new data since last run.
    - Compare current watermark at source vs. last processed watermark (stored in DynamoDB).
    - If no new data: SKIP (no-op) — do not fail.
    """
```

| Result | Action |
|--------|--------|
| New data available | Proceed |
| No new data | Skip batch (log "no new data"; emit metric; exit 0) |
| Cannot determine | Proceed (full safety read) |

### 4. Target Table Health

```python
def check_target_table(config: PipelineConfig) -> CheckResult:
    """
    Verify the target Iceberg table is accessible via Glue Catalog:
    - Table exists (or will be created on first run)
    - No concurrent compaction blocking writes
    - Schema matches expected (from last known good)
    
    Uses boto3 glue client:
      glue_client.get_table(DatabaseName=db, Name=table)
    """
    import boto3
    glue_client = boto3.client('glue')
    
    try:
        response = glue_client.get_table(
            DatabaseName=config.target.glue_database,
            Name=config.target.table
        )
        # Verify table is Iceberg format
        table_params = response['Table'].get('Parameters', {})
        if table_params.get('table_type') != 'ICEBERG':
            return CheckResult(status="warning", msg="Target table is not Iceberg format")
        return CheckResult(status="pass")
    except glue_client.exceptions.EntityNotFoundException:
        # First run — table will be created
        return CheckResult(status="pass", msg="Table does not exist yet; will be created on first write")
    except Exception as e:
        return CheckResult(status="error", msg=f"Cannot access target table: {str(e)}")
```

| Result | Action |
|--------|--------|
| Healthy / accessible | Proceed |
| Table locked (concurrent compaction) | Wait (up to 5 min) → retry → FAIL if still locked |
| Table inaccessible | FAIL + critical alert |

### 5. Contract Availability

```python
def check_contract(config: PipelineConfig) -> CheckResult:
    """
    Verify that the data contract is:
    - Exists in the contracts store (DynamoDB: platform-data-contracts)
    - Status is 'active' (not deprecated/retired)
    - Version matches what the pipeline expects
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('platform-data-contracts')
    
    response = table.get_item(Key={'contract_id': config.contract.id})
    item = response.get('Item')
    
    if not item:
        return CheckResult(status="error", msg="Contract not found in registry")
    if item['status'] == 'retired':
        return CheckResult(status="error", msg="Contract retired; pipeline needs update")
    if item['version'] != config.contract.version:
        return CheckResult(status="warning", msg=f"Contract version mismatch: expected {config.contract.version}, found {item['version']}")
    return CheckResult(status="pass")
```

| Result | Action |
|--------|--------|
| Active & matched | Proceed |
| Version mismatch | Alert (contract evolved); attempt with latest version |
| Not found / retired | FAIL + alert ("Contract retired; pipeline needs update") |

## Hook Output

```python
@dataclass
class PreIngestionResult:
    passed: bool
    checks: List[CheckResult]   # individual check outcomes
    skip_batch: bool            # True if no new data (graceful skip)
    warnings: List[str]         # non-blocking issues to log
    errors: List[str]           # blocking issues (if passed=False)
```

## Behaviour on Failure

- If ANY check returns error: pipeline FAILS immediately.
  - Error details logged.
  - Alert emitted via CloudWatch + SNS.
  - No source read attempted (saves cost).
- If all checks pass but some have warnings: pipeline proceeds with warnings logged.
- If `skip_batch = True`: pipeline exits with SUCCESS status + "no new data" metric.

## MUST Rules

- **MUST** run all pre-ingestion checks before any data is read.
- **MUST** timeout checks at 60 seconds total (fail fast).
- **MUST** log all check results (for audit trail).
- **MUST** never expose credentials in check error messages.
- **MUST** emit `pre_ingestion_check_duration_ms` metric to CloudWatch.
