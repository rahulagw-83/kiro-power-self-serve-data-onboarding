# Pipeline Generation

## Purpose

Transform pipeline configuration YAML into production-ready, deployable code. Zero plumbing for common patterns. The generator handles all boilerplate — connection management, error handling, retry logic, observability, and infrastructure — so teams focus exclusively on business logic and data contracts.

---

## Architecture

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│ Pipeline Config │────▶│ Source Connectors  │────▶│ Contract Validation │
│     (YAML)      │     │ (JDBC/API/S3/Stream)│     │  (Schema + Rules)   │
└─────────────────┘     └───────────────────┘     └─────────────────────┘
                                                            │
                        ┌───────────────────────────────────┘
                        ▼
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│ Schema Validator│────▶│      Dedup        │────▶│   Audit Columns     │
│ (Type + Nulls)  │     │ (Key + Watermark) │     │ (ingested_at, etc.) │
└─────────────────┘     └───────────────────┘     └─────────────────────┘
                                                            │
                        ┌───────────────────────────────────┘
                        ▼
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│Quarantine Router│────▶│  Iceberg Writer   │────▶│Observability Emitter│
│(Bad rows → DLQ) │     │ (Append/Merge/SCD)│     │(Metrics + Lineage)  │
└─────────────────┘     └───────────────────┘     └─────────────────────┘
```

---

## Template Selection

| Source Type      | Template                    | Key Capabilities                              |
|------------------|-----------------------------|-----------------------------------------------|
| RDBMS            | `jdbc_ingestion_template`   | Watermark-based incremental, merge/upsert     |
| REST API         | `api_ingestion_template`    | Pagination, OAuth 2.0, rate limiting          |
| Cloud Storage    | `s3_incremental_template`   | Bookmark tracking, schema inference           |
| Streaming        | `streaming_template`        | Spark Structured Streaming, checkpointing     |
| SaaS             | `connector_template`        | API Gateway + Lambda, webhook support         |

Template selection is automatic based on the `source.type` field in the pipeline config. Override with `template_override` if needed.

---

## Input: Pipeline Config YAML

```yaml
pipeline:
  name: salesforce-opportunities-ingestion
  version: "1.0"
  owner: revenue-analytics-team
  description: "Ingest Salesforce Opportunity records into the data lake"

source:
  type: saas
  connector: salesforce
  auth_type: oauth2_jwt
  credentials_secret: "arn:aws:secretsmanager:us-east-1:123456789:secret:sf-api-creds"
  endpoints:
    - object: Opportunity
      api_version: "v58.0"
      fields:
        - Id
        - Name
        - Amount
        - StageName
        - CloseDate
        - AccountId
        - OwnerId
        - CreatedDate
        - LastModifiedDate
      filters:
        - "LastModifiedDate >= {{last_watermark}}"
  rate_limit:
    requests_per_second: 10
    burst: 25
    retry_on_429: true
  pagination:
    type: cursor
    page_size: 2000

load_type: incremental
watermark_column: LastModifiedDate
merge_keys:
  - Id

schedule:
  cron: "0 */4 * * *"   # Every 4 hours
  timezone: UTC
  retry_attempts: 3
  retry_delay_minutes: 15

target:
  catalog: glue_catalog
  database: bronze_salesforce
  table: opportunities
  format: iceberg
  write_mode: merge
  partition_by:
    - column: CloseDate
      transform: month
  location: "s3://data-lake-bronze/salesforce/opportunities/"

contract:
  schema_version: "1.0"
  required_columns:
    - name: Id
      type: string
      nullable: false
    - name: Amount
      type: decimal(18,2)
      nullable: true
    - name: StageName
      type: string
      nullable: false
    - name: CloseDate
      type: date
      nullable: false
  quality_rules:
    - rule: not_null
      columns: [Id, StageName, CloseDate]
    - rule: unique
      columns: [Id]
    - rule: accepted_values
      column: StageName
      values: [Prospecting, Qualification, Proposal, Negotiation, Closed Won, Closed Lost]
    - rule: range
      column: Amount
      min: 0
      max: 999999999

observability:
  emit_metrics: true
  namespace: "DataPipeline/Salesforce"
  alerts:
    - metric: quarantine_rate
      threshold: 0.05
      action: sns_notify
    - metric: freshness_sla_minutes
      threshold: 300
      action: pagerduty
  lineage:
    enabled: true
    catalog: datahub
```

---

## Output: Generated Artifacts

| Artifact                    | Path                              | Description                                      |
|-----------------------------|-----------------------------------|--------------------------------------------------|
| Glue job script             | `src/glue_jobs/{pipeline_name}.py`| PySpark ETL with all pipeline components         |
| Step Functions definition   | `src/orchestration/sfn.json`      | State machine with validation, retries, alerts   |
| Unit tests                  | `tests/test_{pipeline_name}.py`   | Contract tests, transform tests, mock sources    |
| CDK stack                   | `cdk/stacks/{pipeline_name}.py`   | IAM roles, Glue jobs, schedules, alarms          |
| Generation report           | `reports/generation_report.md`    | Decisions made, warnings, config validation      |

---

## Standard Pipeline Components (In Order)

Every generated pipeline includes these 11 components executed sequentially:

| #  | Component               | Responsibility                                                    |
|----|-------------------------|-------------------------------------------------------------------|
| 1  | Initialization          | Parse args, init Spark/Glue context, load config                  |
| 2  | Source Connection        | Establish connection using secrets, apply auth strategy            |
| 3  | Data Extraction          | Read from source with watermark/bookmark, handle pagination       |
| 4  | Contract Validation      | Validate schema version, required columns, types                  |
| 5  | Schema Enforcement       | Cast types, handle nullability, reject malformed rows             |
| 6  | Deduplication            | Apply dedup logic using merge keys + watermark ordering           |
| 7  | Custom Transforms        | Execute user-defined pre/post transform hooks                     |
| 8  | Audit Column Injection   | Add `_ingested_at`, `_source_file`, `_batch_id`, `_row_hash`     |
| 9  | Quarantine Routing       | Route invalid rows to quarantine table with failure reasons       |
| 10 | Iceberg Write            | Write valid rows using configured write mode (append/merge/SCD)   |
| 11 | Emit Metrics             | Publish CloudWatch metrics, update lineage, log summary           |

---

## Custom Logic Injection Points

Users inject business logic without modifying generated code:

```python
# custom_hooks/{pipeline_name}_hooks.py

def pre_transform(df, context):
    """
    Called after extraction, before schema enforcement.
    Use for source-specific data cleansing.
    """
    # Example: normalize currency codes
    from pyspark.sql.functions import upper, trim
    df = df.withColumn("CurrencyCode", upper(trim(df["CurrencyCode"])))
    return df


def post_transform(df, context):
    """
    Called after schema enforcement, before dedup.
    Use for derived columns or enrichment.
    """
    from pyspark.sql.functions import concat, lit
    df = df.withColumn(
        "opportunity_key",
        concat(df["AccountId"], lit("_"), df["Id"])
    )
    return df


def custom_dedup_key(df, context):
    """
    Override default dedup logic.
    Return the DataFrame with duplicates removed using custom strategy.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    window = Window.partitionBy("Id").orderBy(col("LastModifiedDate").desc())
    df = df.withColumn("_rank", row_number().over(window))
    df = df.filter(col("_rank") == 1).drop("_rank")
    return df
```

Hook discovery is automatic: the generator looks for `custom_hooks/{pipeline_name}_hooks.py` and wires in any defined functions.

---

## Generated Step Functions Definition

```json
{
  "Comment": "Pipeline orchestration: salesforce-opportunities-ingestion",
  "StartAt": "PreIngestionValidation",
  "States": {
    "PreIngestionValidation": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${region}:${account}:function:pre-ingestion-validator",
      "Parameters": {
        "pipeline_name": "salesforce-opportunities-ingestion",
        "checks": ["connectivity", "credentials", "source_freshness", "target_health", "contract_available"]
      },
      "Retry": [
        {
          "ErrorEquals": ["ConnectivityError", "CredentialError"],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyOnFailure"
        }
      ],
      "Next": "RunIngestion"
    },
    "RunIngestion": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "salesforce-opportunities-ingestion",
        "Arguments": {
          "--pipeline_config": "s3://pipeline-configs/salesforce-opportunities-ingestion.yaml",
          "--execution_id.$": "$$.Execution.Id"
        }
      },
      "Retry": [
        {
          "ErrorEquals": ["Glue.EntityNotFoundException"],
          "MaxAttempts": 0
        },
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 120,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyOnFailure"
        }
      ],
      "Next": "PostIngestionChecks"
    },
    "PostIngestionChecks": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${region}:${account}:function:post-ingestion-checker",
      "Parameters": {
        "pipeline_name": "salesforce-opportunities-ingestion",
        "execution_id.$": "$$.Execution.Id",
        "checks": ["row_count", "quarantine_rate", "audit_columns", "schema_consistency", "freshness_sla", "deduplication"]
      },
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyOnFailure"
        }
      ],
      "Next": "NotifyOnComplete"
    },
    "NotifyOnComplete": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:${region}:${account}:pipeline-notifications",
        "Message.$": "States.Format('Pipeline {} completed successfully. Execution: {}', $.pipeline_name, $$.Execution.Id)"
      },
      "End": true
    },
    "NotifyOnFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:${region}:${account}:pipeline-alerts",
        "Message.$": "States.Format('Pipeline {} FAILED. Execution: {}. Error: {}', $.pipeline_name, $$.Execution.Id, $.error)"
      },
      "End": true
    }
  }
}
```

---

## Pre-Ingestion Validation Hook

Runs before the Glue job starts to fail fast on known-bad conditions:

| Check                | What It Does                                              | Failure Action          |
|----------------------|-----------------------------------------------------------|-------------------------|
| Connectivity         | Verify network path to source (TCP/HTTPS)                 | Abort, alert ops        |
| Credentials          | Validate secrets exist and are not expired                 | Abort, alert security   |
| Source Freshness     | Confirm source has new data since last watermark           | Skip run, log info      |
| Target Health        | Confirm Iceberg table is accessible and writable           | Abort, alert ops        |
| Contract Available   | Validate contract YAML exists and parses correctly         | Abort, alert data eng   |

---

## Post-Ingestion Checks Hook

Runs after the Glue job completes to validate output quality:

| Check                | What It Does                                              | Failure Action          |
|----------------------|-----------------------------------------------------------|-------------------------|
| Row Count            | Compare extracted vs loaded count, flag if delta > 5%     | Warn, log anomaly       |
| Quarantine Rate      | Alert if quarantined rows exceed threshold (default 5%)   | Alert, pause pipeline   |
| Audit Columns        | Verify all audit columns populated, no nulls              | Fail, rerun required    |
| Schema Consistency   | Compare output schema to contract, detect drift           | Warn, notify data eng   |
| Freshness SLA        | Verify data arrived within SLA window                     | Alert, escalate         |
| Deduplication        | Confirm no duplicate merge keys in target                 | Fail, investigate       |

---

## Promotion: Dev → Test → Prod

### Gate 1: Dev → Test

Requirements:
- All unit tests pass (contract tests, transform logic, mock sources)
- Generation report has zero errors, warnings reviewed
- Pipeline runs successfully against dev source (or mock)
- Quarantine rate < 1% on sample data
- Code review approved by data engineering team

### Gate 2: Test → Prod

Requirements:
- End-to-end run against test environment with production-like data volume
- Performance benchmarks met (execution time within 2x of baseline)
- Post-ingestion checks all pass
- Data quality report reviewed and signed off by data steward
- Rollback procedure tested and documented
- Observability dashboards configured and validated

### Rollback Procedure

Iceberg's time-travel capability enables instant rollback:

```sql
-- 1. Identify the snapshot before the bad ingestion
SELECT snapshot_id, committed_at, operation
FROM bronze_salesforce.opportunities.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- 2. Roll back to the last known-good snapshot
ALTER TABLE bronze_salesforce.opportunities
  EXECUTE rollback_to_snapshot({{snapshot_id}});

-- 3. Verify row counts match expected state
SELECT COUNT(*) FROM bronze_salesforce.opportunities;

-- 4. Update watermark store to re-process from last good point
UPDATE pipeline_metadata.watermarks
SET watermark_value = '{{last_good_watermark}}'
WHERE pipeline_name = 'salesforce-opportunities-ingestion';
```

---

## CDK Stack Structure

```
pipeline-project/
├── cdk/
│   ├── app.py
│   ├── cdk.json
│   └── stacks/
│       ├── __init__.py
│       ├── pipeline_stack.py          # Main stack: Glue job, IAM, S3
│       ├── orchestration_stack.py     # Step Functions, EventBridge rules
│       └── observability_stack.py     # CloudWatch dashboards, alarms, SNS
├── src/
│   ├── glue_jobs/
│   │   └── salesforce_opportunities_ingestion.py
│   ├── lambdas/
│   │   ├── pre_ingestion_validator/
│   │   │   └── handler.py
│   │   └── post_ingestion_checker/
│   │       └── handler.py
│   ├── orchestration/
│   │   └── sfn.json
│   └── custom_hooks/
│       └── salesforce_opportunities_ingestion_hooks.py
├── tests/
│   ├── unit/
│   │   ├── test_transforms.py
│   │   ├── test_contract_validation.py
│   │   └── test_dedup_logic.py
│   ├── integration/
│   │   └── test_end_to_end.py
│   └── fixtures/
│       ├── sample_input.json
│       └── expected_output.json
├── configs/
│   └── salesforce-opportunities-ingestion.yaml
├── contracts/
│   └── salesforce-opportunities-contract.yaml
└── reports/
    └── generation_report.md
```

---

## MUST Rules for Generated Pipelines

1. **Every pipeline MUST have a data contract.** No contract, no generation. The contract defines schema, quality rules, and SLA expectations before any code is written.

2. **Every pipeline MUST write to Iceberg format.** Iceberg provides ACID transactions, time-travel, and schema evolution. No exceptions for bronze-layer targets.

3. **Every pipeline MUST emit observability metrics.** At minimum: rows_extracted, rows_loaded, rows_quarantined, execution_duration_seconds, watermark_value. Published to CloudWatch.

4. **Every pipeline MUST quarantine invalid rows.** Bad data goes to a quarantine table with failure reasons — never silently dropped, never blocks good data.

5. **Every pipeline MUST be idempotent.** Re-running with the same watermark produces the same result. Merge keys + watermark ordering guarantee this.

6. **Every pipeline MUST use secrets manager for credentials.** No hardcoded credentials, no environment variables for secrets. All auth via AWS Secrets Manager ARN references.

7. **Every pipeline MUST include a rollback path.** Generated code includes Iceberg snapshot metadata. Rollback is a single SQL statement, documented in the generation report.
