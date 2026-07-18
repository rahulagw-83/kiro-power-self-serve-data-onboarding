# Observability

Monitoring, alerting, structured logging, CloudWatch metrics, dashboards, and troubleshooting runbooks for self-serve ingestion pipelines.

---

## Observability Architecture

```
Glue ETL Job
  ├── Structured JSON Logs (CloudWatch Log Group)
  ├── Custom Metrics (CloudWatch Namespace)
  ├── Job Metrics (Glue Namespace)
  └── Pipeline Status (Step Functions)
          │
          ▼
    CloudWatch Alarms
          │
          ▼
       SNS Topic
       ┌──┼──┐
       ▼  ▼  ▼
   Email Slack PagerDuty
```

---

## Structured Logging

### Logger Implementation

```python
import json
import time
from datetime import datetime, timezone


class StructuredLogger:
    """
    Emits JSON-formatted log lines to stdout (captured by CloudWatch).
    Never logs PII values — only metadata, counts, and identifiers.
    """

    def __init__(self, source_id: str, batch_id: str, pipeline_version: str = "1.0"):
        self.source_id = source_id
        self.batch_id = batch_id
        self.pipeline_version = pipeline_version

    def _log(self, level: str, event: str, **kwargs):
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event,
            "source_id": self.source_id,
            "batch_id": self.batch_id,
            "pipeline_version": self.pipeline_version,
            **kwargs,
        }
        print(json.dumps(record))

    def info(self, event: str, **kwargs):
        self._log("INFO", event, **kwargs)

    def warn(self, event: str, **kwargs):
        self._log("WARN", event, **kwargs)

    def error(self, event: str, **kwargs):
        self._log("ERROR", event, **kwargs)
```

> **Important**: The logger must NEVER emit PII values (email addresses, names, phone numbers).
> Log only identifiers (order_id, customer_id), counts, and metadata.

### Log Events Emitted

| Event | Level | Fields | Description |
|-------|-------|--------|-------------|
| `pipeline_started` | INFO | `source_type`, `target_table` | Job execution begins |
| `source_read_complete` | INFO | `rows_read`, `duration_ms` | Source data successfully read |
| `no_new_data` | INFO | `watermark_value` | No new records found since last run |
| `contract_validation_complete` | INFO | `rows_valid`, `rows_quarantined` | Schema/type validation done |
| `rows_quarantined` | WARN | `quarantine_count`, `quarantine_rate`, `sample_reasons` | Rows failed validation |
| `duplicates_removed` | INFO | `duplicate_count` | Deduplication applied |
| `volume_below_min` | WARN | `rows_read`, `min_threshold` | Fewer rows than expected |
| `volume_above_max` | WARN | `rows_read`, `max_threshold` | More rows than expected |
| `freshness_sla_breach` | WARN | `hours_since_last`, `sla_hours` | Data freshness exceeded threshold |
| `write_complete` | INFO | `rows_written`, `target_path`, `duration_ms` | Data written to target |
| `watermark_updated` | INFO | `new_watermark`, `previous_watermark` | Incremental watermark advanced |
| `pipeline_completed` | INFO | `total_duration_ms`, `status` | Job finished successfully |
| `critical_quarantine_rate` | ERROR | `quarantine_rate`, `threshold` | Quarantine rate exceeded critical threshold |
| `pipeline_failed` | ERROR | `error_type`, `error_message` | Unrecoverable failure |

### Log Group Convention

```
/aws/glue/ingest-{source_name}-{table}
```

Examples:
- `/aws/glue/ingest-ecommerce-orders`
- `/aws/glue/ingest-payments-transactions`

### CloudWatch Insights Queries

**Find all failed runs in the last 7 days:**

```
fields @timestamp, source_id, batch_id, error_type, error_message
| filter event = "pipeline_failed"
| sort @timestamp desc
| limit 50
```

**Quarantine rate trend over 30 days:**

```
fields @timestamp, source_id, quarantine_rate
| filter event = "contract_validation_complete"
| stats avg(quarantine_rate) as avg_rate by bin(1d), source_id
| sort @timestamp asc
```

**Duration trend (detect performance degradation):**

```
fields @timestamp, source_id, total_duration_ms
| filter event = "pipeline_completed"
| stats avg(total_duration_ms) as avg_duration, max(total_duration_ms) as max_duration by bin(1d), source_id
| sort @timestamp asc
```

**Find a specific batch:**

```
fields @timestamp, level, event, @message
| filter batch_id = "batch-2024-01-15T08:00:00Z"
| sort @timestamp asc
```

---

## CloudWatch Custom Metrics

### Metrics Emitter Implementation

```python
import boto3
from datetime import datetime, timezone


class MetricsEmitter:
    """
    Publishes custom metrics to CloudWatch for pipeline observability.
    Dimensions: SourceName, PipelineVersion.
    """

    def __init__(self, namespace: str, source_name: str, pipeline_version: str = "1.0"):
        self.client = boto3.client("cloudwatch")
        self.namespace = namespace
        self.source_name = source_name
        self.pipeline_version = pipeline_version

    def emit(self, metric_name: str, value: float, unit: str = "Count"):
        self.client.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Timestamp": datetime.now(timezone.utc),
                    "Dimensions": [
                        {"Name": "SourceName", "Value": self.source_name},
                        {"Name": "PipelineVersion", "Value": self.pipeline_version},
                    ],
                }
            ],
        )
```

### Namespace Convention

```
IngestionPipeline/{SourceDisplayName}
```

Examples:
- `IngestionPipeline/EcommerceOrders`
- `IngestionPipeline/PaymentsTransactions`

### Metrics Emitted

| Metric Name | Unit | Description |
|-------------|------|-------------|
| `pipeline_start` | Count | Pipeline execution initiated (always 1) |
| `rows_read` | Count | Total rows read from source |
| `rows_valid` | Count | Rows passing contract validation |
| `rows_quarantined` | Count | Rows failing validation |
| `quarantine_rate` | Percent | Percentage of rows quarantined |
| `rows_written` | Count | Rows successfully written to target |
| `duration_seconds` | Seconds | Total pipeline execution time |
| `freshness_hours` | None | Hours since last successful ingestion |
| `status` | Count | 1 = success, 0 = failure |

---

## Alarms

### Standard Alarm Set (All Pipelines)

| Alarm | Metric | Condition | Period | Actions |
|-------|--------|-----------|--------|---------|
| Pipeline Failure | `status` | `status == 0` for 1 datapoint | Per execution | SNS → Critical topic |
| High Quarantine Rate | `quarantine_rate` | `quarantine_rate > 10%` for 1 datapoint | Per execution | SNS → Warning topic |

### Additional Alarms (JDBC Sources)

| Alarm | Metric | Condition | Period | Actions |
|-------|--------|-----------|--------|---------|
| Freshness SLA Breach | `freshness_hours` | `freshness_hours > sla_threshold` for 1 datapoint | Hourly check | SNS → Warning topic |

### Alarm Configuration YAML

```yaml
alarms:
  pipeline_failure:
    metric_name: status
    namespace: "IngestionPipeline/${source_display_name}"
    comparison: LessThanThreshold
    threshold: 1
    evaluation_periods: 1
    period: 300
    statistic: Minimum
    actions:
      - arn:aws:sns:${region}:${account_id}:ingestion-critical

  high_quarantine_rate:
    metric_name: quarantine_rate
    namespace: "IngestionPipeline/${source_display_name}"
    comparison: GreaterThanThreshold
    threshold: 10
    evaluation_periods: 1
    period: 300
    statistic: Maximum
    actions:
      - arn:aws:sns:${region}:${account_id}:ingestion-warnings

  freshness_sla_breach:
    metric_name: freshness_hours
    namespace: "IngestionPipeline/${source_display_name}"
    comparison: GreaterThanThreshold
    threshold: ${freshness_sla_hours}
    evaluation_periods: 1
    period: 3600
    statistic: Maximum
    actions:
      - arn:aws:sns:${region}:${account_id}:ingestion-warnings
```

### SNS Topic Structure

| Topic | Purpose | Subscription |
|-------|---------|--------------|
| `ingestion-critical` | Pipeline failures, data loss risk | PagerDuty integration, on-call email |
| `ingestion-warnings` | Quality issues, SLA breaches | Slack channel, team email |
| `ingestion-info` | Daily summaries, non-urgent updates | Email digest |

> **Guidance**: Use PagerDuty for critical alarms that require immediate action.
> Use Slack/email for warnings that can be addressed during business hours.

---

## CloudWatch Dashboard

### Recommended Widgets

```
┌────────────────────────┬──────────────────────────────┐
│ Pipeline Status (24h)  │ Rows Processed (7d)          │
│ [SingleValue: status]  │ [TimeSeries: read/written]   │
├────────────────────────┼──────────────────────────────┤
│ Duration (7d)          │ Quarantine Rate (7d)         │
│ [TimeSeries: avg/p95]  │ [TimeSeries + threshold]     │
├────────────────────────┼──────────────────────────────┤
│ Freshness SLA (24h)    │ Active Alarms                │
│ [Gauge: hours]         │ [Alarm status widget]        │
└────────────────────────┴──────────────────────────────┘
```

### Dashboard JSON (Template)

```json
{
  "widgets": [
    {
      "type": "metric", "x": 0, "y": 0, "width": 6, "height": 6,
      "properties": {
        "metrics": [["IngestionPipeline/${SourceName}", "status", "SourceName", "${SourceName}"]],
        "view": "singleValue", "period": 86400, "stat": "Sum",
        "title": "Pipeline Status (24h)"
      }
    },
    {
      "type": "metric", "x": 6, "y": 0, "width": 18, "height": 6,
      "properties": {
        "metrics": [
          ["IngestionPipeline/${SourceName}", "rows_read", "SourceName", "${SourceName}"],
          ["IngestionPipeline/${SourceName}", "rows_written", "SourceName", "${SourceName}"]
        ],
        "view": "timeSeries", "period": 86400, "stat": "Sum",
        "title": "Rows Processed (7d)"
      }
    },
    {
      "type": "metric", "x": 0, "y": 6, "width": 12, "height": 6,
      "properties": {
        "metrics": [["IngestionPipeline/${SourceName}", "duration_seconds", "SourceName", "${SourceName}"]],
        "view": "timeSeries", "period": 86400, "stat": "Average",
        "title": "Duration Trend (7d)"
      }
    },
    {
      "type": "metric", "x": 12, "y": 6, "width": 12, "height": 6,
      "properties": {
        "metrics": [["IngestionPipeline/${SourceName}", "quarantine_rate", "SourceName", "${SourceName}"]],
        "view": "timeSeries", "period": 86400, "stat": "Maximum",
        "title": "Quarantine Rate (7d)",
        "annotations": { "horizontal": [{ "label": "Threshold", "value": 10 }] }
      }
    },
    {
      "type": "metric", "x": 0, "y": 12, "width": 12, "height": 6,
      "properties": {
        "metrics": [["IngestionPipeline/${SourceName}", "freshness_hours", "SourceName", "${SourceName}"]],
        "view": "timeSeries", "period": 3600, "stat": "Maximum",
        "title": "Freshness SLA"
      }
    },
    {
      "type": "alarm", "x": 12, "y": 12, "width": 12, "height": 6,
      "properties": {
        "alarms": ["arn:aws:cloudwatch:${region}:${account_id}:alarm:ingestion-*"],
        "title": "Active Alarms"
      }
    }
  ]
}
```

---

## Step Functions Monitoring

```bash
# List recent failed executions
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:ingest-${SOURCE}-${TABLE} \
  --status-filter FAILED --max-results 10

# Describe a specific execution
aws stepfunctions describe-execution --execution-arn ${EXECUTION_ARN}

# Get execution history (debugging)
aws stepfunctions get-execution-history --execution-arn ${EXECUTION_ARN} --reverse-order
```

**Built-in Metrics**: `ExecutionsStarted`, `ExecutionsSucceeded`, `ExecutionsFailed`, `ExecutionsTimedOut`, `ExecutionTime` (ms).

---

## Glue Job Monitoring

### Built-in Glue Metrics

| Metric | Description |
|--------|-------------|
| `glue.ALL.aggregate.numTasks` | Total tasks executed |
| `glue.ALL.aggregate.elapsedTime` | Wall-clock time |
| `glue.ALL.aggregate.bytesRead` | Bytes read from source |
| `glue.ALL.aggregate.bytesWritten` | Bytes written to target |
| `glue.ALL.jvm.heap.usage` | JVM heap utilization |
| `glue.ALL.s3.filesystem.read_bytes` | S3 read throughput |
| `glue.ALL.s3.filesystem.write_bytes` | S3 write throughput |

### Job Bookmark Commands

```bash
aws glue get-job-bookmark --job-name ingest-${SOURCE}-${TABLE}
aws glue reset-job-bookmark --job-name ingest-${SOURCE}-${TABLE}  # reprocess all
```

---

## Troubleshooting Runbooks

### Runbook 1: Pipeline Failure (Status Alarm)

**Trigger**: `ingestion-critical` alarm fires with `status == 0`.

**Diagnosis Steps:**

1. **Check CloudWatch Logs** for the `pipeline_failed` event:
   ```
   fields @timestamp, error_type, error_message, batch_id
   | filter event = "pipeline_failed"
   | sort @timestamp desc
   | limit 5
   ```

2. **Check Glue Job run details:**
   ```bash
   aws glue get-job-runs --job-name ingest-${SOURCE}-${TABLE} --max-results 1
   ```

3. **Review error category:**
   - `ConnectionError` → See Runbook 5
   - `SchemaValidationError` → Source schema changed, update contract
   - `PermissionError` → IAM role missing permissions
   - `OutOfMemoryError` → Increase worker type or count

4. **Check Step Functions history:**
   ```bash
   aws stepfunctions get-execution-history --execution-arn ${ARN} --reverse-order
   ```

5. **Re-run:**
   ```bash
   aws stepfunctions start-execution --state-machine-arn ${STATE_MACHINE_ARN} \
     --input '{"retry": true}'
   ```

### Runbook 2: High Quarantine Rate

**Trigger**: `quarantine_rate > 10%` alarm fires.

**Diagnosis Steps:**

1. **Query quarantined records in Athena:**
   ```sql
   SELECT quarantine_reason, COUNT(*) as cnt
   FROM ingestion_db.quarantine_{table}
   WHERE batch_id = '{latest_batch_id}'
   GROUP BY quarantine_reason
   ORDER BY cnt DESC;
   ```

2. **Identify root cause patterns:**
   - `type_mismatch` → Source data types changed
   - `null_required_field` → Source stopped populating a field
   - `regex_failed` → Format change (dates, IDs)
   - `range_exceeded` → Values outside expected bounds

3. **Sample quarantined rows:**
   ```sql
   SELECT * FROM ingestion_db.quarantine_{table}
   WHERE batch_id = '{latest_batch_id}' LIMIT 20;
   ```

4. **Decide action**: source defect → notify owner; contract too strict → update and reprocess; transient → monitor next run.

5. **Recovery:**
   ```bash
   aws stepfunctions start-execution --state-machine-arn ${STATE_MACHINE_ARN} \
     --input '{"reprocess_quarantine": true, "batch_id": "${BATCH_ID}"}'
   ```

### Runbook 3: Freshness SLA Breach

**Trigger**: `freshness_hours > threshold` alarm fires.

**Diagnosis Steps:**

1. **Check EventBridge rule:**
   ```bash
   aws events describe-rule --name ingest-${SOURCE}-${TABLE}-schedule
   ```

2. **Check last successful execution:**
   ```bash
   aws stepfunctions list-executions \
     --state-machine-arn ${STATE_MACHINE_ARN} --status-filter SUCCEEDED --max-results 1
   ```

3. **Common causes**: Schedule disabled, previous run still in progress, source unavailable during window, job found no new data.

4. **Verify source availability:**
   ```bash
   # S3 sources
   aws s3 ls s3://${BUCKET}/${PREFIX}/ --recursive | tail -5
   # JDBC sources
   aws glue get-connection --name ${CONNECTION_NAME}
   ```

5. **Manual trigger:**
   ```bash
   aws stepfunctions start-execution --state-machine-arn ${STATE_MACHINE_ARN} --input '{}'
   ```

### Runbook 4: No New Data Processed

**Trigger**: `pipeline_completed` with `rows_read == 0` for multiple consecutive runs.

**Diagnosis Steps:**

1. **Check for new files (S3):**
   ```bash
   aws s3 ls s3://${BUCKET}/${PREFIX}/ --recursive | sort -k1,2 | tail -10
   ```

2. **Check watermark (JDBC):**
   ```bash
   aws dynamodb get-item --table-name ingestion-watermarks \
     --key '{"source_id": {"S": "${SOURCE_ID}"}}'
   ```

3. **Verify Job Bookmark (S3):**
   ```bash
   aws glue get-job-bookmark --job-name ingest-${SOURCE}-${TABLE}
   ```

4. **Confirm source is producing data** — contact source owner or run count query with watermark filter.

5. **Reset if stuck:**
   ```bash
   # Reset bookmark (S3)
   aws glue reset-job-bookmark --job-name ingest-${SOURCE}-${TABLE}
   # Reset watermark (JDBC)
   aws dynamodb put-item --table-name ingestion-watermarks \
     --item '{"source_id": {"S": "${SOURCE_ID}"}, "watermark": {"S": "${EARLIER_VALUE}"}}'
   ```

### Runbook 5: Connection Issues (JDBC)

**Trigger**: `pipeline_failed` with `error_type == ConnectionError`.

| Error Message | Likely Cause | Fix |
|---------------|--------------|-----|
| `Connection refused` | Security group blocks port | Add inbound rule for Glue ENI CIDR |
| `Connection timed out` | No route to host | Check VPC routing, NAT Gateway, subnet config |
| `Authentication failed` | Credentials expired | Rotate secret in Secrets Manager |
| `Unknown host` | DNS resolution failure | Check VPC DNS settings, endpoint configuration |
| `SSL handshake failed` | Certificate mismatch | Update CA bundle or disable SSL verification |
| `Too many connections` | Connection pool exhausted | Reduce Glue DPU count or increase DB max connections |
| `Schema not found` | Database reorganized | Update connection JDBC URL path |

**Resolution commands:**

```bash
# 1. Verify secret
aws secretsmanager get-secret-value --secret-id ingestion/${SOURCE}/credentials \
  --query 'SecretString' --output text | python3 -m json.tool

# 2. Test connection
aws glue test-connection --connection-name ${CONNECTION_NAME}

# 3. Check security group
aws ec2 describe-security-groups --group-ids ${SG_ID} \
  --query 'SecurityGroups[].IpPermissions'

# 4. Check VPC endpoint
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.${REGION}.glue"
```

---

## Self-Healing Recommendations

| Pattern Detected | Automated Recommendation |
|------------------|--------------------------|
| Quarantine rate spike then immediate recovery | Transient source issue — no action needed, monitor |
| Quarantine rate steadily increasing over 7 days | Schema drift likely — flag for contract review |
| Duration increasing linearly with data volume | Partition strategy needed — recommend partitioning |
| Duration spikes on specific days | Correlate with source batch timing — adjust schedule |
| Repeated `OutOfMemoryError` | Auto-scale: increase worker type from G.1X → G.2X |
| Consecutive `no_new_data` for 3+ runs | Alert source owner, check upstream pipeline |
| Connection failures during maintenance windows | Implement maintenance window awareness in scheduler |
| Freshness breach every Monday | Weekend data accumulation — add weekend catch-up run |

---

## Cost Monitoring

| Service | Cost Driver | Optimization Tip |
|---------|-------------|------------------|
| AWS Glue | DPU-hours (billed per second, 10-min minimum) | Right-size workers; use G.025X for small jobs |
| Amazon S3 | Storage volume + request count | Lifecycle policies; Intelligent Tiering for infrequent access |
| DynamoDB | Read/Write capacity (on-demand) | Watermark table is minimal; on-demand is cost-effective |
| CloudWatch | Metrics (custom), Logs (ingestion + storage), Alarms | Set log retention; avoid high-cardinality dimensions |
| AWS KMS | API calls (encrypt/decrypt) | Use SSE-S3 if KMS isn't required by policy |
| Step Functions | State transitions | Keep state machines simple; minimize retry states |

> **Tip**: Tag all resources with `project`, `source_name`, and `environment` for
> Cost Explorer filtering and chargeback reporting.

---

## Observability Checklist

### Per-Pipeline Setup

- [ ] Structured logger initialized with `source_id` and `batch_id`
- [ ] All log events from the standard set are emitted
- [ ] Custom metrics emitter publishing to correct namespace
- [ ] CloudWatch Log Group created with correct naming convention
- [ ] Log retention policy set per environment
- [ ] Pipeline Failure alarm configured
- [ ] High Quarantine Rate alarm configured
- [ ] Freshness SLA alarm configured (JDBC sources)
- [ ] SNS topic subscriptions verified
- [ ] Dashboard widget added for new source
- [ ] Runbook links added to alarm descriptions

### Platform-Level Setup

- [ ] SNS topics created (critical, warnings, info)
- [ ] PagerDuty integration configured for critical topic
- [ ] Slack integration configured for warnings topic
- [ ] CloudWatch dashboard deployed
- [ ] Cost allocation tags applied to all resources
- [ ] Log Insights saved queries available to team

---

## Log Retention

| Environment | Retention Period | Rationale |
|-------------|-----------------|-----------|
| Dev | 14 days | Short-lived experiments, minimize cost |
| Test | 30 days | Enough for test cycle analysis |
| Prod | 90 days | Covers quarterly review and audit needs |

> Adjust retention based on compliance requirements. Some regulated industries
> require 1+ year retention — use S3 export for long-term archival.
