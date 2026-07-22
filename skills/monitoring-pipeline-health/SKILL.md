---
name: monitoring-pipeline-health
description: >-
  Proactively monitor pipeline health across all active sources. Checks both Landing
  layer services (DMS tasks, AppFlow flows, Firehose streams) and Raw layer pipelines
  (Glue jobs, DQDL quality results). Detects freshness breaches, DQDL failures,
  error taxonomy matches, cost anomalies. Triggers auto-remediation from error taxonomy.
  Triggers on: check pipeline health, are pipelines healthy, SLA status, pipeline
  dashboard, which pipelines are failing, freshness check, health report.
  Do NOT use for deploying (use deploying-cdk-pipeline), or querying (use querying-data-lake).
version: 2
argument-hint: '[source-name|domain|''all'']'
author: "Rahul Agarwal, Manish Choudhary"
---

# Monitor Pipeline Health

Proactively assess the health of all active ingestion pipelines. Monitors both the
Landing layer (DMS replication status, AppFlow flow runs, Firehose delivery health)
and the Raw layer (Glue job success/failure, DQDL quality scores, quarantine rates).
Uses the error taxonomy from `steering/troubleshooting.md` to auto-remediate known failures.

## Philosophy

**Proactive over reactive.** Don't wait for alarms — actively check pipeline health on a
regular cadence and surface issues before they breach SLAs. The best incident is the one
that never happens.

## Onboarding Workflow Position

This skill supports **Step 16 (Activate Observability)** of the onboarding workflow and
provides ongoing operational support for all deployed pipelines.

## Workflow

### 1. Identify Target Pipelines

Determine scope of health check:

| User says... | Scope |
|---|---|
| "check all pipelines" | All active sources in Source Registry |
| "check {domain} pipelines" | Filter by domain (sales, finance, etc.) |
| "check {source_name}" | Single pipeline |
| No args | All active sources |

Query the Source Registry:

```bash
aws dynamodb scan --table-name SourceRegistry \
  --filter-expression "#s = :active" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values '{":active": {"S": "active"}}' \
  --projection-expression "source_name, source_type, domain, pipeline_arn, target_table"
```

### 2. Check Freshness SLAs

For each active pipeline, compare last successful run against contract SLA:

```bash
aws stepfunctions list-executions \
  --state-machine-arn {pipeline_arn} \
  --status-filter SUCCEEDED \
  --max-results 1
```

Calculate hours since last success:
- `hours_stale = (NOW - last_success_timestamp) / 3600`

Classify:

| Condition | Status | Action |
|---|---|---|
| hours_stale < sla_hours * 0.8 | Healthy | None |
| hours_stale >= sla_hours * 0.8 | Warning | Flag for attention |
| hours_stale >= sla_hours | Breaching | Alert — SLA violated |
| hours_stale >= sla_hours * 2 | Critical | Escalate to on-call |

### 3. Check Execution Trends

Look at recent execution history for patterns:

```bash
aws stepfunctions list-executions \
  --state-machine-arn {pipeline_arn} \
  --max-results 10
```

Assess:

| Pattern | Status | Action |
|---|---|---|
| Last 10 all SUCCEEDED | Healthy | None |
| 1 failure in last 10 | Warning | Monitor (may be transient) |
| 2+ consecutive failures | Failing | Investigate — pattern emerging |
| 5+ consecutive failures | Critical | Circuit breaker — pause and escalate |
| All 10 FAILED | Dead | Pipeline broken — immediate escalation |

### 4. Check Quarantine Rates

Query recent quarantine records via Athena (delegate to `querying-data-lake`):

```sql
SELECT
  _batch_id,
  COUNT(*) as quarantined_rows,
  _error_reason,
  MAX(_quarantined_at) as latest
FROM {database}.{table}_quarantine
WHERE _quarantined_at > CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY _batch_id, _error_reason
ORDER BY latest DESC
LIMIT 20;
```

Classify:

| Quarantine Rate (last batch) | Status | Action |
|---|---|---|
| 0% | Healthy | None |
| 0.1% - 2% | Normal | Expected noise |
| 2% - 5% | Warning | Investigate trend |
| > 5% | Critical | Quality degradation — alert |

### 5. Check Alarm States

Query CloudWatch alarm states for all pipeline alarms:

```bash
aws cloudwatch describe-alarms \
  --alarm-name-prefix "pipeline-" \
  --state-value ALARM
```

Summarize alarms by type:
- Job failure alarms
- Freshness SLA alarms
- Quarantine rate alarms
- Duration spike alarms

### 6. Check Cost Trends

Query Glue job metrics for cost assessment:

```bash
aws glue get-job-runs --job-name ingest-{source_name}-prod --max-results 7
```

For each run, calculate DPU-hours:
- `dpu_hours = (execution_time_seconds / 3600) * number_of_workers * worker_dpu_multiplier`

Detect cost anomalies:
- Calculate 7-day average DPU-hours
- If latest run > 2x average → cost spike (likely data volume increase)
- If latest run < 0.1x average → suspiciously cheap (likely processing no data)

### 7. Generate Health Report

Produce a summary report:

```
Pipeline Health Report — {timestamp}
═══════════════════════════════════════

Overall: {X} healthy, {Y} warning, {Z} critical

┌────────────────────┬──────────┬───────────┬────────────┬──────────┐
│ Pipeline           │ Status   │ Freshness │ Quarantine │ Last Run │
├────────────────────┼──────────┼───────────┼────────────┼──────────┤
│ oracle-hr-employees│ Healthy  │ 2h / 24h  │ 0.1%       │ 2h ago   │
│ s3-vendor-exports  │ Warning  │ 20h / 24h │ 0.0%       │ 20h ago  │
│ postgres-orders    │ Critical │ 30h / 24h │ 8.2%       │ 30h ago  │
└────────────────────┴──────────┴───────────┴────────────┴──────────┘

Critical Issues:
  1. postgres-orders: SLA breached (30h > 24h SLA) + quarantine rate 8.2%
     → Recommended: Check source connectivity, review quarantine errors

Warnings:
  1. s3-vendor-exports: Approaching SLA (20h of 24h budget used)
     → Recommended: Monitor — if no run in next 4h, will breach
```

### 8. Trigger Self-Healing (if applicable)

For known failure patterns, apply automated fixes:

| Pattern | Auto-Fix | Requires Approval |
|---|---|---|
| Single transient failure after N successes | Re-run pipeline with backoff | No |
| OOM error (container killed) | Scale up workers by 50% and re-run | No |
| Credential expired error | Trigger secret refresh, re-run | No |
| Source unreachable (1-2 occurrences) | Wait 5 min, retry | No |
| 5+ consecutive failures | Pause pipeline, alert on-call | No (but alerts) |
| Quarantine rate > threshold | Flag for investigation | Yes |
| Watermark in future | Reset watermark to source MAX - 1h | Yes |
| Duration > 3x baseline | Scale up workers | No |

**Self-healing execution:**
```bash
# Example: re-run after transient failure
aws stepfunctions start-execution \
  --state-machine-arn {pipeline_arn} \
  --input '{"retry_after_failure": true}'
```

## Scheduling

This skill can be run:
- **On demand**: User asks "are my pipelines healthy?"
- **Scheduled**: Via EventBridge rule triggering a Lambda that runs the health check
- **Triggered**: By a CloudWatch alarm firing (reactive mode)

## MUST Rules

- MUST check freshness SLA for every active pipeline
- MUST report both healthy and unhealthy pipelines (not just failures)
- MUST NOT auto-fix issues that could cause data loss (watermark resets need approval)
- MUST escalate after 5 consecutive failures (circuit breaker)
- MUST include cost context in the health report
- MUST log all self-healing actions for audit trail
- MUST NOT suppress alarms — they exist for a reason

## Gotchas

- A pipeline with status=active but no executions in the last 30 days is likely misconfigured
- Step Functions execution history is limited to 90 days — for longer trends, use CloudWatch metrics
- DPU cost calculation differs by worker type (G.025X=0.25 DPU, G.1X=1 DPU, G.2X=2 DPU)
- Freshness checks must account for expected quiet periods (weekends, holidays)
- Self-healing retries should have a maximum count to prevent infinite loops
