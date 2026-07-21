---
name: cost-estimation-and-optimization
description: >-
  Estimate pipeline costs before deployment, track actual spend per source, detect
  cost anomalies, and recommend optimizations (right-sizing workers, adjusting
  schedules, compacting tables). Triggers on: how much will this cost, pipeline cost,
  cost estimate, optimize costs, reduce spend, cost anomaly, budget, DPU hours,
  right-size workers. Do NOT use for deploying changes (use deploying-cdk-pipeline),
  monitoring health (use monitoring-pipeline-health), or querying data (use
  querying-data-lake).
version: 1
argument-hint: '[source-name|domain|''all''|''estimate'']'
author: MANIOX
---

# Cost Estimation and Optimization

Estimate, track, and optimize the cost of ingestion pipelines. Provides pre-deployment
cost estimates, ongoing cost monitoring, anomaly detection, and actionable optimization
recommendations.

## Philosophy

**Know the cost before you commit, track it after you deploy.** Every pipeline should
have a cost estimate before it reaches production, and actual spend should be monitored
against that estimate. When costs drift, the platform surfaces actionable recommendations
rather than just alerts.

## Workflow

### 1. Pre-Deployment Cost Estimation

Before deploying a new pipeline, estimate monthly cost:

**Inputs needed:**
- Data volume per batch (rows and approximate size in GB)
- Schedule frequency (hourly, daily, weekly)
- Worker type and count (from sizing heuristic)
- Source type (affects read cost)

**Cost components:**

| Component | Formula | Unit Price (reference) |
|---|---|---|
| Glue ETL compute | DPU-hours = workers × DPU_multiplier × (duration_min / 60) | Per DPU-hour |
| S3 storage (bronze) | GB_per_batch × batches_per_month × compression_ratio | Per GB-month |
| S3 requests | PUT/GET operations per batch × batches_per_month | Per 1000 requests |
| S3 Tables (if used) | Managed Iceberg overhead (compaction, snapshots) | Per GB-month + requests |
| Secrets Manager | 1 secret × API calls per batch | Per secret + per 10K calls |
| Step Functions | State transitions per execution × executions_per_month | Per 1000 transitions |
| CloudWatch | Metrics + log ingestion + alarms | Per metric + per GB logs |
| EventBridge | Scheduled invocations per month | Per million invocations |

**DPU multipliers:**

| Worker Type | DPU per Worker |
|---|---|
| G.025X | 0.25 |
| G.1X | 1.0 |
| G.2X | 2.0 |
| G.4X | 4.0 |
| G.8X | 8.0 |

**Estimation example:**

```
Source: postgres-orders (JDBC, daily, ~2 GB per batch)
Workers: 5 × G.1X = 5 DPU
Estimated duration: 15 minutes
Monthly batches: 30

Glue compute: 5 DPU × 0.25 hours × 30 days = 37.5 DPU-hours/month
S3 storage: 2 GB × 30 days × 0.3 (ZSTD compression) = 18 GB/month (growing)
S3 requests: ~500 PUTs/batch × 30 = 15,000 requests/month
Step Functions: ~10 transitions × 30 = 300 transitions/month
CloudWatch: 5 custom metrics + 100 MB logs/month

Estimated monthly cost: $XX.XX (compute dominant)
```

### 2. Track Actual Cost Per Pipeline

After deployment, track actual spend:

**Query Glue job run metrics:**
```bash
aws glue get-job-runs --job-name ingest-{source_name}-prod \
  --max-results 30 \
  --query "JobRuns[].{RunId:Id,Duration:ExecutionTime,Workers:NumberOfWorkers,WorkerType:WorkerType,Status:JobRunState,StartedOn:StartedOn}"
```

**Calculate actual DPU-hours per run:**
```
actual_dpu_hours = execution_time_seconds / 3600 × workers × dpu_multiplier
```

**Calculate monthly spend:**
```
monthly_compute = sum(dpu_hours for last 30 days) × price_per_dpu_hour
monthly_storage = current_table_size_gb × price_per_gb_month
monthly_total = monthly_compute + monthly_storage + monthly_requests + monthly_orchestration
```

### 3. Compare Estimate vs Actual

| Metric | Estimated | Actual | Variance |
|---|---|---|---|
| DPU-hours/month | 37.5 | 42.0 | +12% (acceptable) |
| Duration/run | 15 min | 18 min | +20% (monitor) |
| Storage growth | 18 GB/month | 22 GB/month | +22% (data volume higher) |

**Variance thresholds:**

| Variance | Status | Action |
|---|---|---|
| < 20% | Normal | No action |
| 20-50% | Warning | Investigate root cause |
| 50-100% | High | Recommend optimization |
| > 100% | Critical | Alert — cost spike |

### 4. Detect Cost Anomalies

Compare each run against the rolling 7-day average:

```
anomaly_score = (latest_dpu_hours - avg_7day_dpu_hours) / avg_7day_dpu_hours × 100
```

| Anomaly Score | Interpretation | Action |
|---|---|---|
| < 50% | Normal variance | None |
| 50-100% | Moderate spike | Log and monitor |
| 100-300% | Significant spike | Alert — likely data volume increase |
| > 300% | Extreme spike | Alert — possible full-table scan or watermark reset |

### 5. Generate Optimization Recommendations

Based on actual metrics, recommend specific changes:

**Worker Right-Sizing:**

| Observation | Recommendation |
|---|---|
| Peak memory < 30% of worker capacity | Downgrade worker type (G.2X → G.1X) |
| Job consistently finishes in < 5 min | Reduce worker count |
| Job duration > 80% of timeout | Increase workers (not type) |
| Frequent OOM errors | Upgrade worker type |
| Workers mostly idle (low CPU) | Reduce worker count |

**Schedule Optimization:**

| Observation | Recommendation |
|---|---|
| Source has no new data in 50%+ of runs | Switch from hourly to daily |
| Skip-batch rate > 70% | Reduce frequency or switch to event-driven |
| Consistent data arrival at specific time | Align schedule to arrive + 30 min buffer |
| Weekend runs always empty | Add day-of-week filter to schedule |

**Storage Optimization:**

| Observation | Recommendation |
|---|---|
| Table has > 1000 snapshots | Run snapshot expiration |
| Small files (< 128 MB average) | Run compaction optimization |
| Partition with > 10K files | Repartition with coarser grain |
| Compression ratio > 0.5 | Verify ZSTD compression is enabled |
| Quarantine table growing unbounded | Set quarantine retention policy |

**Query Cost (Athena):**

| Observation | Recommendation |
|---|---|
| Full table scans on large tables | Add partition filters to queries |
| Repeated queries on same data | Consider materialized views |
| Small frequent queries | Batch into larger queries |

### 6. Generate Cost Report

```
Pipeline Cost Report — {month}
═══════════════════════════════

Total platform cost: $XXX.XX
  Compute (Glue): $XXX.XX (XX%)
  Storage (S3): $XX.XX (XX%)
  Orchestration: $X.XX (X%)
  Monitoring: $X.XX (X%)

Top 5 expensive pipelines:
┌────────────────────┬───────────┬───────────┬──────────┐
│ Pipeline           │ Monthly $ │ DPU-hours │ Trend    │
├────────────────────┼───────────┼───────────┼──────────┤
│ oracle-erp-full    │ $45.20    │ 120.0     │ ↑ +15%   │
│ salesforce-contacts│ $32.10    │ 85.5      │ → stable │
│ postgres-orders    │ $18.50    │ 42.0      │ ↓ -8%    │
│ s3-vendor-exports  │ $12.30    │ 25.0      │ → stable │
│ kafka-events       │ $89.90    │ 240.0     │ ↑ +45%   │
└────────────────────┴───────────┴───────────┴──────────┘

Optimization opportunities:
  1. kafka-events: Cost up 45%. Data volume grew — consider larger
     batch windows to reduce per-batch overhead. Estimated savings: $15/month
  2. oracle-erp-full: Using G.2X workers but memory usage < 40%.
     Downgrade to G.1X. Estimated savings: $22/month
  3. 3 pipelines have > 70% skip-batch rate.
     Switch to event-driven. Estimated savings: $8/month

Total potential monthly savings: $45/month (12% of current spend)
```

### 7. Apply Optimizations

For approved optimizations, update the pipeline config and redeploy:

**Worker right-sizing:**
Update `pipeline-config.yaml`:
```yaml
execution:
  worker_type: "G.1X"      # was G.2X
  number_of_workers: 3      # was 5
```

Then delegate to `deploying-cdk-pipeline` to apply the change.

**Schedule changes:**
Update `pipeline-config.yaml`:
```yaml
schedule:
  frequency: "daily"        # was hourly
  cron: "0 6 * * ? *"
```

**Storage maintenance:**
```sql
-- Expire old snapshots
ALTER TABLE {catalog}.{database}.{table}
SET TBLPROPERTIES ('history.expire.max-snapshot-age-ms' = '604800000');

-- Run compaction
OPTIMIZE {catalog}.{database}.{table};
```

## MUST Rules

- MUST provide cost estimate before any production deployment
- MUST track actual cost per pipeline per month
- MUST alert when cost variance exceeds 100% of estimate
- MUST present optimization recommendations with estimated savings
- MUST NOT auto-apply optimizations that could affect SLA (require user approval)
- MUST include storage costs in total (not just compute)
- MUST account for data growth when estimating future costs
- MUST tag all resources with cost_center for allocation

## Gotchas

- Glue job minimum billing is 1 minute — very short jobs still cost 1 min of DPU
- S3 Tables has different pricing than general-purpose S3 — check current rates
- Iceberg compaction runs are billed as Glue DPU-hours if using Glue maintenance
- Step Functions Express workflows have different pricing than Standard
- CloudWatch custom metrics cost accumulates with cardinality — avoid high-cardinality dimensions
- Cost estimates should use current pricing — rates change, always verify against latest
