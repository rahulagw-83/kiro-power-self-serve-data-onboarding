---
name: cost-estimation-and-optimization
description: >-
  Estimate landing pipeline costs using service-specific pricing (DMS, AppFlow,
  Firehose, Transfer Family, DataSync, DataBrew). Show cost comparison between
  applicable services before deployment. Track actual spend. Triggers on: how much
  will this cost, cost estimate, cost comparison, DMS vs Glue, optimize costs,
  reduce spend, budget, monthly cost.
version: 3
argument-hint: '[source-name|''compare''|''all'']'
author: "Rahul Agarwal, Manish Choudhary"
---

# Cost Estimation and Optimization

Show cost comparison before deployment. Track actual spend after.

## Cost Estimation Formulas

| Service | Monthly Cost Formula |
|---|---|
| **DMS** (CDC, t3.medium) | ~$75 instance + $0.115/GB storage + $0.02/GB transfer |
| **DMS** (full load only, on-demand) | Per-hour runtime only (no always-on instance needed) |
| **AppFlow** | $0.001/flow run + $0.025/GB processed |
| **Firehose** | $0.029/GB ingested + $0.018/GB format conversion |
| **Transfer Family** (always-on) | $0.30/hour (~$216/month) + $0.04/GB upload |
| **Transfer Family** (on-demand) | Start/stop per transfer window — much cheaper |
| **DataSync** | $0.0125/GB transferred |
| **DataBrew profiling** | $1.00/DataBrew node per 30-min session |
| **S3 storage** | $0.023/GB-month (Standard) |
| **Glue JDBC** (for comparison) | $0.44/DPU-hour |

## Cost Comparison Template

```
For your workload ({source_type}, {tables} tables, ~{volume}/day, {sync_mode}):

  Option A: {recommended_service}
    Cost: ~${monthly}/month
    Why: {one-sentence rationale}

  Option B: {alternative}
    Cost: ~${monthly}/month
    Trade-off: {limitation}

  Which fits your requirements?
```

## Optimization Recommendations

| Observation | Recommendation |
|---|---|
| DMS instance idle 80%+ of the time | Switch to smaller instance or serverless DMS |
| Transfer Family always-on but transfers only daily | Switch to on-demand (start/stop) |
| Firehose buffering too small | Increase buffer to 128 MB / 300s to reduce PUT costs |
| AppFlow running hourly but data changes daily | Reduce to daily schedule |
| DataBrew profiling on every batch | Profile weekly or on first load only |
| S3 Landing retention > needed | Reduce lifecycle to match compliance minimum |

## MUST Rules

- MUST show cost comparison before user commits to a service
- MUST use current AWS pricing (verify periodically)
- MUST include S3 storage costs in total
- MUST recommend on-demand/serverless where workload is intermittent
