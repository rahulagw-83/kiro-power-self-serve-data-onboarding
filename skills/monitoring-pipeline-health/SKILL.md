---
name: monitoring-pipeline-health
description: >-
  Check if landing pipelines are healthy. Two questions: did data arrive? Is it
  fresh? Monitors DMS tasks, AppFlow flows, Firehose streams, Transfer Family,
  and DataSync tasks. Triggers on: check pipeline health, are pipelines healthy,
  is data arriving, freshness check, did the sync run, pipeline status.
  Do NOT use for data quality checks or profiling (use data-profiling skill).
version: 3
argument-hint: '[source-name|''all'']'
author: "Rahul Agarwal, Manish Choudhary"
---

# Monitor Pipeline Health

Two questions: **Did data arrive?** and **Is it fresh?**

No quality checks, no schema validation, no contract enforcement.

## Checks

### 1. Did Data Arrive?

Check the ingestion service status:

| Service | Check Command |
|---|---|
| DMS | `aws dms describe-replication-tasks --query "ReplicationTasks[?Status]"` |
| AppFlow | `aws appflow describe-flow-execution-records --flow-name {name} --max-results 1` |
| Firehose | `aws firehose describe-delivery-stream --query "...DeliveryStreamStatus"` |
| Transfer Family | Check S3 for new files in SFTP landing prefix |
| DataSync | `aws datasync describe-task-execution --task-execution-arn {arn}` |

### 2. Is It Fresh?

Check most recent file in Landing S3:

```bash
aws s3api list-objects-v2 \
  --bucket {bucket} --prefix "{domain}/{source}/" \
  --query "sort_by(Contents, &LastModified)[-1].LastModified"
```

Compare against expected SLA. If `now - last_modified > sla_hours` → stale.

### 3. Health Report

```
Pipeline Health — {timestamp}

Source: aurora-orders (DMS CDC)
  Status: RUNNING ✅
  Last data: 2 hours ago (SLA: 24h) ✅

Source: salesforce-contacts (AppFlow daily)
  Status: SUCCEEDED ✅
  Last data: 18 hours ago (SLA: 24h) ✅

Source: vendor-sftp (Transfer Family)
  Status: No new files in 30 hours ⚠️
  Expected: daily (SLA: 24h) — STALE
```

### 4. Self-Healing

| Issue | Auto-Fix |
|---|---|
| DMS task stopped | Restart task |
| Firehose suspended | Fix permissions, resume |
| AppFlow OAuth expired | Flag for credential refresh |
| 5+ consecutive failures | Pause, alert on-call |

## MUST Rules

- MUST check service status (running/failed/stopped)
- MUST check freshness (latest file timestamp vs SLA)
- MUST NOT check data quality, schema, or contracts
- MUST alert on failures and SLA breaches
