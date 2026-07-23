---
name: provisioning-landing-pipeline
description: >-
  Create and start the AWS ingestion service that moves data from source to S3
  Landing. Handles DMS replication tasks, AppFlow flows, Firehose delivery streams,
  Transfer Family servers, and DataSync tasks. Starts initial load, verifies first
  data arrives in S3. Triggers on: start ingestion, start DMS, start replication,
  run AppFlow flow, start pipeline, begin sync, initial load, start CDC.
  Do NOT use for Terraform deployment (use deploying-terraform) or profiling
  (use data-profiling).
version: 1
argument-hint: '[source-name|service-type]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Provision Landing Pipeline

Create and start the ingestion service that moves data from source to S3 Landing.

## Philosophy

**The simplest service that gets data to S3.** No transforms, no Glue jobs reading
from source directly. Just the purpose-built AWS service doing what it was designed for.

## When This Skill Runs

After Terraform has been applied (infrastructure exists), this skill:
1. Starts the ingestion service (initial load or ongoing CDC)
2. Waits for first data to arrive in S3
3. Verifies the landing path has files
4. Confirms the pipeline is operational

## Workflow by Service

### AWS DMS (Databases)

#### Start Replication Task

```bash
# Check task status
aws dms describe-replication-tasks \
  --filters Name=replication-task-id,Values={task_id} \
  --query "ReplicationTasks[0].Status"

# Start task (full-load-and-cdc or cdc-only)
aws dms start-replication-task \
  --replication-task-arn {task_arn} \
  --start-replication-task-type start-replication
```

#### Monitor Progress

```bash
# Check full load progress
aws dms describe-table-statistics \
  --replication-task-arn {task_arn} \
  --query "TableStatistics[].{Table:TableName,Loaded:FullLoadRows,State:TableState}"
```

#### Verify Landing

```bash
# Check S3 for landed files
aws s3 ls s3://{bucket}/{domain}/{source}/ --recursive | tail -5
```

**Expected:** Parquet files partitioned by date appear within 5-15 minutes of start.

---

### Amazon AppFlow (SaaS)

#### Start Flow Execution

```bash
# Trigger a manual run (or wait for schedule)
aws appflow start-flow --flow-name {flow_name}

# Check execution status
aws appflow describe-flow-execution-records \
  --flow-name {flow_name} \
  --max-results 1 \
  --query "FlowExecutions[0].{Status:ExecutionStatus,Records:ExecutionResult.RecordsProcessed}"
```

#### Verify Landing

```bash
aws s3 ls s3://{bucket}/{domain}/{source}/ --recursive | tail -5
```

**Expected:** Parquet files appear within 2-5 minutes of flow completion.

---

### Kinesis Data Firehose (Streaming)

#### Verify Stream is Active

```bash
aws firehose describe-delivery-stream \
  --delivery-stream-name {stream_name} \
  --query "DeliveryStreamDescription.DeliveryStreamStatus"
```

Status should be `ACTIVE`. Firehose starts delivering automatically once producers send data.

#### Send Test Record (optional)

```bash
aws firehose put-record \
  --delivery-stream-name {stream_name} \
  --record '{"Data":"eyJ0ZXN0IjogdHJ1ZX0="}'
```

#### Verify Landing

Wait for buffer interval (default 300 seconds), then:
```bash
aws s3 ls s3://{bucket}/{domain}/{stream_name}/ --recursive | tail -5
```

**Expected:** Parquet files appear after buffer interval elapses.

---

### AWS Transfer Family (SFTP)

#### Verify Server is Online

```bash
aws transfer describe-server --server-id {server_id} \
  --query "Server.State"
```

Status should be `ONLINE`.

#### Test SFTP Connection

```bash
sftp -i {private_key} {username}@{server_endpoint}
# Or: provide endpoint to partner for their scheduled upload
```

#### Verify Landing

After partner uploads (or test upload):
```bash
aws s3 ls s3://{bucket}/{domain}/{source}/ --recursive | tail -5
```

---

### AWS DataSync (On-Premises)

#### Start Task Execution

```bash
aws datasync start-task-execution --task-arn {task_arn}

# Monitor progress
aws datasync describe-task-execution \
  --task-execution-arn {execution_arn} \
  --query "{Status:Status,BytesTransferred:BytesTransferred,FilesTransferred:FilesTransferred}"
```

#### Verify Landing

```bash
aws s3 ls s3://{bucket}/{domain}/{source}/ --recursive | tail -5
```

---

## Post-Start Verification (All Services)

After the service is started and first data expected:

| Check | Command | Pass Condition |
|---|---|---|
| Files exist in S3 | `aws s3 ls s3://{prefix}/ --recursive \| wc -l` | Count > 0 |
| Files are non-empty | `aws s3api head-object --key {first_file}` | ContentLength > 0 |
| Partitioned correctly | Check path matches `year=YYYY/month=MM/day=DD/` | Pattern matches |
| Format correct | Check file extension | `.parquet` or expected format |

Present summary:
```
Landing Pipeline Active
═══════════════════════
Service: AWS DMS (CDC)
Source: aurora-orders (5 tables)
Target: s3://data-landing-prod/sales/aurora-orders/

First data arrived:
  ✅ orders/year=2025/month=07/day=21/  (3 files, 2.1 MB)
  ✅ customers/year=2025/month=07/day=21/ (1 file, 540 KB)
  ✅ products/year=2025/month=07/day=21/ (1 file, 120 KB)
  ⏳ order_items/ (waiting for first CDC event...)
  ⏳ payments/ (waiting for first CDC event...)

Pipeline is operational. CDC will capture ongoing changes.
Want me to run a profiling report on the landed data?
```

## Troubleshooting

| Issue | Service | Fix |
|---|---|---|
| DMS task stuck in "starting" | DMS | Check replication instance capacity, SG rules |
| AppFlow "CONNECTOR_RUNTIME_ERROR" | AppFlow | Refresh OAuth token, check connector profile |
| Firehose "SUSPENDED" | Firehose | Fix S3 permissions, resume stream |
| Transfer Family "STARTING" stuck | Transfer | Check VPC endpoint or public hosting config |
| DataSync "UNAVAILABLE" | DataSync | Verify agent is online, check source path |
| No files after 30 minutes | Any | Check service logs, verify source has data |

## MUST Rules

- MUST verify the service is in a healthy state before declaring success
- MUST wait for at least one file to appear in S3 Landing before declaring operational
- MUST show which tables/objects have landed and which are still pending
- MUST suggest profiling after first successful data landing
- MUST NOT start DMS tasks without confirming the user is ready (initial full load can be expensive)
- MUST inform user of expected wait times (DMS: 5-15 min, AppFlow: 2-5 min, Firehose: buffer interval)
