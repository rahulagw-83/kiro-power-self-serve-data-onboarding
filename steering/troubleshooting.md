# Troubleshooting Failed Pipelines

## Trigger Conditions

This guide activates when:
- A Glue job fails (exit code ≠ 0)
- Step Functions execution enters FAILED or TIMED_OUT state
- DMS replication task fails or enters ERROR state
- AppFlow flow run fails
- Firehose delivery stream enters SUSPENDED state
- Freshness SLA alarm fires (no successful run within threshold)
- Data quality checks fail (DQDL quarantine records exceed threshold)
- Pipeline produces zero rows when rows were expected
- CloudWatch alarm triggers for any pipeline metric

---

## Error Taxonomy with Auto-Remediation

Known failure patterns mapped to automatic fixes. The power attempts remediation before escalating.

| Error Signal | Root Cause | Auto-Remediation | Requires Approval? |
|---|---|---|---|
| `security group must open all ingress ports` | Missing self-referencing SG rule | Add self-referencing rule to Glue SG | No (non-destructive) |
| `Access denied for user` (MySQL/PostgreSQL) | Wrong credentials | Check Secrets Manager keys, prompt to verify/rotate | Yes |
| `ORA-01017: invalid username/password` | Oracle credentials wrong | Verify secret, check Oracle account lock status | Yes |
| `TIMEOUT — Waiting to start the job run` | Glue capacity queue full | Increase timeout, suggest Flex execution class, suggest off-peak schedule | No |
| `not authorized to perform: glue:GetConnection` | Missing IAM permission | Auto-add `glue:GetConnection` to SFN execution role | No |
| `SCHEMA_VALIDATION_FAILED: value must be a valid JSONPath` | Bad `States.Format` expression | Fix JSONPath syntax in state machine definition | No |
| `G.025X is only supported for job command gluestreaming` | Wrong worker type for glueetl | Auto-correct to G.1X | No |
| `UnableToFindVpcEndpoint` | Missing S3 VPC endpoint | Create S3 gateway endpoint in VPC | No |
| `No suitable driver found` | Missing JDBC driver JAR | Upload driver JAR to S3, add `--extra-jars` | No |
| `Connect timed out` / `Connection refused` | VPC routing or NAT missing | Check subnet routes, SG egress, NAT gateway | No (diagnostic only) |
| `DMS replication task ERROR` | Source connectivity or permission | Check DMS endpoint connectivity, source grants | No (diagnostic) |
| `AppFlow CONNECTOR_RUNTIME_ERROR` | OAuth token expired | Refresh connector profile credentials | Yes |
| `Firehose SUSPENDED` | Delivery failures exceeding threshold | Check S3 bucket permissions, fix, resume | No |
| DQDL quarantine rate > threshold | Data quality degradation | Alert, inspect quarantine table, check source | Yes |
| `Container killed by YARN` / OOM | Worker memory exhausted | Scale up: G.1X → G.2X or increase worker count | No |

---

## Step 1: Triage

### Gather Failure Context

**CloudWatch Logs query — last Glue job error:**
```
# CloudWatch Insights query for Glue job errors
fields @timestamp, @message
| filter @logStream like /glue/
| filter @message like /ERROR|Exception|FATAL/
| sort @timestamp desc
| limit 20
```

**Athena query — recent pipeline runs:**
```sql
SELECT pipeline_name, run_id, status, error_message, 
       started_at, completed_at,
       date_diff('minute', started_at, completed_at) as duration_min
FROM pipeline_registry
WHERE started_at > current_timestamp - interval '24' hour
ORDER BY started_at DESC
LIMIT 50;
```

### Symptom → Category Routing

| Symptom | Category | Go To |
|---|---|---|
| `Connection timed out` / `Connection refused` | Connectivity | Step 2a |
| `Access denied` / `Authentication failed` / `Invalid credentials` | Credentials | Step 2b |
| `Data quality check failed` / Quarantine records > threshold | Data Quality | Step 2c |
| `0 rows processed` / `No new data` / Empty DataFrame | Source Empty | Step 2d |
| Job succeeds but takes 3x+ longer than baseline | Performance | Step 2e |
| `OutOfMemoryError` / `Container killed by YARN` | Performance | Step 2e |
| `SchemaEvolutionException` / `Type mismatch` | Data Quality | Step 2c |

---

## Step 2a: Connectivity Issues

| Check | Command | Resolution |
|---|---|---|
| Security group self-reference | `aws ec2 describe-security-groups --group-ids sg-xxx` | Add inbound rule: source=self, protocol=all TCP |
| Outbound to DB port | Check egress rules for DB port (1521/5432/3306/5439) | Add egress rule to DB subnet CIDR on correct port |
| S3 VPC endpoint exists | `aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=vpc-xxx"` | Create S3 gateway endpoint, attach to Glue subnet route table |
| Subnet AZ matches connection | `aws ec2 describe-subnets --subnet-ids subnet-xxx` | Update connection to use subnet in correct AZ |
| DNS resolution enabled | `aws ec2 describe-vpc-attribute --vpc-id vpc-xxx --attribute enableDnsSupport` | Enable DNS support and DNS hostnames on VPC |
| Database is reachable | Run Glue test-connection: `aws glue test-connection --connection-name xxx` | Fix network path: routing, NACLs, or DB security group |
| NAT Gateway (for external DBs) | Check route table for 0.0.0.0/0 → NAT | Add NAT gateway route for Snowflake/BigQuery connections |

---

## Step 2b: Credential Issues

| Check | Resolution |
|---|---|
| Secret exists: `aws secretsmanager describe-secret --secret-id xxx` | Create secret if missing |
| Secret format: Verify keys match expected schema (username, password, host, port) | Update secret with correct key names |
| Glue role has `secretsmanager:GetSecretValue` permission | Add IAM policy to Glue role |
| KMS decrypt permission (if secret uses CMK) | Grant `kms:Decrypt` to Glue role for the secret's KMS key |
| Password expired or rotated | Update secret value; check rotation schedule |
| IAM DB auth token expired (RDS/Aurora) | Token auto-refreshes; ensure job generates token at runtime |
| Snowflake key pair: private key format | Ensure PKCS8 format, no passphrase, stored correctly in secret |

**MUST NOT log credential values.** Only verify key names and permissions.

---

## Step 2c: Data Quality Issues

**Query quarantine table for recent failures:**
```sql
SELECT pipeline_name, rule_name, failure_count, sample_values,
       quarantine_timestamp
FROM bronze.quarantine_records
WHERE quarantine_timestamp > current_timestamp - interval '24' hour
ORDER BY quarantine_timestamp DESC
LIMIT 100;
```

| Cause | Resolution |
|---|---|
| Schema drift: new columns in source | If `additive_auto`: columns added automatically. If `strict`: update contract, redeploy. |
| Type mismatch: source changed column type | Check evolution_policy. If rejected: coordinate with source team to revert or update contract. |
| Null spike: required field suddenly NULL | Investigate source system. Temporary: lower completeness threshold. Permanent: fix source. |
| Value out of range: constraint violation | Review constraint in contract. If legitimate new values: update allowed_values or range. |
| Volume anomaly: >50% variance from baseline | Check for duplicate runs, source backfill, or upstream pipeline failure. |
| Encoding issues: garbled characters | Verify source encoding matches contract (UTF-8). Add encoding parameter to source config. |

---

## Step 2d: Source Empty / No New Data

### 4 Checks

1. **Verify source has data:**
   ```bash
   # For S3 sources
   aws s3 ls s3://source-bucket/incoming/ --recursive | tail -5

   # For JDBC sources — run count query via Glue connection
   # Check watermark: what was the last extracted timestamp?
   aws dynamodb get-item --table-name pipeline-watermarks-prod \
     --key '{"pipeline_name": {"S": "oracle-hr-employees"}}'
   ```

2. **Check watermark is not ahead of source:**
   - Compare DynamoDB watermark timestamp with MAX(incremental_column) in source
   - A future watermark (e.g., from timezone mismatch) causes permanent empty results

3. **Verify schedule ran:**
   ```bash
   aws stepfunctions list-executions \
     --state-machine-arn arn:aws:states:us-east-1:111111111111:stateMachine:pipeline-name \
     --status-filter SUCCEEDED \
     --max-results 5
   ```

4. **Check source system status:**
   - Is the source database in maintenance window?
   - Has the source API rate limit been hit?
   - Is there an upstream pipeline that feeds this source?

**Resolution:** If watermark is ahead of source, reset watermark in DynamoDB. If source genuinely has no new data, this is normal — suppress freshness alarm for expected quiet periods.

---

## Step 2e: Performance Degradation

| Cause | Fix |
|---|---|
| Data volume spike (10x+ normal) | Increase `number_of_workers` temporarily. Check source for backfill. |
| Worker type too small | Upgrade from G.025X → G.1X or G.1X → G.2X |
| Full table scan (watermark lost) | Restore watermark from DynamoDB backup. Never re-run without watermark. |
| Shuffle spill to disk | Increase workers or use G.2X (more memory). Repartition data. |
| S3 throttling (503 SlowDown) | Add random prefix to S3 paths. Reduce parallelism. |
| Iceberg metadata bottleneck | Run `OPTIMIZE` and snapshot expiration on target table. |
| Network throttling | Check if NAT gateway bandwidth limit reached. Consider VPC endpoint. |

**Glue Metrics Commands:**
```bash
# Get job run metrics
aws glue get-job-run --job-name "s3-file-ingest-prod" --run-id "jr_xxxxx" \
  --query "JobRun.{State:JobRunState,ExecutionTime:ExecutionTime,Workers:NumberOfWorkers,DPU:AllocatedCapacity}"

# CloudWatch metrics for Glue job
aws cloudwatch get-metric-statistics \
  --namespace "Glue" \
  --metric-name "glue.driver.aggregate.bytesRead" \
  --dimensions Name=JobName,Value=s3-file-ingest-prod Name=Type,Value=gauge \
  --start-time "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
  --period 300 --statistics Sum
```

---

## Step 3: Apply Fix

After identifying the root cause:

1. **Make the change** (IAM policy, security group, secret update, config change)
2. **Re-run the pipeline manually:**
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn arn:aws:states:us-east-1:111111111111:stateMachine:pipeline-name \
     --input '{"source_path": "s3://bucket/path/", "table_name": "target_table"}'
   ```
3. **Monitor execution:**
   ```bash
   aws stepfunctions describe-execution \
     --execution-arn arn:aws:states:us-east-1:111111111111:execution:pipeline-name:run-id
   ```

---

## Step 4: Verify Recovery

### CLI Commands
```bash
# Verify Glue job succeeded
aws glue get-job-runs --job-name "s3-file-ingest-prod" --max-results 1 \
  --query "JobRuns[0].{State:JobRunState,StartTime:StartedOn,EndTime:CompletedOn}"

# Verify data landed in target
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) as row_count, MAX(ingestion_timestamp) as latest FROM bronze.target_table" \
  --work-group primary \
  --query-execution-context Database=bronze
```

### CloudWatch Check
```bash
# Verify alarm returned to OK
aws cloudwatch describe-alarms \
  --alarm-names "pipeline-freshness-sla-prod" \
  --query "MetricAlarms[0].StateValue"
```

### Confirm Criteria
- [ ] Job completed with SUCCEEDED status
- [ ] Row count > 0 and within volume_bounds
- [ ] No new quarantine records generated
- [ ] Freshness alarm returned to OK state
- [ ] Downstream consumers can query new data

---

## Step 5: Post-Mortem

### Required When
- Pipeline was down for > 2× freshness SLA
- Data loss occurred (records permanently lost)
- Same failure happened 3+ times in 30 days
- Manual intervention was required by on-call engineer

### Post-Mortem Template

```markdown
## Incident: [Pipeline Name] — [Date]

**Duration:** [Start time] to [Resolution time]
**Impact:** [Number of missed SLAs, affected consumers]
**Severity:** [P1/P2/P3]

### Timeline
- HH:MM — Alarm fired
- HH:MM — Engineer acknowledged
- HH:MM — Root cause identified
- HH:MM — Fix applied
- HH:MM — Recovery verified

### Root Cause
[Clear description of what failed and why]

### Resolution
[What was done to fix it]

### Prevention
- [ ] Action item 1 (owner, deadline)
- [ ] Action item 2 (owner, deadline)

### Lessons Learned
[What could we improve in detection, response, or prevention?]
```

---

## Escalation Path

| Level | Who | When | Time Target |
|---|---|---|---|
| 1 | Kiro (automated) | First detection — auto-diagnose and attempt fix | 0-15 min |
| 2 | On-call Engineer | Kiro cannot resolve, or fix requires infrastructure change | 15-60 min |
| 3 | Platform Team | VPC/networking issue, IAM policy change, cross-account access | 1-4 hours |
| 4 | Source Team | Source system issue, schema change, credentials rotation | 4-24 hours |

---

## Self-Healing Recommendations

| Pattern | Trigger | Auto-Fix | Requires Approval |
|---|---|---|---|
| Retry with backoff | Transient network error (1-2 occurrences) | Re-run job after 5 min wait | No |
| Scale up workers | OOM or execution time > 3× baseline | Increase workers by 50% | No |
| Reset watermark | Watermark in future (timezone drift) | Set watermark to MAX(source) - 1 hour | Yes |
| Refresh credentials | Auth failure after secret rotation | Trigger secret retrieval, re-run | No |
| Skip corrupt file | Single file parse error, others succeed | Move file to dead-letter, continue | No |
| Compact Iceberg | Write latency spike, metadata size > 100MB | Run OPTIMIZE on target table | No |
| Circuit breaker | 5+ consecutive failures | Pause pipeline, alert on-call | No (but alerts) |
| Failover connection | Primary DB unreachable for > 10 min | Switch to read replica connection | Yes |

---

## Alert Rules Reference

| Alert | Metric | Threshold | Action |
|---|---|---|---|
| Job Failure | `glue.driver.aggregate.numFailedTasks` | ≥ 1 | Run troubleshooting triage |
| Freshness SLA Breach | `HoursSinceLastSuccess` | > contract SLA | Check source, verify schedule |
| Volume Anomaly | `RowsProcessed` variance | > 50% from 7-day avg | Check for duplicates or source issues |
| Quality Gate Failure | `QuarantineRecordCount` | > configured threshold | Inspect quarantine table |
| Duration Spike | `ExecutionTime` | > 3× rolling average | Check data volume, worker sizing |
| Step Functions Failure | `ExecutionsFailed` | ≥ 1 | Check state machine execution history |
| S3 Access Denied | Glue job log contains 403 | Any occurrence | Verify IAM role and bucket policy |
| OOM Kill | Glue log contains `Container killed` | Any occurrence | Scale up worker type |

---

## Definition of Done

A pipeline failure is resolved when ALL of the following are true:

1. **Root cause identified** — documented in pipeline registry or post-mortem
2. **Fix applied** — infrastructure, code, or configuration change deployed
3. **Recovery verified** — successful pipeline run with data in target table
4. **Alarms clear** — all CloudWatch alarms returned to OK state
5. **Consumers notified** — if SLA was breached, downstream teams informed
6. **Prevention documented** — if recurring, self-healing pattern added or escalated
