# Troubleshooting

## Error Taxonomy

Known failure patterns from real-world POC deployments with auto-remediation:

| Error Signal | Root Cause | Auto-Remediation |
|---|---|---|
| `G.025X is only supported for gluestreaming` | Wrong worker type for glueetl | Auto-correct to G.1X |
| `States.Format` ASL validation failure | `\n` characters in format strings | Remove newlines, use spaces |
| `not authorized to perform: glue:GetConnection` | Missing IAM permission on SFN role | Auto-add permission |
| Security group `all ingress ports` error | Missing self-referencing SG rule | Auto-add self-ref rule |
| `Access denied for user` (MySQL/PostgreSQL) | Stale/wrong Secrets Manager secret | Cross-check host, prompt verify |
| Job timeout on first full load | Static timeout too short | Dynamic: provisioning + estimate × 3 |
| Files generated outside workspace | No workspace root detection | Always detect IDE workspace root |
| DMS task ERROR state | Source connectivity or grants | Check endpoint, source permissions |
| AppFlow CONNECTOR_RUNTIME_ERROR | OAuth token expired | Refresh connector profile |
| Firehose SUSPENDED state | S3 delivery failures | Check bucket permissions, resume |
| Transfer Family SFTP auth failure | SSH key mismatch or user mapping | Verify user config and key |
| DataSync task UNAVAILABLE | Source agent offline or path wrong | Check agent status, verify path |
| `Connect timed out` | VPC routing or NAT missing | Check subnet routes, SG egress |
| S3 `AccessDenied` on landing bucket | Missing s3:PutObject on service role | Add bucket policy or IAM |

---

## Diagnostic Order

When a pipeline fails, check in this order:

1. **Service status** — Is the ingestion service itself healthy?
   ```bash
   # DMS
   aws dms describe-replication-tasks --query "ReplicationTasks[?Status!='running']"
   # AppFlow
   aws appflow describe-flow-execution-records --flow-name {name} --max-results 1
   # Firehose
   aws firehose describe-delivery-stream --delivery-stream-name {name} --query "DeliveryStreamDescription.DeliveryStreamStatus"
   ```

2. **Connectivity** — Can the service reach the source?
   ```bash
   aws dms test-connection --replication-instance-arn {arn} --endpoint-arn {arn}
   ```

3. **Credentials** — Is the secret valid and current?
   ```bash
   aws secretsmanager describe-secret --secret-id {id} --query "LastChangedDate"
   ```

4. **Permissions** — Does the role have what it needs?
   ```bash
   aws iam simulate-principal-policy --policy-source-arn {role_arn} \
     --action-names s3:PutObject dms:* --resource-arns {bucket_arn}/*
   ```

5. **Landing verification** — Did any data actually arrive?
   ```bash
   aws s3 ls s3://{bucket}/{domain}/{source}/ --recursive | tail -5
   ```

---

## Self-Healing Patterns

| Pattern | Trigger | Action | Approval? |
|---|---|---|---|
| Retry with backoff | Transient failure (1-2 times) | Re-run after 5 min | No |
| Refresh credentials | Auth failure after rotation | Trigger secret retrieval | No |
| Scale timeout | Duration exceeded on first run | Set timeout = estimate × 3 | No |
| Fix SG rule | Self-reference missing | Add self-referencing ingress | No |
| Resume Firehose | SUSPENDED state | Fix permissions, resume stream | No |
| Restart DMS task | ERROR state, transient | Stop + start task | No |
| Circuit breaker | 5+ consecutive failures | Pause pipeline, alert on-call | No (but alerts) |

---

## Escalation

| Level | Who | When |
|---|---|---|
| 1 | Kiro (auto-fix) | Known patterns from error taxonomy |
| 2 | On-call engineer | Auto-fix failed or unknown error |
| 3 | Platform team | Networking, IAM policy changes |
| 4 | Source team | Source system issue, credential rotation |
