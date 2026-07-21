---
name: decommissioning-data-source
description: >-
  Safely retire a data source from the platform. Disables schedules, archives tables,
  notifies consumers, revokes access, and cleans up resources. Ensures no data loss
  and maintains audit trail. Triggers on: decommission source, retire pipeline, remove
  data source, shutdown pipeline, offboard source, archive table, sunset data.
  Do NOT use for pausing pipelines temporarily (update Source Registry status to
  suspended instead), or troubleshooting (see steering/troubleshooting.md).
version: 1
argument-hint: '[source-name]'
author: MANIOX
---

# Decommission Data Source

Safely retire a data source and its associated pipeline from the platform. Follows a
controlled shutdown sequence that preserves data, notifies stakeholders, and cleans
up resources without leaving orphaned infrastructure.

## Philosophy

**Retirement is not deletion.** Data in the bronze layer is never destroyed — it's
archived with extended retention. Infrastructure is removed, schedules disabled, and
access revoked, but the historical data remains queryable for the retention period
defined in the contract.

## Workflow

### 1. Validate Decommission Request

Confirm the source exists and is eligible for retirement:

```bash
aws dynamodb get-item --table-name SourceRegistry \
  --key '{"source_id": {"S": "{source_id}"}}'
```

**Eligibility checks:**

| Check | Condition | Action if fails |
|---|---|---|
| Source exists in registry | Item found | Abort — source not registered |
| Source is active or suspended | status in (active, suspended) | Abort — already decommissioned |
| No active consumers | consumers list is empty or all acknowledged | Block — notify consumers first |
| User has authority | Requester is owner or platform admin | Abort — insufficient permissions |

If consumers exist, MUST notify them and wait for acknowledgment before proceeding.

### 2. Notify Consumers

For each registered consumer in the data contract:

```
Subject: Data Source Retirement Notice — {source_name}

The data source "{source_name}" ({domain}) is scheduled for retirement.

Timeline:
  - Deprecation: {today} — pipeline marked as deprecated
  - Last run: {today + grace_period} — final data ingestion
  - Archive: {today + grace_period + 7d} — infrastructure removed
  - Data retention: {today + retention_days} — table remains queryable

Action required:
  - Identify alternative data sources or confirm you no longer need this data
  - Acknowledge this notice by {deadline}

Contact: {owner} / {owner_email}
```

**Grace period by sensitivity:**

| Sensitivity | Grace Period | Reason |
|---|---|---|
| Public | 7 days | Low impact |
| Internal | 14 days | Business teams need time to adjust |
| Restricted / PHI | 30 days | Compliance review may be needed |

### 3. Mark as Deprecated

Update the Source Registry:

```bash
aws dynamodb update-item --table-name SourceRegistry \
  --key '{"source_id": {"S": "{source_id}"}}' \
  --update-expression "SET #s = :status, updated_at = :ts, decommission_date = :dd" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values '{
    ":status": {"S": "deprecated"},
    ":ts": {"S": "{now}"},
    ":dd": {"S": "{target_decommission_date}"}
  }'
```

Update data contract status:
```yaml
contract:
  status: "deprecated"
  deprecated_at: "{ISO 8601}"
  retirement_date: "{target_date}"
```

### 4. Disable Schedule (after grace period)

Stop the pipeline from running:

```bash
# Disable EventBridge rule
aws events disable-rule --name pipeline-{source_name}-schedule-prod

# Disable Glue trigger (if used instead of EventBridge)
aws glue update-trigger --name trigger-{source_name}-prod \
  --trigger-update '{"Schedule": null}'
```

Verify no new executions are starting:
```bash
aws stepfunctions list-executions \
  --state-machine-arn {pipeline_arn} \
  --status-filter RUNNING
```

If any executions are running, wait for them to complete before proceeding.

### 5. Archive Data

The bronze Iceberg table is NOT deleted. Instead:

**Set extended retention on Iceberg snapshots:**
```sql
ALTER TABLE {catalog}.{database}.{table}
SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '{retention_days * 86400000}'
);
```

**Tag table as archived:**
```bash
aws glue update-table --database-name {database} --table-input '{
  "Name": "{table}",
  "Parameters": {
    "platform_status": "archived",
    "archived_at": "{ISO 8601}",
    "retention_until": "{retention_end_date}",
    "original_source": "{source_name}"
  }
}'
```

**Archive the quarantine table similarly.**

### 6. Revoke Access

Remove pipeline-specific IAM permissions:

```bash
# Delete the Glue job role's inline policies
aws iam delete-role-policy \
  --role-name glue-ingest-{source_name}-prod \
  --policy-name s3-access-{source_name}

aws iam delete-role-policy \
  --role-name glue-ingest-{source_name}-prod \
  --policy-name secrets-access-{source_name}
```

Remove Lake Formation permissions if column-level access was granted:
```bash
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier": "{consumer_role_arn}"}' \
  --resource '{"Table": {"DatabaseName": "{database}", "Name": "{table}"}}' \
  --permissions '["SELECT"]'
```

**Do NOT revoke read access to the archived table** until retention period expires.
Consumers may still need to query historical data.

### 7. Remove Infrastructure

Destroy the CDK stack for the pipeline:

```bash
cdk destroy DataPipeline-{source_name}-prod \
  --context env=prod \
  --context source_name={source_name} \
  --force
```

This removes:
- Glue job definition
- Step Functions state machine
- EventBridge rule
- CloudWatch alarms
- IAM role (if stack-managed)

**Resources intentionally preserved:**
- S3 data (bronze table files)
- Glue Data Catalog table entry (tagged as archived)
- DynamoDB Source Registry entry (status=decommissioned)
- CloudWatch log groups (for audit — auto-expire per retention policy)

### 8. Clean Up Secrets

Remove credentials that are no longer needed:

```bash
# Schedule secret deletion (7-day recovery window)
aws secretsmanager delete-secret \
  --secret-id "data-platform/prod/{source_name}" \
  --recovery-window-in-days 7
```

MUST use recovery window (not force delete) in case rollback is needed.

If the Glue connection is not shared with other pipelines:
```bash
aws glue delete-connection --connection-name {connection_name}
```

**Check before deleting connection:**
```bash
# Ensure no other pipelines reference this connection
aws glue get-jobs --query "Jobs[?Connections.Connections[?contains(@, '{connection_name}')]]"
```

### 9. Finalize Registry

Mark decommission as complete:

```bash
aws dynamodb update-item --table-name SourceRegistry \
  --key '{"source_id": {"S": "{source_id}"}}' \
  --update-expression "SET #s = :status, updated_at = :ts, decommissioned_at = :da, retention_until = :ru" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values '{
    ":status": {"S": "decommissioned"},
    ":ts": {"S": "{now}"},
    ":da": {"S": "{now}"},
    ":ru": {"S": "{retention_end_date}"}
  }'
```

### 10. Post-Retention Cleanup (future action)

After the retention period expires, the archived table data can be permanently removed.
This is a separate action triggered by a scheduled check:

```bash
# Only after retention_until date has passed:
aws s3 rm s3://{bucket}/{table_path}/ --recursive
aws glue delete-table --database-name {database} --name {table}
aws glue delete-table --database-name {database} --name {table}_quarantine
```

This step MUST be a separate, explicit action — never automated without confirmation.

## Decommission Checklist

A decommission is complete when ALL of the following are true:

- [ ] Consumers notified and acknowledged (or grace period expired)
- [ ] Source Registry status = `decommissioned`
- [ ] Data contract status = `retired`
- [ ] EventBridge schedule disabled
- [ ] No pipeline executions running
- [ ] CDK stack destroyed
- [ ] Secrets scheduled for deletion
- [ ] Glue connection removed (if not shared)
- [ ] IAM inline policies removed
- [ ] Table tagged as archived with retention date
- [ ] Audit trail complete (who requested, who approved, when)

## MUST Rules

- MUST notify all consumers before decommissioning
- MUST observe the grace period — no immediate shutdowns for non-emergency cases
- MUST NOT delete bronze data — archive only
- MUST NOT delete Glue connections shared with other pipelines
- MUST use secret recovery window (not force-delete)
- MUST preserve audit trail (registry entry, CloudWatch logs)
- MUST record retention_until date for future cleanup
- MUST verify no running executions before removing infrastructure

## Gotchas

- Shared Glue connections: always check if other jobs reference it before deleting
- Lake Formation permissions: revoking table access doesn't revoke column-level grants — do both
- CloudFormation stack deletion can fail if resources were manually modified — check events
- Secrets Manager deletion has a minimum 7-day recovery window — plan accordingly
- Archived tables still incur S3 storage costs — factor into retention decisions
- Step Functions execution history persists 90 days after state machine deletion
