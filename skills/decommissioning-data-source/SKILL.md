---
name: decommissioning-data-source
description: >-
  Safely retire a landing pipeline. Disables the ingestion service (DMS task,
  AppFlow flow, Firehose stream, Transfer Family), removes infrastructure via
  Terraform destroy, and optionally deletes landed S3 data after retention period.
  Triggers on: decommission, retire, remove source, shutdown pipeline, offboard,
  stop syncing, delete pipeline.
version: 2
argument-hint: '[source-name]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Decommission Data Source

Safely retire a landing pipeline. Disable the ingestion service, destroy
infrastructure, and clean up S3 data after retention expires.

## Workflow

### 1. Confirm Source

Verify the source exists and user has authority to decommission.

### 2. Stop Ingestion

| Service | Stop Command |
|---|---|
| DMS | `aws dms stop-replication-task --replication-task-arn {arn}` |
| AppFlow | Delete or disable the scheduled flow |
| Firehose | `aws firehose delete-delivery-stream --delivery-stream-name {name}` |
| Transfer Family | `aws transfer stop-server --server-id {id}` |
| DataSync | `aws datasync cancel-task-execution --task-execution-arn {arn}` |
| EventBridge rule | `aws events disable-rule --name {rule_name}` |

### 3. Destroy Terraform Infrastructure

```bash
cd {source-name}-landing-pipeline/terraform
terraform destroy -var-file=environments/{env}.tfvars -auto-approve
```

This removes: ingestion service resources, IAM roles, CloudWatch alarms,
DataBrew profiling job (if configured).

### 4. Handle S3 Data

**Options (ask user):**
- **Keep with lifecycle rule:** Data expires after `landing_retention_days`
- **Delete immediately:** `aws s3 rm s3://{bucket}/{domain}/{source}/ --recursive`
- **Archive to Glacier:** Add S3 lifecycle rule for immediate Glacier transition

### 5. Clean Up Secrets

```bash
aws secretsmanager delete-secret \
  --secret-id "{secret_id}" \
  --recovery-window-in-days 7
```

Use 7-day recovery window (not force-delete) in case rollback needed.

### 6. Update Source Registry

```bash
aws dynamodb update-item \
  --table-name {prefix}-source-registry \
  --key '{"source_id": {"S": "{source_id}"}}' \
  --update-expression "SET #s = :status, decommissioned_at = :ts" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values '{":status": {"S": "decommissioned"}, ":ts": {"S": "{ISO8601_now}"}}'
```

### 7. Notify

- Inform requesting team that pipeline is decommissioned
- Record decommission date and reason

## Checklist

- [ ] Ingestion service stopped
- [ ] Terraform infrastructure destroyed
- [ ] S3 data retention decision made (keep/delete/archive)
- [ ] Secrets scheduled for deletion
- [ ] **Source Registry updated: status = decommissioned**
- [ ] Team notified

## MUST Rules

- MUST stop ingestion before destroying infrastructure
- MUST ask user about S3 data disposition (never auto-delete)
- MUST use secret recovery window (7 days minimum)
- MUST NOT delete S3 data without explicit user confirmation
