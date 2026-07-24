---
name: deploying-terraform
description: >-
  Initialize, plan, apply, and verify Terraform deployments for landing pipelines.
  Handles backend configuration, environment-specific tfvars, plan review with user
  confirmation, apply execution, post-deploy verification, and destroy for
  decommissioning. Triggers on: deploy, terraform apply, terraform plan, deploy to
  prod, deploy pipeline, release, promote, destroy infrastructure.
  Do NOT use for generating Terraform files (the power does that inline) or for
  service-specific provisioning logic (use provisioning-landing-pipeline).
version: 1
argument-hint: '[source-name|environment|''plan''|''apply''|''destroy'']'
author: "Rahul Agarwal, Manish Choudhary"
---

# Deploy Terraform

Initialize, plan, apply, and verify Terraform infrastructure for landing pipelines.

## Philosophy

**Plan before apply. Verify after apply. Never auto-apply without user confirmation.**

## Workflow

### 1. Locate Terraform Directory

Find the generated Terraform in the workspace:
```
{workspace}/{source-name}-landing-pipeline/terraform/
```

If not found, inform user that Terraform files need to be generated first.

### 2. Existing Resource Check

Before planning, verify what already exists to prevent duplicates:

```bash
# Check if resources in the TF state already exist in AWS
terraform state list 2>/dev/null

# If fresh deployment (no state), scan for potential conflicts:
# - S3 bucket name collision
aws s3api head-bucket --bucket {proposed_bucket_name} 2>&1

# - DMS instance name collision
aws dms describe-replication-instances \
  --filters Name=replication-instance-id,Values={proposed_instance_id} 2>&1

# - IAM role name collision
aws iam get-role --role-name {proposed_role_name} 2>&1
```

If conflicts found, present options:
- Import existing resource into Terraform state (`terraform import`)
- Use a different name (adjust variables)
- Skip creation (reference existing resource via data source)

### 3. Select Environment

Determine target environment from user or context:
- `dev` → uses `environments/dev.tfvars`
- `prod` → uses `environments/prod.tfvars`

If no environment specified, default to `dev` and confirm with user.

### 3. Initialize Backend

```bash
cd {source-name}-landing-pipeline/terraform
terraform init
```

Checks:
- Backend S3 bucket exists (create if not, with user confirmation)
- DynamoDB lock table exists (create if not)
- Provider plugins downloaded successfully
- State file accessible

**On failure:** Present specific error (usually missing S3 bucket or IAM permission).

### 4. Plan (Always Show Before Apply)

```bash
terraform plan -var-file=environments/{env}.tfvars -out=tfplan
```

Present summary to user:
```
Terraform Plan Summary
══════════════════════
  + 8 resources to create
  ~ 0 resources to modify
  - 0 resources to destroy

Resources:
  + aws_dms_replication_instance.landing
  + aws_dms_replication_task.cdc
  + aws_dms_endpoint.source
  + aws_dms_endpoint.s3_target
  + aws_iam_role.dms_role
  + aws_s3_bucket_notification.landing_trigger
  + aws_cloudwatch_metric_alarm.pipeline_failure
  + aws_databrew_profile_job.profiling (if profiling enabled)

Estimated monthly cost: ~$75 (DMS t3.medium + S3 storage)

Apply this plan?
```

**MUST wait for user confirmation before applying.**

### 5. Apply

```bash
terraform apply tfplan
```

Stream output to user. On completion, show:
- Resources created
- Output values (ARNs, endpoints, S3 paths)
- Any warnings

### 6. Post-Deploy Verification

After apply succeeds, verify the deployed resources are operational:

| Resource | Verification | Command |
|---|---|---|
| DMS replication instance | Status = available | `aws dms describe-replication-instances` |
| DMS task | Status = running (if started) | `aws dms describe-replication-tasks` |
| AppFlow flow | Status = Active | `aws appflow describe-flow` |
| Firehose stream | Status = ACTIVE | `aws firehose describe-delivery-stream` |
| Transfer Family server | State = ONLINE | `aws transfer describe-server` |
| S3 landing prefix | Accessible | `aws s3 ls s3://{bucket}/{prefix}/` |
| CloudWatch alarm | State = OK | `aws cloudwatch describe-alarms` |
| DataBrew job | Exists | `aws databrew describe-job` |

Present verification summary:
```
Post-Deploy Verification
════════════════════════
✅ DMS instance: available
✅ DMS task: ready (not started yet — start manually or wait for schedule)
✅ S3 landing prefix: accessible
✅ CloudWatch alarm: configured
✅ DataBrew profiling job: ready

All resources operational. Ready to start ingestion.
```

### 7. Destroy (for decommissioning)

When user wants to tear down:

```bash
terraform plan -destroy -var-file=environments/{env}.tfvars
```

Show what will be destroyed. **Require explicit confirmation** ("yes" or "destroy").

```bash
terraform destroy -var-file=environments/{env}.tfvars -auto-approve
```

**MUST warn about data loss:** "This will delete the ingestion service. S3 Landing data will remain unless you delete it separately."

## Environment Promotion

For dev → prod promotion:

1. Deploy to dev first, verify data flows
2. User confirms ready for prod
3. Run `terraform plan -var-file=environments/prod.tfvars`
4. Show differences (likely: larger instance, different bucket, alerts to prod SNS)
5. Apply after confirmation

## Terraform Version Handling

Before init, verify version matches `versions.tf`:

```bash
terraform version
# Compare against required_version constraint in versions.tf
```

If mismatch, warn user and suggest:
- Install correct version via `tfenv` or `tfswitch`
- Or update `versions.tf` constraint if intentional

## Error Handling

| Error | Cause | Fix |
|---|---|---|
| `Backend initialization required` | First run in new workspace | Run `terraform init` |
| `Error acquiring the state lock` | Concurrent execution | Wait or force-unlock with caution |
| `AccessDenied` on state bucket | Missing S3 permissions | Add s3:GetObject/PutObject on state bucket |
| `Plugin not found` | Network issue during init | Retry `terraform init` |
| `Resource already exists` | Terraform state mismatch | Import existing resource or rename |
| Apply timeout | Resource creation slow | Check AWS console for stuck resources |

## MUST Rules

- MUST show plan output before every apply (never blind-apply)
- MUST wait for explicit user confirmation before apply
- MUST verify resources are operational after apply
- MUST warn about data implications before destroy
- MUST use environment-specific tfvars (never apply without -var-file)
- MUST check Terraform version compatibility before init
- MUST NOT store tfplan files in git (add to .gitignore)
