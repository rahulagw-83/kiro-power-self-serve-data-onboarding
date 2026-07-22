---
name: deploying-cdk-pipeline
description: >-
  Deploy, validate, and promote ingestion pipeline infrastructure across environments
  (dev, test, prod). Supports AWS CDK (Python), Terraform (HCL), and CloudFormation
  (YAML) based on user preference. Handles diff/plan, deploy/apply, post-deploy
  verification, promotion gates, and rollback. Triggers on: deploy pipeline, cdk deploy,
  terraform apply, promote to prod, deploy to dev, deploy stack, release pipeline.
  Do NOT use for creating connections (use connecting-to-data-source), writing ETL
  code (use ingesting-into-data-lake), or running queries (use querying-data-lake).
version: 2
argument-hint: '[source-name|environment|stack-name]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Deploy Pipeline Infrastructure

Deploy, validate, and promote ingestion pipeline infrastructure across environments.
Manages the full deployment lifecycle from dev through production with gate checks,
verification, and rollback capability.

**Two-layer context:** Deployment includes both Landing layer services (DMS tasks,
AppFlow flows, Firehose streams, Transfer Family servers) AND Raw layer infrastructure
(Glue jobs, Step Functions, EventBridge rules, CloudWatch alarms). Pre-deployment
validation from `steering/pre-deployment-validation.md` MUST pass before deploy.

## Philosophy

**Diff before deploy, verify after deploy, gate before promote.** Every deployment
follows a predictable pattern: show what will change, apply it, confirm it worked,
then decide whether to move forward. No silent deployments, no skipped validations.

## Onboarding Workflow Position

This skill executes **Step 13 (Deploy to Dev)**, **Step 14 (Integration Tests)**, and
**Step 15 (Promote to Prod)** of the master onboarding workflow.

**Inputs:** Generated pipeline code from Step 11 (Glue job + IaC in user's preferred tool).

**Outputs:** Deployed and verified infrastructure in the target environment with all
resources operational.

## IaC Tool Detection

The deployment workflow adapts based on the user's chosen IaC tool:

| Tool | Diff Command | Deploy Command | Rollback |
|---|---|---|---|
| AWS CDK | `cdk diff` | `cdk deploy` | CloudFormation auto-rollback |
| Terraform | `terraform plan` | `terraform apply` | `terraform apply` with previous state |
| CloudFormation | `aws cloudformation create-change-set` | `aws cloudformation execute-change-set` | `aws cloudformation rollback-stack` |

Detect by checking which directory exists in the pipeline workspace: `cdk/`, `terraform/`, or `cloudformation/`.

## Workflow

### 1. Pre-Deploy Checks

Before any deployment, verify preconditions:

**CDK:**

| Check | Command | Required |
|---|---|---|
| CDK bootstrapped | `cdk ls` (should list stacks without error) | Yes |
| AWS credentials valid | `aws sts get-caller-identity` | Yes |
| Pipeline code exists | Verify `src/`, `cdk/` directories present | Yes |
| Data contract approved | Check Source Registry status = `contracted` or `active` | Yes |
| No active deployment | Check CloudFormation stack status != `*_IN_PROGRESS` | Yes |

**Terraform:**

| Check | Command | Required |
|---|---|---|
| Terraform installed | `terraform version` | Yes |
| Correct version | Compare output against `required_version` in `versions.tf` | Yes |
| AWS credentials valid | `aws sts get-caller-identity` | Yes |
| Backend initialized | `terraform init` (idempotent) | Yes |
| Pipeline code exists | Verify `src/`, `terraform/` directories present | Yes |
| Data contract approved | Check Source Registry status = `contracted` or `active` | Yes |
| No active state lock | Check DynamoDB lock table | Yes |
| AWS credentials valid | `aws sts get-caller-identity` | Yes |
| Pipeline code exists | Verify `src/`, `cdk/`, `config/` directories present | Yes |
| Data contract approved | Check Source Registry status = `contracted` or `active` | Yes |
| No active deployment | Check CloudFormation stack status != `*_IN_PROGRESS` | Yes |

If data contract is not yet approved, STOP and redirect to `data-contract-authoring`.

### 2. Diff and Review

Show what will change before applying:

```bash
cdk diff DataPipeline-{source_name}-{env} \
  --context env={env} \
  --context source_name={source_name}
```

Present the diff summary:
- Resources to be CREATED (green)
- Resources to be UPDATED (yellow)
- Resources to be DELETED (red — requires explicit user confirmation)

**Rules:**
- If ANY resource will be deleted, MUST get user confirmation before proceeding
- If IAM policy changes detected, MUST highlight them explicitly
- If security group changes detected, MUST highlight them explicitly

### 3. Deploy

Execute the deployment:

```bash
cdk deploy DataPipeline-{source_name}-{env} \
  --context env={env} \
  --context source_name={source_name} \
  --require-approval {approval_level} \
  --outputs-file cdk-outputs-{env}.json
```

**Approval levels by environment:**
| Environment | `--require-approval` | Reason |
|---|---|---|
| dev | `never` | Fast iteration |
| test | `broadening` | Alert on permission expansion |
| prod | `broadening` | Alert on permission expansion |

### 4. Post-Deploy Verification

After deployment succeeds, verify all resources are operational:

**Glue Job:**
```bash
aws glue get-job --job-name ingest-{source_name}-{env}
```
Verify: job exists, correct script location, correct connections attached.

**Step Functions:**
```bash
aws stepfunctions describe-state-machine \
  --state-machine-arn arn:aws:states:{region}:{account}:stateMachine:ingest-{source_name}-{env}
```
Verify: state machine exists, status = ACTIVE.

**EventBridge Rule (prod only):**
```bash
aws events describe-rule --name pipeline-{source_name}-schedule-{env}
```
Verify: rule exists, state = ENABLED (prod) or DISABLED (dev/test).

**CloudWatch Alarms:**
```bash
aws cloudwatch describe-alarms \
  --alarm-name-prefix pipeline-{source_name}-{env}
```
Verify: alarms exist for job-failure, freshness-sla, quarantine-rate.

**IAM Role:**
```bash
aws iam get-role --role-name glue-ingest-{source_name}-{env}
```
Verify: role exists with correct trust policy for Glue service.

### 5. Smoke Test Trigger

After verification, trigger a limited smoke test:

```bash
aws stepfunctions start-execution \
  --state-machine-arn {state_machine_arn} \
  --input '{"smoke_test": true, "max_rows": 100}'
```

Wait for completion (timeout: 10 minutes for dev):
```bash
aws stepfunctions describe-execution --execution-arn {execution_arn}
```

**Pass criteria:**
- Execution status = SUCCEEDED
- At least 1 row written to target table
- Zero rows in quarantine (for smoke test data)
- CloudWatch metrics emitted

### 6. Environment Promotion

Promotion follows a two-gate process:

**Gate 1: Dev → Test**

| Requirement | Verification |
|---|---|
| Smoke test passes in dev | Step 5 succeeded |
| Data contract approved | Source Registry status = `active` |
| No restricted data without masking confirmed | Schema classification reviewed |
| Resource sizing validated | Cost estimate within budget |

```bash
cdk deploy DataPipeline-{source_name}-test \
  --context env=test \
  --context source_name={source_name} \
  --require-approval broadening
```

**Gate 2: Test → Prod**

| Requirement | Verification |
|---|---|
| Integration tests pass in test | Full-volume run succeeded |
| Performance within SLA | Duration < 2x expected |
| Rollback procedure documented | Rollback steps in README |
| On-call team notified | Notification sent |
| Monitoring dashboard created | Dashboard exists |
| Runbook published | Runbook committed |

```bash
cdk deploy DataPipeline-{source_name}-prod \
  --context env=prod \
  --context source_name={source_name} \
  --require-approval broadening
```

### 7. Rollback (if needed)

If deployment or smoke test fails:

**Option A: CloudFormation rollback (automatic)**
CloudFormation automatically rolls back on deploy failure.

**Option B: Manual rollback to previous version**
```bash
# Find previous successful stack version
aws cloudformation describe-stack-events \
  --stack-name DataPipeline-{source_name}-{env} \
  --query "StackEvents[?ResourceStatus=='UPDATE_COMPLETE'].Timestamp" \
  --output text | head -2

# Redeploy previous code version
git checkout {previous_commit}
cdk deploy DataPipeline-{source_name}-{env} --context env={env}
```

**Option C: Destroy and recreate (dev only)**
```bash
cdk destroy DataPipeline-{source_name}-dev --force
# Fix issues, then redeploy
cdk deploy DataPipeline-{source_name}-dev --context env=dev
```

MUST NOT use Option C in test or prod — data loss risk.

### 8. Update Registry and Notify

After successful production deployment:

- Update Source Registry: status = `active`, pipeline_arn = {state_machine_arn}
- Enable EventBridge schedule (prod only)
- Send completion notification to requester
- Record deployment metadata (commit hash, deployer, timestamp)

## Environment Configuration

| Parameter | Dev | Test | Prod |
|---|---|---|---|
| Worker count | 2 | 5 | Per sizing heuristic |
| Worker type | G.025X | G.1X | Per sizing heuristic |
| Timeout (min) | 10 | 30 | Per sizing heuristic |
| EventBridge | Disabled | Disabled | Enabled |
| Alarm actions | Log only | SNS (team) | SNS (on-call) |
| Retention | 7 days | 30 days | Per contract |

## MUST Rules

- MUST run `cdk diff` before every deploy and present changes to user
- MUST verify all resources are operational after deploy (not just "stack complete")
- MUST run smoke test in dev before promoting to test
- MUST complete all gate requirements before promoting to prod
- MUST NOT deploy to prod without a passing integration test in test
- MUST NOT enable EventBridge schedules in non-prod environments
- MUST NOT destroy stacks in prod without explicit user confirmation
- MUST record deployment audit trail (who, what, when, which commit)

## Gotchas

- `cdk deploy` can succeed even if the Glue job configuration is wrong — always verify resources after
- EventBridge rules default to ENABLED — explicitly disable in dev/test via CDK context
- CloudFormation rollback can leave orphaned S3 objects — clean up manually if needed
- IAM policy changes take seconds to propagate — wait 10s after deploy before testing
- Step Functions definition updates require a new deployment (not just script changes)
