# Onboarding Workflow: Source → S3 Landing → Profile

> **Master Workflow** — The canonical flow for onboarding any data source.
> Scope: move data to S3 Landing as-is, then profile it. No transforms, no
> schema enforcement, no masking, no downstream processing.

---

## Flow

```
1. Discovery (5 turns max)
   ↓
2. Service recommendation + cost estimate → user confirms
   ↓
3. Collect connection details for chosen service
   ↓
4. Pre-deployment validation (connectivity, credentials, networking, IAM)
   ↓
5. Generate Terraform (Landing + optional Profiling)
   ↓
6. Deploy to target environment
   ↓
7. Verify data arrives in S3 Landing
   ↓
8. Run profiling job (if requested)
   ↓
9. Publish profiling report
```

---

## Phase 1: Discovery

Ask 1-2 questions per turn. 5 turns max.

| Turn | Question | Purpose |
|---|---|---|
| 1 | What kind of data source? (database / SaaS / streaming / files / on-prem) | Determine source category |
| 2 | One-time load or recurring? If recurring, freshness? (real-time / hourly / daily / weekly) | Determine sync mode |
| 3 | Approximate volume per batch? | Size the service |
| — | → Recommend service + show cost estimate → user confirms | — |
| 4 | Naming convention, tags, existing resource scan + **registry duplicate check** | Reuse + prevent duplicates |
| 5 | Connection details (endpoint, credentials location, etc.) | Configure the service |
| 6 | Want a profiling report after data lands? (recommended) | Enable/skip profiling |

**Registry check at Turn 4:**
```bash
aws dynamodb query --table-name {prefix}-source-registry \
  --index-name source_name-index \
  --key-condition-expression "source_name = :name" \
  --expression-attribute-values '{":name": {"S": "{source_name}"}}'
```
If found with `status=active` → warn: "This source is already onboarded. Update or skip?"

**After Turn 6:** proceed directly to validation → deploy.

---

## Phase 2: Pre-Deployment Validation

Run ALL checks from `steering/pre-deployment-validation.md`. If ANY fail, stop.

| Check | What | On Failure |
|---|---|---|
| Connectivity | Can we reach the source? | Stop. Fix first. |
| Credentials | Secret exists, keys valid, host matches? | Stop. Verify secret. |
| Networking | SG self-reference, subnet routes, S3 endpoint? | Auto-fix if IAM allows. |
| IAM | Roles have required permissions? | Auto-add with confirmation. |
| State Machine | ASL valid? No `\n` in States.Format? | Fix expressions. |
| Worker Type | G.025X not used for glueetl? | Auto-correct to G.1X. |
| Timeout | Dynamic calculation applied? | Recalculate. |

---

## Phase 3: Generate + Deploy

1. Generate Terraform in workspace: `{source-name}-landing-pipeline/terraform/`
2. Confirm path with user before writing
3. Include: Landing infrastructure + S3 bucket/prefix + optional DataBrew profiling job
4. Deploy: `terraform init && terraform plan && terraform apply`
5. Verify: data arrives in `s3://{bucket}/{domain}/{source}/{table}/year=.../`

---

## Phase 4: Profile (Optional)

If user opted in at Turn 5:
1. DataBrew profile job runs on landed data
2. Report published to `s3://{bucket}/reports/{source}/` (JSON) and workspace `reports/` (Markdown)
3. Report is informational only — no enforcement, no gates, no blocking

---

## Definition of Done

An onboarding is complete when:
- [ ] Data arrives in S3 Landing at the expected path
- [ ] Partitioned by `year=YYYY/month=MM/day=DD/`
- [ ] Format matches expectation (Parquet preferred)
- [ ] Schedule running (if recurring)
- [ ] CloudWatch alarm configured for pipeline failure
- [ ] Profiling report published (if requested)
- [ ] **Source registered in DynamoDB Source Registry with `status=active`**
