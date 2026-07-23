---
name: data-profiling
description: >-
  Run profiling jobs on landed S3 data using AWS Glue DataBrew. Generates reports
  with column types, PII candidates, statistics, cardinality, null rates, sample
  values, volume stats, and recommendations. Reports are informational only — they
  do not enforce anything. Triggers on: profile data, run profiling, what does the
  data look like, PII scan, data quality report, describe the dataset, column stats.
  Do NOT use for: transforming data, enforcing quality gates, masking PII, creating
  tables, or querying data.
version: 1
argument-hint: '[source-name|s3://landing-path]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Data Profiling

Run a profiling job on landed S3 data and publish a report with statistics,
PII candidates, and recommendations for downstream teams.

## Philosophy

**Profile, don't enforce.** The report is informational. It helps downstream
teams plan their processing (medallion architecture, quality rules, masking).
This skill does not act on findings or block anything.

## Workflow

### 1. Identify Landing Data

Determine what to profile:
- User provides source name → resolve to S3 path via convention
- User provides S3 path directly → use as-is
- Latest data: `s3://{bucket}/{domain}/{source}/{table}/year=YYYY/month=MM/day=DD/`

### 2. Create DataBrew Dataset (if not exists)

```bash
aws databrew create-dataset \
  --name "{source_name}-landing" \
  --input '{"S3InputDefinition":{"Bucket":"{bucket}","Key":"{prefix}/"}}'  \
  --format "PARQUET"
```

### 3. Run Profile Job

```bash
aws databrew create-profile-job \
  --name "{source_name}-profile" \
  --dataset-name "{source_name}-landing" \
  --role-arn "{databrew_role_arn}" \
  --output-location '{"Bucket":"{bucket}","Key":"reports/{source_name}/"}'

aws databrew start-job-run --name "{source_name}-profile"
```

### 4. Wait for Completion

```bash
aws databrew list-job-runs --name "{source_name}-profile" --max-results 1 \
  --query "JobRuns[0].State"
```

### 5. Retrieve and Format Report

- Fetch JSON results from `s3://{bucket}/reports/{source_name}/`
- Extract: schema, PII candidates, statistics, recommendations
- Generate Markdown summary for the workspace `reports/` directory
- Present key findings to the user

## PII Detection

Applied on top of DataBrew statistics:

| Category | Column Name Pattern | Value Pattern |
|---|---|---|
| Email | `email`, `e_mail` | `[\w.+-]+@[\w-]+\.[\w.-]+` |
| Phone | `phone`, `mobile`, `cell` | `\d{3}[-.\s]?\d{3}[-.\s]?\d{4}` |
| Name | `first_name`, `last_name`, `full_name` | — |
| Address | `address`, `street`, `city`, `zip` | — |
| SSN | `ssn`, `social_security` | `\d{3}-\d{2}-\d{4}` |
| Financial | `card_number`, `credit_card`, `cvv` | Luhn-valid sequences |

## Report Output

- **JSON** → `s3://{bucket}/reports/{source_name}/profile-{timestamp}.json`
- **Markdown** → `{workspace}/reports/{source_name}-profile.md`
- **DataBrew Console** → visual interactive view

## MUST Rules

- MUST profile Landing S3 data (not source directly)
- MUST flag PII candidates using name + value patterns
- MUST NOT enforce any rule or block any pipeline
- MUST NOT transform data
- MUST publish in both JSON and Markdown
