---
name: "self-serve-data-onboarding"
displayName: "Self-Serve Data Onboarding"
description: "Move data from any source to S3 Landing using the right AWS service, then profile it. Supports databases (DMS), SaaS (AppFlow), streaming (Firehose), file drops (Transfer Family), and on-prem (DataSync). Produces a profiling report with PII candidates, statistics, and recommendations. Terraform-only IaC."
keywords: ["data ingestion", "aws dms", "appflow", "kinesis firehose", "transfer family", "datasync", "s3", "landing zone", "data profiling", "databrew", "terraform", "self-serve", "data lake", "cdc", "pii detection"]
author: "Rahul Agarwal, Manish Choudhary"
---

# Self-Serve Data Onboarding

## Overview

This power moves data from any source to S3 (Landing layer) using the simplest, cheapest AWS service for that source type, then profiles the landed data and publishes a report.

**What it does:**
1. Select the right AWS ingestion service per source type (DMS, AppFlow, Firehose, Transfer Family, DataSync)
2. Move data to S3 exactly as-is — no transforms, no schema enforcement, no masking
3. Run a profiling job (AWS Glue DataBrew) on the landed data
4. Publish a profiling report with statistics, PII candidates, and recommendations

**What it does NOT do:**
- Medallion architecture (Bronze/Silver/Gold)
- Data transformation of any kind
- Schema enforcement or schema evolution handling
- PII masking or redaction
- Data quality gates that block pipelines
- Quarantine tables or deduplication
- Data contracts as enforcement mechanisms
- Iceberg table format or MERGE INTO logic

Those are downstream concerns. The profiling report gives downstream teams the information they need to build on top of the landed data.

---

## Getting Started

Describe what you need. The power asks 3-4 questions, recommends a service, validates connectivity, and deploys via Terraform.

Examples:
- "Onboard our Aurora MySQL orders database"
- "Set up daily Salesforce contacts sync to S3"
- "We get vendor CSV files via SFTP — land them in S3"
- "Stream Kafka events to S3"
- "Migrate our on-prem Oracle database to S3"
- "Profile the data that landed yesterday"

---

## Architecture

```
Source System
     │
     ▼  (simplest AWS service for this source type)
┌──────────────────────────────────────────────────────┐
│  S3 LANDING                                          │
│  s3://{bucket}/{domain}/{source}/{table}/            │
│       └── year=YYYY/month=MM/day=DD/{files}          │
│                                                      │
│  Data arrives exactly as-is from source              │
│  No transforms, no audit columns, no type casting    │
│  Append-only — never overwrite or mutate             │
│  Format: whatever the service outputs (Parquet pref) │
│  Retention: configurable (default 90 days)           │
└──────────────────────────────────────────────────────┘
     │
     ▼  (optional, recommended)
┌──────────────────────────────────────────────────────┐
│  DATA PROFILING (AWS Glue DataBrew)                  │
│  - Column types, nulls, cardinality, distributions   │
│  - PII candidate detection (name/value patterns)     │
│  - Suggested keys, partitions, quality observations  │
│  - Published as JSON + Markdown report               │
└──────────────────────────────────────────────────────┘
```

---

## Discovery Flow

The agent asks 1-2 questions per turn:

**Turn 1:** "What kind of data source?" (database / SaaS / streaming / files / on-prem)

**Turn 2:** "One-time load or recurring sync? If recurring, what freshness?" (real-time / hourly / daily / weekly)

**Turn 3:** "Approximate volume per batch?"

→ Agent recommends a specific AWS service and explains why (one sentence).
→ Shows cost comparison. User confirms or asks for alternative.

**Turn 4 — Naming, Tags, and Existing Resources:**

Ask once, remember for session:

> "A few setup questions before I generate anything:
>
> 1. **Naming convention** — Does your org have a pattern for AWS resource names?
>    (e.g., `{env}-{team}-{source}-{purpose}`, or a required prefix like `mycompany-`)
>    Default if none: `{env}-{source_name}-{service}` (e.g., `prod-aurora-orders-dms-task`)
>
> 2. **Required tags** — Any mandatory tags? (e.g., team, cost-center, project, environment)
>
> 3. **Let me check what already exists in your account..."

Then run the **Existing Resource Scan** (see below).

**Turn 5:** Collect connection details for the chosen service:
- Database: endpoint, port, database name, credentials location in Secrets Manager
- SaaS: platform name, auth type (OAuth / API key), connector profile
- Streaming: stream/topic ARN, region
- Files: bucket/prefix or SFTP host, format, delivery mechanism
- On-prem: source path, agent location, bandwidth estimate

**Turn 6:** "Do you want a profiling report after data lands?" (recommended)

→ Run pre-deployment validation (connectivity, credentials, networking, IAM)
→ Generate Terraform for Landing + Profiling
→ Deploy

**Not asked** (out of scope): transforms, masking, quality rules, schema enforcement, data contracts, evolution policies, approval workflows.

---

## Existing Resource Scan (Before Generating Terraform)

Before creating ANY resource, scan the account for reusable infrastructure:

```bash
# DMS replication instances
aws dms describe-replication-instances \
  --query "ReplicationInstances[].{Id:ReplicationInstanceIdentifier,Class:ReplicationInstanceClass,Status:ReplicationInstanceStatus,Tasks:length(ReplicationTasks||'[]')}"

# S3 buckets with "landing" or "data" in name
aws s3api list-buckets \
  --query "Buckets[?contains(Name,'landing') || contains(Name,'data')].Name"

# AppFlow connector profiles
aws appflow describe-connector-profiles \
  --query "ConnectorProfileDetailList[].{Name:ConnectorProfileName,Type:ConnectorType}"

# Transfer Family servers
aws transfer list-servers \
  --query "Servers[].{Id:ServerId,State:State,Endpoint:EndpointType}"

# DataSync agents
aws datasync list-agents \
  --query "Agents[].{Arn:AgentArn,Name:Name,Status:Status}"

# Existing DMS tasks (check for duplicate source)
aws dms describe-replication-tasks \
  --query "ReplicationTasks[].{Id:ReplicationTaskIdentifier,Source:SourceEndpointArn,Status:Status}"

# Firehose delivery streams
aws firehose list-delivery-streams --query "DeliveryStreamNames"
```

**Present findings to user:**

```
Existing Resources Found
════════════════════════

S3 Buckets:
  ✅ "data-landing-prod" exists — use this? Or create new?

DMS Instances:
  ✅ "dms-prod-01" (t3.medium, AVAILABLE, 2 tasks running)
     → Capacity OK for +1 task. Reuse? (saves ~$75/month)
  ❌ No existing task replicates "aurora-orders" — safe to create

AppFlow Profiles:
  ✅ "salesforce-prod" exists (Salesforce connector)
     → (not relevant for database source)

Transfer Family: None found
DataBrew: No existing dataset for this source

Recommendation:
  • Reuse bucket: data-landing-prod
  • Reuse DMS instance: dms-prod-01
  • Create new: DMS task, DMS endpoints, CloudWatch alarms, DataBrew job

Confirm? Or adjust?
```

**Rules:**
- MUST scan before generating Terraform
- MUST offer to reuse existing DMS instances (saves significant cost)
- MUST offer to reuse existing S3 buckets (landing data should be centralized)
- MUST check for duplicate DMS tasks targeting the same source
- MUST warn if source is already being replicated
- If no existing resources → inform user, proceed with full creation

---

## Naming Convention

Ask once in Turn 4. Apply to ALL generated resources consistently.

**Default pattern (if user has no preference):**
```
{env}-{source_name}-{resource_type}
```

Examples with default:
- DMS instance: `prod-dms-shared-01` (shared, reused)
- DMS task: `prod-aurora-orders-cdc-task`
- DMS source endpoint: `prod-aurora-orders-source`
- DMS S3 target endpoint: `prod-aurora-orders-s3-target`
- S3 prefix: `sales/aurora-orders/orders/`
- CloudWatch alarm: `prod-aurora-orders-pipeline-failure`
- DataBrew job: `prod-aurora-orders-profile`
- IAM role: `prod-aurora-orders-dms-role`

**Custom pattern examples:**
- `{company}-{env}-{team}-{source}-{purpose}` → `acme-prod-data-aurora-orders-cdc`
- `{project}/{env}/{source}` → `data-platform/prod/aurora-orders`

**Tags applied to ALL resources:**

```hcl
locals {
  common_tags = {
    Environment = var.environment
    Source      = var.source_name
    Team        = var.team           # from user input
    CostCenter  = var.cost_center    # from user input
    ManagedBy   = "terraform"
    CreatedBy   = "kiro-power-self-serve-data-onboarding"
  }
}
```

---

## Source Tool Selection

**Do not default to Glue JDBC.** Use the simplest, cheapest service per source type.

| Source | Sync Mode | Recommended Service | Why |
|---|---|---|---|
| Aurora/RDS/MySQL/PostgreSQL/Oracle/SQL Server | CDC (ongoing) | **AWS DMS** | Purpose-built for database replication |
| Aurora/RDS (any) | Full load (periodic) | **AWS DMS** (full-load mode) | Same service, different mode |
| Aurora → Redshift specifically | Continuous | **Aurora Zero-ETL** | Native, zero cost for the integration |
| Aurora (snapshot export) | One-time/periodic | **Aurora S3 Export** | Cheapest (~$0.01/GB) |
| Salesforce, SAP, HubSpot, ServiceNow, Zendesk, Workday + 45 more | Any | **Amazon AppFlow** | 50+ native connectors, no code |
| SaaS not in AppFlow | Any | Custom Lambda or Glue REST | Last resort for unsupported platforms |
| Kinesis Data Streams / MSK Kafka | Real-time | **Kinesis Data Firehose** | Zero code, JSON→Parquet native |
| SFTP/FTP partner file drops | Batch | **AWS Transfer Family** | Managed SFTP server → S3 |
| Already in S3 | N/A | **EventBridge** (detect arrival) | Just trigger profiling |
| On-prem NFS/SMB/HDFS (good bandwidth) | Any | **AWS DataSync** | Incremental sync |
| On-prem (>10 TB or constrained bandwidth) | One-time | **AWS Snow Family** | Physical transfer |
| Cross-account Glue tables | N/A | **Lake Formation sharing** | No data movement |
| Third-party datasets | N/A | **AWS Data Exchange** | Subscription model |

**Selection logic:**
1. Determine source type
2. Determine sync mode (one-time / batch-recurring / CDC / streaming)
3. Pick the service that handles it natively with the least code
4. Only use Glue for ingestion if NO simpler service exists

---

## Pre-Deployment Validation (Mandatory)

Before generating or deploying ANY infrastructure, ALL checks must pass:

### Connectivity
- Database: `aws dms test-connection` or `aws glue test-connection`
- S3 source: `aws s3 ls s3://{bucket}/{prefix}/ --max-items 1`
- SaaS: validate connector profile / OAuth token
- **If fails → STOP. Fix before proceeding.**

### Credentials
- Verify Secrets Manager secret exists with expected keys
- Cross-reference secret's `host` field with declared source endpoint
- WARN if secret last updated > 90 days ago
- WARN if secret host ≠ target endpoint

### Networking (when Glue/DMS connections need VPC)
- Security group MUST have self-referencing ingress rule (all TCP to itself)
- Subnet MUST have route to source (NAT/peering/Direct Connect)
- Auto-fix: add self-referencing SG rule if IAM permits

### IAM
- Enumerate all API calls in state machine → verify execution role has each permission
- Verify ingestion service role can access S3, Secrets Manager, CloudWatch
- Auto-add missing permissions with user confirmation

### State Machine (if Step Functions used)
- `aws stepfunctions validate-state-machine-definition`
- No `\n` characters in `States.Format` strings (causes ASL validation failure)
- All state transitions reference existing states

### Worker Type (if Glue used)
- G.025X is ONLY valid for `gluestreaming` — NEVER for `glueetl`
- Auto-correct to G.1X if G.025X specified for glueetl

### Timeout Calculation
- Never use static timeouts
- Formula: `10 (provisioning) + estimated_execution_time + 20% safety margin`
- First full load: multiply by 3
- Glue Flex: add 15 minutes for queue time

---

## Data Profiling

After data lands in S3, run a profiling job and publish a report.

### Profiling Strategy: First Source vs Subsequent Sources

| Context | Approach | Why |
|---|---|---|
| **First source onboarded** (or first time for this source) | **AWS Glue DataBrew** | Richest output, zero code, visual console for stakeholders, comprehensive stats |
| **Subsequent sources** (user already has profiling running) | **Ask the user** — offer cheaper alternatives | At scale, DataBrew cost adds up |

**First-time behavior (default):** Use DataBrew. Don't ask — just do it.

**When user onboards their 2nd+ source, ask:**

> "You already have DataBrew profiling running for {previous_source}. For recurring
> profiling across multiple sources, there are cheaper options:
>
> 1. **Keep DataBrew** (~$2-4/run) — richest output, visual console
> 2. **Athena SQL** (~$0.03/run) — 100× cheaper, runs in seconds, custom SQL
> 3. **Amazon Macie** (~$1/GB) — best PII detection accuracy, compliance-focused
> 4. **Glue Crawler** (free) — schema detection only, no statistics
>
> Which approach for this source?"

### Profiling Service Details

**Option 1: AWS Glue DataBrew (default for first source)**
- Zero code, auto-generates comprehensive statistics
- Outputs JSON report to S3 + visual in DataBrew console
- Schedulable after each ingestion
- Cost: ~$1.00/node per 30-min session

**Option 2: Amazon Athena SQL (cheapest for recurring)**
- $5/TB scanned — profiling 5 GB costs ~$0.025
- Custom SQL gives full control over what's profiled
- Runs in seconds, results via query output
- PII detection: custom regex in SQL (you write the patterns)

**Option 3: Amazon Macie (best for PII/compliance)**
- Purpose-built for sensitive data detection in S3
- Best accuracy for PII/PHI classification
- Cost: ~$1/GB scanned
- No column statistics — PII-focused only

**Option 4: Glue Crawler (free, schema only)**
- Detects column names and types only
- No statistics, no PII detection, no cardinality
- Good enough when you just need schema registered in Glue Catalog

### Profiling Report Contents

| Section | What It Includes |
|---|---|
| **Schema** | Column names, inferred data types, nullable percentage |
| **PII Candidates** | Columns flagged by name patterns + value regex (email, phone, SSN, address, financial) |
| **Statistics** | Min, max, mean, stddev, p25/p50/p75/p95 per numeric column |
| **Cardinality** | Unique count / total count per column |
| **Top-N Values** | Most frequent values for low-cardinality columns |
| **Nulls** | Null count and null percentage per column |
| **Samples** | 5 representative non-null values per column |
| **Volume** | Total row count, total file size, number of files |
| **Freshness** | Most recent timestamp value found in the data |
| **Recommendations** | Suggested partition columns, primary/merge keys, quality observations |

### PII Detection Patterns

| Category | Column Name Pattern | Value Pattern |
|---|---|---|
| Email | `/email\|e_mail/` | `/[\w.+-]+@[\w-]+\.[\w.-]+/` |
| Phone | `/phone\|mobile\|cell/` | `/(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/` |
| Name | `/first_name\|last_name\|full_name/` | — |
| Address | `/address\|street\|city\|zip\|postal/` | — |
| SSN | `/ssn\|social_security/` | `/\d{3}-\d{2}-\d{4}/` |
| Financial | `/card_number\|credit_card\|cvv\|pan/` | Luhn validation on numeric strings |

### Quality Observations (informational only — NOT enforced)

The report notes patterns like:
- "column X is never null in this dataset"
- "column Y has 15 distinct values (possible enum)"
- "column Z is always positive"
- "column W appears to be a monotonically increasing ID"
- "column V has 99.8% unique values (potential primary key)"

These are suggestions for downstream teams, not enforcement rules.

### Report Output Formats
- **JSON** — machine-readable, stored in `s3://{bucket}/reports/{source}/`
- **Markdown** — human-readable, stored in pipeline workspace `reports/` directory
- **DataBrew Console** — visual, interactive (accessible via AWS Console)

---

## Landing Layer Rules

| Rule | Rationale |
|---|---|
| **Data arrives as-is** | No transforms, no type casting, no column renaming |
| **No audit columns** | No `_ingested_at`, `_batch_id`, `_row_hash` — just source data |
| **Append-only** | Never overwrite or mutate landed files |
| **Partitioned by arrival date** | `year=YYYY/month=MM/day=DD/` enables time-bounded access |
| **Format: service output** | Parquet if the service supports it, otherwise source format (CSV, JSON) |
| **Configurable retention** | Default 90 days; override per project/compliance requirements |
| **Not queried by analysts** | Landing is a staging area; profiling report is the consumer interface |

### S3 Path Convention

```
s3://{landing-bucket}/{domain}/{source_name}/{table_or_object}/year=YYYY/month=MM/day=DD/{files}
```

Examples:
- `s3://data-landing-prod/sales/aurora-orders/orders/year=2025/month=07/day=21/`
- `s3://data-landing-prod/marketing/salesforce/contacts/year=2025/month=07/day=21/`
- `s3://data-landing-prod/vendor/sftp-invoices/invoices/year=2025/month=07/day=21/`

---

## Project Structure (Generated Per Source)

```
{source-name}-landing-pipeline/
├── config/
│   └── source-config.yaml          # Source details, schedule, service choice
├── terraform/
│   ├── main.tf                     # Landing infra (DMS/AppFlow/Firehose/etc. + S3)
│   ├── profiling.tf                # DataBrew dataset + profile job
│   ├── variables.tf                # Input variables
│   ├── outputs.tf                  # Output values (S3 paths, ARNs)
│   ├── versions.tf                 # required_version + required_providers
│   ├── backend.tf                  # S3 + DynamoDB state locking
│   └── environments/
│       ├── dev.tfvars
│       └── prod.tfvars
├── reports/
│   └── (profiling reports land here after first run)
└── README.md
```

**Terraform only.** No CDK. No CloudFormation. One tool, consistent structure.

**Terraform version:** Ask user for pinned version or use latest stable.

---

## Cost-Aware Service Recommendation

Before deploying, show the user estimated monthly cost for the recommended service:

```
For your workload (Aurora MySQL, 5 tables, ~2 GB/day, CDC):

  Recommended: AWS DMS (CDC mode)
  Cost: ~$45-75/month (t3.medium replication instance + S3 storage)

  Alternative: Glue JDBC (direct read)
  Cost: ~$180/month (5 DPU × 15 min × 30 days)

  Cheapest: Aurora S3 Export (snapshots only, no CDC)
  Cost: ~$8/month

  Which approach fits?
```

---

## Error Taxonomy (Bug Fixes from POC)

Known failure patterns with auto-remediation:

| Error | Root Cause | Fix |
|---|---|---|
| `G.025X is only supported for gluestreaming` | Wrong worker type for glueetl | Auto-correct to G.1X |
| `States.Format` validation failure | `\n` characters in format strings | Remove newlines, use spaces |
| `not authorized to perform: glue:GetConnection` | Missing IAM permission | Auto-add to execution role |
| SG `all ingress ports` required | Missing self-referencing SG rule | Auto-add self-referencing rule |
| `Access denied for user` | Stale/wrong Secrets Manager secret | Cross-check host, prompt to verify |
| Timeout on first full load | Static timeout too short | Dynamic: provisioning + estimate × 3 |
| Files generated outside workspace | No workspace detection | Always write inside IDE workspace root |

---

## Available Steering Files

| Steering File | Use When |
|---|---|
| **onboarding-workflow** | End-to-end flow: discover → validate → deploy landing → profile |
| **source-tool-selection** | Deciding which AWS service to use per source type |
| **landing-layer** | S3 path conventions, retention, partitioning rules |
| **pre-deployment-validation** | Connectivity, credentials, networking, IAM, ASL, worker type checks |
| **data-profiling** | DataBrew profiling job setup, report spec, PII patterns |
| **connection-setup** | Glue/DMS connection creation and testing |
| **observability** | Pipeline success/failure alerting, freshness SLA monitoring |
| **troubleshooting** | Error taxonomy, known failure patterns, auto-remediation |
| **security-and-access** | IAM roles, Secrets Manager, networking, least privilege |

---

## Available Skills

| Skill | Use When |
|---|---|
| **connecting-to-data-source** | Setting up Glue/DMS connections, testing connectivity |
| **provisioning-landing-pipeline** | Starting the ingestion service (DMS task, AppFlow flow, Firehose stream), verifying first data lands in S3 |
| **deploying-terraform** | Running terraform init/plan/apply/destroy, post-deploy verification, environment promotion |
| **data-profiling** | Running profiling jobs, generating reports, detecting PII candidates |
| **exploring-data-catalog** | Auditing what exists in Glue Data Catalog |
| **finding-data-lake-assets** | Resolving table/dataset references by business name |
| **monitoring-pipeline-health** | Checking: did data arrive? Is it fresh? Did the job succeed? |
| **cost-estimation-and-optimization** | Service cost comparison, right-sizing, optimization |
| **decommissioning-data-source** | Retiring a source: disable schedule, destroy infra, notify |

---

## Agent Behavior

- **First turn:** ~80-120 words. Ask Q1 (source type) only.
- **Be opinionated:** Recommend one service and explain why in one sentence.
- **Show cost before commit:** Always present estimated cost before deploying.
- **Validate before generating:** Run pre-deployment checks. If any fail, stop and fix.
- **Workspace-aware:** Detect workspace root from IDE context. Never write files elsewhere.
- **Terraform only:** Generate `.tf` files. No CDK, no CloudFormation.
- **Don't over-ask:** 5 turns max before generating infrastructure.
- **Profiling is informational:** The report suggests, it does not enforce.

---

## Key AWS References

| Document | What It Covers |
|---|---|
| [AWS DMS User Guide](https://docs.aws.amazon.com/dms/latest/userguide/) | Database replication, CDC, S3 target |
| [Amazon AppFlow](https://docs.aws.amazon.com/appflow/latest/userguide/) | SaaS connectors, flow configuration |
| [Kinesis Data Firehose](https://docs.aws.amazon.com/firehose/latest/dev/) | Streaming to S3, JSON→Parquet |
| [AWS Transfer Family](https://docs.aws.amazon.com/transfer/latest/userguide/) | Managed SFTP → S3 |
| [AWS DataSync](https://docs.aws.amazon.com/datasync/latest/userguide/) | On-prem file sync to S3 |
| [AWS Glue DataBrew](https://docs.aws.amazon.com/databrew/latest/dg/) | Data profiling, zero code |
| [Aurora S3 Export](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/export-cluster-data.html) | Native snapshot export |
| [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs) | All AWS resource definitions |

---

## Support & Legal

- **License**: Apache-2.0 (see [LICENSE](LICENSE))
- **Authors**: Rahul Agarwal, Manish Choudhary
- **Issues**: Report via GitHub Issues

**Disclaimer:** This power provides technical guidance for landing data on AWS. It does not provide legal, regulatory, or compliance advice. PII detection in profiling reports is pattern-based and should be validated by qualified personnel.
