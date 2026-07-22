---
name: "self-serve-data-onboarding"
displayName: "Self-Serve Data Onboarding"
description: "Build and operate self-serve data ingestion pipelines on AWS. Onboard sources (databases via DMS, SaaS via AppFlow, streaming via Firehose, file drops via Transfer Family, on-prem via DataSync) using a two-layer Landing+Raw architecture, Glue Data Quality (DQDL), and your IaC tool of choice."
keywords: ["data ingestion", "aws glue", "aws dms", "appflow", "kinesis firehose", "transfer family", "datasync", "aurora zero-etl", "glue data quality", "dqdl", "etl", "data pipeline", "iceberg", "s3", "jdbc", "cdk", "terraform", "data contract", "schema inference", "self-serve", "data lake", "landing zone", "bronze"]
well_architected: true
author: "Rahul Agarwal, Manish Choudhary"
---

# Self-Serve Data Onboarding Platform

## Overview

> **✅ AWS Well-Architected Compliant** — This power enforces all 6 pillars (52 rules) via `steering/well-architected.md`. Every generated pipeline passes a Well-Architected review by default.

This power is the **complete, production-grade platform** for building and operating self-serve data ingestion pipelines on AWS. It merges implementation-ready code generation with comprehensive architecture, governance, and operational knowledge into a single unified experience.

**What it delivers:**

1. Accept a data-source onboarding request from any team member (analyst, data scientist, product owner).
2. Recommend the right AWS ingestion service per source type — DMS for databases, AppFlow for SaaS, Firehose for streaming, Transfer Family for file drops. Only use Glue JDBC when transforms are genuinely needed.
3. Implement a two-layer architecture: Landing (fast, cheap, raw copy) → Raw/Bronze (trusted, governed, Iceberg).
4. Run mandatory pre-deployment validation — connectivity, credentials, networking, IAM, and ASL checks — before generating or deploying anything.
5. Compile data contract quality rules to DQDL (AWS Glue Data Quality) — no custom validator code to maintain.
6. Generate IaC (CDK, Terraform, or CloudFormation) with intelligent worker sizing and cost estimates shown upfront.
7. Deploy with observability, error taxonomy with auto-remediation, and self-healing built in.

**The goal:** Reduce source onboarding from 2-4 weeks to under 1 hour while maintaining production-grade quality, security, and observability.

---

## Onboarding

**What this power does:** Helps you build and operate self-serve data ingestion pipelines on AWS — from source discovery through production deployment and ongoing operations. It selects the right AWS ingestion service per source type, implements a Landing+Raw two-layer architecture, enforces quality via DQDL rules, and runs mandatory pre-deployment validation before generating or deploying anything.

**What this power does NOT do:** Provide legal advice on data privacy regulations, replace a qualified Data Protection Officer, or guarantee regulatory compliance. PII/PHI classification is heuristic-based — validate with your privacy team. Silver/Gold layer transforms (masking, joins, business logic) are out of scope — this power handles Landing and Raw only.

**Scope:** AWS-native data lake architectures. Does not cover ML/AI model training pipelines, Silver/Gold layer transforms, or non-AWS platforms.

---

## Getting Started

Describe what you need. No jargon required — the power will ask clarifying questions and guide you through the appropriate workflow.

Examples:
- "Onboard our Aurora MySQL orders database into the data lake"
- "Set up daily Salesforce contacts sync"
- "We get vendor CSV files via SFTP — automate the ingestion"
- "Stream Kafka events into S3"
- "Migrate our on-premises Oracle tables to the data lake"
- "Check if our pipelines are healthy"
- "What would it cost to replicate 5 Aurora tables with CDC?"
- "The orders pipeline has schema drift — help me handle it"
- "Retire the legacy_reports source"

---

## Example Journeys

**"Onboard our Aurora MySQL orders database"**

The agent asks: Q1 (database → Aurora MySQL) → Q2 (CDC or snapshots?) → Q3 (transforms needed at ingestion?). Shows a cost comparison: DMS CDC ~$45/mo vs Glue JDBC ~$180/mo vs Aurora S3 Export ~$8/mo (no CDC). User picks DMS. Agent runs pre-deployment validation (VPC, SG self-reference, IAM), provisions DMS replication instance + S3 target (Landing layer), then creates a lightweight Glue job to promote Landing → Raw/Bronze Iceberg with DQDL quality rules. Deploys CDK stack, runs smoke test, promotes to prod.

**"Set up daily Salesforce contacts sync"**

Agent identifies: SaaS → AppFlow has a native Salesforce connector. No Glue connection needed, no JDBC, no VPC complexity. Provisions AppFlow flow (Salesforce → S3 Landing, Parquet, daily schedule), Glue job promotes Landing → Raw Iceberg with sensitivity tagging. Total cost: ~$15/month vs ~$200/month for custom REST+Glue.

**"We get vendor CSV files daily via SFTP"**

Agent identifies: file drops + SFTP → Transfer Family. Provisions SFTP server with S3 storage backend, EventBridge rule on S3 PutObject triggers Glue job. Schema inferred from first file drop. DQDL rules generated from contract. Under 45 minutes end-to-end.

**"Stream Kinesis events into the data lake"**

Agent identifies: streaming + no transforms → Kinesis Data Firehose. Zero Glue code for Landing. Firehose converts JSON→Parquet natively and delivers to S3 Landing partitioned by arrival time. Scheduled Glue job promotes Landing → Raw Iceberg with DQDL quality rules. Cost: ~$8/month vs ~$180/month for Glue Streaming.

---

## Agent Behavior Guidelines

### Response Style

- Be direct and opinionated. Recommend one approach and explain why for this user's context.
- Acknowledge trade-offs honestly. Flag data quality or security concerns proactively.
- Use the user's source type and domain to make examples concrete.
- Keep first-turn responses short (~80-120 words). A brief framing + first 1-2 discovery questions is enough.
- Always propose the next step. Never end a response without suggesting what to do next.
- Don't expose internal plumbing. Users don't need to know which steering files or skills are loading.
- Don't preview hypothetical scenarios. Ask the question, wait for the answer.

### Discovery Before Action — 7 Questions Before Recommending Architecture

Ask 1-2 questions per turn. Wait for the answer. Each answer shapes what comes next.

**Q1 — Source type (ask first, always):**
- What kind of source? (database / SaaS platform / file drops / streaming / on-premises)
- What specific system? (Aurora MySQL, Salesforce, vendor SFTP files, Kafka on MSK…)

**Q2 — Sync pattern:**
- Do you need ongoing sync (CDC — capture every change) or periodic snapshots (e.g., nightly full extract)?

**Q3 — Transform requirement (determines architecture):**
- Do you need transforms, quality validation, or masking AT ingestion time?
- → No = use the simplest ingestion service, Landing layer only
- → Yes = Landing + Raw two-layer pattern (ingestion service → S3 → Glue from S3)

**Q4 — Freshness SLA:**
- How fresh does the data need to be? (real-time / sub-minute / hourly / daily / weekly)

**Q5 — Data volume:**
- Approximately how much data per batch or per day? (< 1 GB / 1-10 GB / 10-100 GB / > 100 GB)

**Q6 — Migration vs recurring:**
- Is this a one-time migration or a recurring pipeline?

**Q7 — Sensitivity:**
- Does this data contain PII (names, emails, SSNs) or PHI (health/medical data)?
- Any compliance requirements? (GDPR, HIPAA, SOC 2, internal governance)

**After Q1-Q7, BEFORE collecting technical details:**
Show a cost comparison table for applicable services and ask the user to confirm. Only then collect connection details for the chosen service.

**Trigger conditions — signals that shape questions and recommendations:**
- "CDC", "replication", "changes only" → recommend DMS
- "Salesforce", "SAP", "ServiceNow", "HubSpot", "Zendesk", "Workday" → recommend AppFlow
- "Kinesis", "Kafka", "MSK", "events", "streaming" → recommend Firehose (no transforms) or Glue Streaming
- "SFTP", "FTP", "file drop", "partner files" → recommend Transfer Family
- "on-prem", "NFS", "SMB", "HDFS", "file share" → recommend DataSync
- "mainframe", "VSAM", "DB2", "IMS" → recommend Mainframe Modernization
- "share data", "cross-account", "no movement" → recommend Lake Formation sharing
- "Aurora" + "Redshift" → ask about Aurora Zero-ETL integration
- "Aurora" + "snapshot" → ask about Aurora native S3 Export
- "cost", "budget", "expensive" → show cost comparison before any recommendation
- "PII", "customer data", "emails", "SSN" → ask about masking strategy and approval
- "Terraform" → ask version pinning, state backend, module structure

**Workspace-aware file generation:**
Before writing ANY file: detect workspace root from IDE context, propose path `{workspace_root}/{source-name}-ingestion-pipeline/`, confirm with user. Never write to home directory.

**Interaction cadence:**
- Turn 1: Q1 only (~80-120 words)
- Turns 2-4: Q2 → Q7, 1-2 per turn
- After Q7: show cost comparison, confirm approach
- After confirmation: collect technical details (endpoint, credentials, VPC)
- After technical details: run pre-deployment validation
- After validation passes: generate infrastructure and deploy

### Infrastructure-as-Code Support

**Primary support (first-class, with templates and patterns):**
- **AWS CDK (Python)** — default recommendation for greenfield projects. Full template library in `steering/cdk-infrastructure.md`.

**Full support (production-ready generation):**
- **Terraform (HCL)** — for teams with existing Terraform estates or organizational mandates. Generates modules with proper state management, variable files, and environment workspaces.
- **AWS CloudFormation (YAML)** — for teams that prefer native AWS tooling without CDK abstraction.

**IaC is the user's choice.** Do not push a specific tool. Ask once, remember for the session.

**Terraform-specific requirements:**
- Always ask for version preference: "Do you use the latest stable Terraform version, or does your organization pin to a specific version?"
- Generate `required_version` constraint in `versions.tf` matching the user's answer
- Generate `required_providers` block with pinned AWS provider version
- Use workspace-based or directory-based environment separation (ask preference)
- Generate proper backend configuration (S3 + DynamoDB state locking as default)
- Follow HashiCorp module structure: `main.tf`, `variables.tf`, `outputs.tf`, `versions.tf`
- Include `.terraform.lock.hcl` guidance for reproducible builds

**Terraform version handling:**

| User says... | Action |
|---|---|
| "Use latest Terraform" | Set `required_version = ">= 1.9.0"` (or current latest stable) |
| "We use Terraform 1.5" | Set `required_version = "~> 1.5.0"` (pessimistic constraint) |
| "We're on 1.3 due to compliance" | Set `required_version = "~> 1.3.0"` and note any feature limitations |
| No preference stated | Ask: "Does your org pin Terraform versions, or can we use latest stable?" |

---

## Target Platform

| Component | Technology |
|-----------|-----------|
| **Landing — Databases** | AWS DMS (CDC + full load), Aurora Zero-ETL, Aurora S3 Export |
| **Landing — SaaS** | Amazon AppFlow (50+ native connectors) |
| **Landing — Streaming** | Kinesis Data Firehose (zero code) or AWS Glue Streaming ETL |
| **Landing — File Drops** | AWS Transfer Family (SFTP/FTP/FTPS) |
| **Landing — On-Premises** | AWS DataSync, AWS Snow Family, Mainframe Modernization |
| **Landing Storage** | Amazon S3 (plain Parquet or source format, 90-day retention) |
| **Raw/Bronze Compute** | AWS Glue PySpark (reads from Landing S3 only — never from source) |
| **Raw/Bronze Storage** | Amazon S3 + Apache Iceberg (ACID, time-travel, schema evolution) |
| **Data Quality** | AWS Glue Data Quality (DQDL rules, compiled from data contracts) |
| **Orchestration** | AWS Step Functions + Amazon EventBridge |
| **Metadata / Registry** | Glue Data Catalog + DynamoDB (source registry) |
| **Governance** | AWS Lake Formation (column-level tagging, cross-account sharing) |
| **Observability** | Amazon CloudWatch + SNS (metrics, alarms, dashboards) |
| **Secrets** | AWS Secrets Manager |
| **Deployment** | AWS CDK (Python), Terraform (HCL), or CloudFormation (YAML) |
| **Query Layer** | Amazon Athena |
| **Data Sharing** | Lake Formation cross-account, AWS Data Exchange, Redshift Data Sharing |

---

## Supported Source Types and Recommended Tools

| Source | CDC? | Transforms? | Recommended Service | Notes |
|---|---|---|---|---|
| Aurora MySQL/PostgreSQL → Redshift | Yes | No | **Aurora Zero-ETL** | Native, sub-minute latency, zero cost |
| Aurora (snapshot to S3) | No | No | **Aurora S3 Export** | ~$8/month, no CDC |
| RDS / Aurora / MySQL / PostgreSQL / Oracle / SQL Server | Yes | Any | **AWS DMS** → S3 Landing → Glue | Purpose-built CDC |
| Salesforce, SAP, ServiceNow, HubSpot, Zendesk, Workday, Marketo + 45 more | Any | No | **Amazon AppFlow** | 50+ native connectors, no code |
| Kinesis / MSK Kafka (no transforms) | Real-time | No | **Kinesis Data Firehose** | JSON→Parquet native, zero code |
| Kinesis / MSK Kafka (transforms needed) | Real-time | Yes | **AWS Glue Streaming ETL** | Full Spark, checkpointing |
| S3 file drops (CSV/JSON/Parquet/ORC/Avro) | N/A | Yes | S3 Event → EventBridge → **Glue** | Already landed |
| SFTP/FTP/FTPS partner file drops | N/A | Any | **AWS Transfer Family** → S3 → Glue | Managed SFTP server |
| On-prem file shares (NFS, SMB, HDFS) | Any | Any | **AWS DataSync** → S3 → Glue | Incremental sync |
| Large one-time migration (>10 TB) | One-time | Any | **AWS Snow Family** + DataSync | Bandwidth-constrained |
| Mainframe (VSAM, DB2, IMS) | Any | Any | **AWS Mainframe Modernization** | Managed replatforming |
| Glue Catalog cross-account sharing | N/A | No | **Lake Formation cross-account** | Zero data movement |
| Third-party licensed datasets | N/A | No | **AWS Data Exchange** | Subscription model |
| Redshift to data lake | Any | No | **Redshift Data Sharing** / Unload | Unload → S3 |
| Snowflake | Any | Yes | Glue SNOWFLAKE connector | Only valid Glue JDBC use case |
| BigQuery | Any | Yes | Glue BIGQUERY connector | Only valid Glue JDBC use case |
| DynamoDB | Any | Any | DynamoDB native S3 Export | No Glue connection needed |

---

## Available Steering Files

This power organizes deep knowledge into on-demand steering files. Load only what is relevant to the current task.

| Steering File | Use When |
|---------------|----------|
| **onboarding-workflow** | Onboarding a new data source end-to-end (the master workflow) |
| **landing-layer** | Understanding the Landing layer design, retention, partitioning, and trigger patterns |
| **source-tool-selection** | Deciding which AWS service to use per source type (DMS, AppFlow, Firehose, etc.) |
| **pre-deployment-validation** | Running connectivity, credential, networking, IAM, and ASL checks before deploy |
| **glue-data-quality** | Compiling data contract rules to DQDL syntax and configuring Glue Data Quality |
| **s3-file-ingestion** | Building S3 file pipelines (CSV, JSON, Parquet) with Glue bookmarks |
| **jdbc-rds-ingestion** | JDBC/RDS pipelines — when transforms are genuinely needed at read time |
| **saas-streaming-ingestion** | AppFlow SaaS flows, Kinesis Firehose, Glue Streaming ETL |
| **pipeline-generation** | The code generator's template model, hooks, and config-to-code patterns |
| **schema-inference** | Schema detection, drift, PII classification, and evolution handling |
| **data-contracts** | Authoring data contracts: schema, quality rules, SLA, evolution policies |
| **data-contract-governance** | Contract lifecycle, versioning rules, enforcement points |
| **cdk-infrastructure** | CDK stack patterns for Glue, Step Functions, DMS, AppFlow, Firehose, Transfer Family |
| **connection-setup** | Glue connections (JDBC, Snowflake, BigQuery) — for the cases that still need them |
| **observability** | CloudWatch metrics, alarms, dashboards, structured logging |
| **security-and-access** | RBAC, credential management, approval matrices, Lake Formation tagging |
| **ingestion-standards** | Non-negotiable engineering rules (DQDL, audit columns, two-layer model) |
| **troubleshooting** | Error taxonomy, auto-remediation runbooks, self-healing patterns |
| **well-architected** | **ALWAYS** — 52 rules across 6 AWS Well-Architected pillars enforced on every generation |

**Usage:** Call `readSteering` with the steering file name to load detailed guidance for that topic.

---

## Available Skills

Skills are executable workflows that handle specific operational tasks interactively. They are integrated into the onboarding workflow at their corresponding steps.

### AWS Data Analytics Skills (adapted from [AWS Agent Toolkit](https://github.com/aws/agent-toolkit-for-aws))

| Skill | Onboarding Step | Use When | Location |
|-------|----------------|----------|----------|
| **connecting-to-data-source** | Steps 4-5 | Setting up or troubleshooting Glue connections to JDBC databases, Redshift, Snowflake, or BigQuery. Discovers existing connections and RDS/Redshift candidates, registers credentials, configures VPC, and tests. | `skills/connecting-to-data-source/SKILL.md` |
| **ingesting-into-data-lake** | Steps 11-12 | Importing data from S3, JDBC, Snowflake, BigQuery, DynamoDB, or migrating existing Glue tables. Handles one-time loads, recurring pipelines, and format migrations to Iceberg. | `skills/ingesting-into-data-lake/SKILL.md` |
| **creating-data-lake-table** | Step 11 | Creating managed Iceberg tables using S3 Tables with automatic compaction and snapshot management. Sets up table bucket, namespace, schema, partitioning, and IAM access. | `skills/creating-data-lake-table/SKILL.md` |
| **querying-data-lake** | Steps 12, 14 | Executing Athena SQL queries across Glue, S3 Tables, and Redshift catalogs. Used for smoke tests, integration validation, and data profiling. | `skills/querying-data-lake/SKILL.md` |
| **finding-data-lake-assets** | Steps 2, 4, 11 | Resolving table references by business name, keyword, column, or S3 path. Acts as a resolver for other skills when users provide fuzzy or business-language references. | `skills/finding-data-lake-assets/SKILL.md` |
| **exploring-data-catalog** | Pre-onboarding | Full inventory and audit of Glue Data Catalog assets across S3 Tables, Redshift-federated, and remote Iceberg catalogs. Maps the data landscape before onboarding. | `skills/exploring-data-catalog/SKILL.md` |

### Platform Custom Skills (original — authored by Rahul Agarwal, Manish Choudhary)

| Skill | Onboarding Step | Use When | Location |
|-------|----------------|----------|----------|
| **schema-inference-and-classification** | Steps 6-7 | Auto-detecting schema structure, data types, nullability, and PII/PHI sensitivity classification from source samples. Produces versioned schema documents for contract authoring. | `skills/schema-inference-and-classification/SKILL.md` |
| **data-contract-authoring** | Steps 8-9 | Generating, validating, and managing data-contract.yaml files. Produces quality rules, SLA targets, evolution policies, and routes contracts for approval. | `skills/data-contract-authoring/SKILL.md` |
| **deploying-cdk-pipeline** | Steps 13-15 | Deploying CDK stacks, running post-deploy verification, executing smoke tests, and promoting through dev → test → prod with gate checks and rollback capability. | `skills/deploying-cdk-pipeline/SKILL.md` |
| **monitoring-pipeline-health** | Step 16 + Ongoing | Proactively checking freshness SLAs, quarantine rates, execution trends, alarm states, and cost metrics. Triggers self-healing for known failure patterns. | `skills/monitoring-pipeline-health/SKILL.md` |
| **managing-schema-evolution** | Post-deployment | Detecting schema drift between source and contract, classifying change severity, applying evolution policies for safe changes, and coordinating human review for breaking changes. | `skills/managing-schema-evolution/SKILL.md` |
| **decommissioning-data-source** | End-of-life | Safely retiring a data source: disabling schedules, archiving tables, notifying consumers, revoking access, and cleaning up infrastructure while preserving audit trail. | `skills/decommissioning-data-source/SKILL.md` |
| **cost-estimation-and-optimization** | Pre-deploy + Ongoing | Estimating pipeline costs before deployment, tracking actual spend, detecting cost anomalies, and recommending optimizations (right-sizing, schedule changes, compaction). | `skills/cost-estimation-and-optimization/SKILL.md` |

### Skill Interaction Model

Skills delegate to each other following a clear separation of concerns across the full lifecycle:

```
                              ONBOARDING PHASE
                              ════════════════
User Request
     │
     ▼
[exploring-data-catalog]           ←── "What data exists?" (landscape discovery)
     │
     ▼
[finding-data-lake-assets]         ←── "Where is table X?" (asset resolution)
     │
     ▼
[connecting-to-data-source]        ←── "Connect to the source" (connection + test)
     │
     ▼
[schema-inference-and-classification] ←── "What does the data look like?" (detect + classify)
     │
     ▼
[data-contract-authoring]          ←── "Define the agreement" (contract + approval)
     │
     ▼
[creating-data-lake-table]         ←── "Create target table" (Iceberg table)
     │
     ▼
[ingesting-into-data-lake]         ←── "Move data" (ETL execution)
     │
     ▼
[deploying-cdk-pipeline]           ←── "Deploy infrastructure" (CDK + promote)
     │
     ▼
[querying-data-lake]               ←── "Validate results" (Athena queries)

                              OPERATIONAL PHASE
                              ═════════════════

[monitoring-pipeline-health]       ←── "Are pipelines healthy?" (proactive checks)
     │
     ▼
[managing-schema-evolution]        ←── "Schema changed" (drift detection + evolution)
     │
     ▼
[cost-estimation-and-optimization] ←── "Optimize spend" (right-sizing + savings)

                              END-OF-LIFE PHASE
                              ═════════════════

[decommissioning-data-source]      ←── "Retire this source" (controlled shutdown)
```

### Skill Trigger Phrases

| User says... | Skill activated |
|---|---|
| "connect to database", "set up Glue connection", "test connection" | connecting-to-data-source |
| "import data", "load data", "ingest", "set up pipeline", "ETL" | ingesting-into-data-lake |
| "create table", "data lake table", "S3 Tables", "Iceberg table" | creating-data-lake-table |
| "query data", "run SQL", "athena query", "profile table" | querying-data-lake |
| "find the table", "where is our data", "which table has" | finding-data-lake-assets |
| "inventory the catalog", "list all tables", "data landscape" | exploring-data-catalog |
| "infer schema", "detect columns", "classify data", "find PII" | schema-inference-and-classification |
| "create contract", "generate contract", "quality rules", "SLA" | data-contract-authoring |
| "deploy pipeline", "cdk deploy", "promote to prod" | deploying-cdk-pipeline |
| "check pipeline health", "are pipelines healthy", "SLA status" | monitoring-pipeline-health |
| "schema changed", "new column", "type mismatch", "schema drift" | managing-schema-evolution |
| "decommission source", "retire pipeline", "archive table" | decommissioning-data-source |
| "how much will this cost", "optimize costs", "reduce spend" | cost-estimation-and-optimization |

---

## Governing Principles (Non-Negotiable)

These principles apply to ALL work performed under this power:

1. **Right tool per source** — never default to Glue JDBC; use DMS for databases, AppFlow for SaaS, Firehose for streaming. Glue reads from S3 only.
2. **Two layers: Landing + Raw** — Landing gets data to S3 fast and cheap; Raw makes it trustworthy and governed.
3. **Validate before generating** — pre-deployment validation (connectivity, credentials, SG, IAM, ASL) must pass before any code is generated or deployed.
4. **DQDL over custom code** — data contract quality rules compile to Glue Data Quality DQDL syntax; no hand-rolled validator classes.
5. **Contracts at the boundary** — every source has a versioned data contract defining schema, freshness SLA, volume bounds, and quality rules.
6. **Observe everything** — pipelines emit CloudWatch metrics (rows processed, errors, latency, freshness) from day one.
7. **Fail fast, fail loud** — errors are quarantined, SNS alerts fired, never silently swallowed.
8. **Immutable bronze** — Raw/Bronze is append-only Iceberg; never mutate the landing or raw layer.
9. **Self-service does not mean ungoverned** — every onboarding request passes through automated validation gates and (where needed) human approval.
10. **Schema is a first-class citizen** — inferred, versioned, and evolution-handled automatically.
11. **Never drop data** — quarantine invalid rows with clear error reasons.
12. **Least privilege everywhere** — IAM scoped to specific resources, Lake Formation for column-level access.

---

## Capability Model

| # | Capability | What It Delivers |
|---|-----------|------------------|
| 1 | **Source Registry** | Centralized inventory of all data sources (DynamoDB) with metadata, status, and lineage |
| 2 | **Tool Selector** | Recommends the right AWS ingestion service per source type with cost comparison — powered by `steering/source-tool-selection.md` |
| 3 | **Pre-Deploy Validator** | Connectivity, credential, networking, IAM, and ASL checks before any code generation — powered by `steering/pre-deployment-validation.md` |
| 4 | **Schema Inference Engine** | Automatic schema detection, typing, sensitive-field classification, drift detection |
| 5 | **Data Contract Engine** | Machine-readable contracts compiled to DQDL rules for Glue Data Quality |
| 6 | **Pipeline Generator** | Config-to-code engine producing Landing service config + Raw Glue job + IaC stacks |
| 7 | **Observability** | CloudWatch metrics, freshness SLAs, error taxonomy with auto-remediation, self-healing |

### How Capabilities Interlock

```
User request
     │
     ▼
Q1-Q7 Discovery + Cost Comparison
     │
     ▼
Pre-Deploy Validation  ──── FAIL → stop, fix, retry
     │ PASS
     ▼
Schema Inference + Data Contract (DQDL rules compiled)
     │
     ▼
┌──────────────────────────────┐
│  LANDING LAYER               │  ← DMS / AppFlow / Firehose / Transfer Family / DataSync
│  Plain S3, source format     │
└──────────────────────────────┘
     │  S3 Event → EventBridge
     ▼
┌──────────────────────────────┐
│  RAW / BRONZE LAYER          │  ← Glue ETL (reads S3 only) + Glue Data Quality (DQDL)
│  Iceberg, audit cols, tagged │
└──────────────────────────────┘
     │
     ▼
IaC Deploy (CDK / Terraform / CloudFormation) + Observability
```

---

## Onboarding a New Data Source

### High-Level Flow

```
1. Q1-Q7 Discovery (ask 1-2 questions per turn)
   ↓
2. Duplicate check in Source Registry (DynamoDB)
   ↓
3. Register source (status: pending)
   ↓
4. Show cost comparison — confirm ingestion service with user
   ↓
5. Pre-deployment validation
   (connectivity / credentials / SG self-reference / IAM / ASL)
   ↓
6. Infer schema + classify PII/PHI sensitivity
   ↓
7. Risk assessment (auto-approve if no PII; Data Steward review if sensitive)
   ↓
8. Author data-contract.yaml → compile DQDL quality rules
   ↓
9. Provision Landing layer (DMS / AppFlow / Firehose / Transfer Family / DataSync)
   ↓
10. Generate Glue ETL job (reads from Landing S3) + IaC stack
    ↓
11. Deploy to dev (CDK / Terraform / CloudFormation)
    ↓
12. Run smoke test — validate Landing S3 files + Raw Iceberg table
    ↓
13. Promote dev → test → prod (gate checks at each step)
    ↓
14. Activate observability (CloudWatch metrics, alarms, dashboards)
    ↓
15. Update Source Registry (status: active)
    ↓
16. Notify requester + generate pipeline report
```

### Estimated Timeline

| Risk Level | End-to-End Time |
|------------|----------------|
| Low (no PII, public) | < 1 hour (fully automated) |
| Medium (internal, some sensitive) | 2-4 hours (1 approval) |
| High (PII/PHI, external) | 4-8 hours (multiple approvals) |

---

## Project Structure (Per Pipeline)

```
{source-name}-ingestion-pipeline/   ← always inside workspace root
├── config/
│   ├── onboarding-request.yaml     # Source metadata, owner, sensitivity, SLA
│   ├── data-contract.yaml          # Schema, quality rules (compiled to DQDL), evolution policy
│   └── pipeline-config.yaml        # Ingestion service config + Glue job settings
├── landing/                        # Landing layer configuration
│   ├── dms-task.json               # DMS task definition (databases)
│   ├── appflow-flow.json           # AppFlow flow definition (SaaS)
│   ├── firehose-delivery.json      # Firehose delivery stream config (streaming)
│   └── transfer-family.json        # Transfer Family server + S3 mapping (SFTP)
├── src/
│   └── {source}_{table}_raw.py     # Glue ETL job (Landing → Raw, reads S3 only)
│   └── utils/
│       ├── dqdl_rules.txt          # Compiled DQDL quality rules
│       ├── metrics_emitter.py
│       ├── quarantine_router.py
│       └── watermark_manager.py
├── cdk/                            # If IaC = CDK
├── terraform/                      # If IaC = Terraform
│   ├── main.tf / variables.tf / outputs.tf / versions.tf / backend.tf
│   └── environments/ (dev.tfvars, test.tfvars, prod.tfvars)
├── cloudformation/                 # If IaC = CloudFormation
├── tests/
├── reports/
│   └── generation_report.md
└── README.md
```

---

## Core Patterns

### Two-Layer Architecture

```
LANDING LAYER
  Purpose  : Get data to S3 as-is, as fast and cheaply as possible
  Storage  : Plain S3 (Parquet or source format) — NO Iceberg
  Content  : Exact copy of source — no transforms, no validation, no masking
  Partition: year/month/day of arrival
  Retention: 90 days (reprocessing buffer)
  Trigger  : S3 Event Notification → EventBridge → Glue Raw job

RAW (BRONZE) LAYER
  Purpose  : Make the landed data trustworthy and governable
  Storage  : S3 + Apache Iceberg (ACID, time-travel, schema evolution)
  Content  : Landing data + audit columns + DQDL quality validation + dedup
  PII      : Classified and tagged via Lake Formation (NO masking — Silver's job)
  Retention: Per data contract (default 7 years)
```

### Data Quality: DQDL (Glue Data Quality)

Data contract quality rules compile to DQDL syntax — no custom Python validator:

```
Rules = [
    Completeness "order_id" >= 0.99,
    IsUnique "order_id",
    ColumnValues "total_amount" >= 0,
    ColumnValues "status" in ["PENDING","CONFIRMED","SHIPPED","CANCELLED"],
    CustomSql "SELECT COUNT(*) FROM primary WHERE updated_at < created_at" = 0
]
```

### Intelligent Worker Sizing

```
timeout_minutes = 10 (provisioning buffer)
                + (volume_gb / throughput_per_dpu_gb_per_min)
                × 1.2 (safety margin)

For initial full load: timeout × 3
For S3-sourced Glue jobs (Landing → Raw): worker_count × 0.5
  (S3 reads are ~2× faster than JDBC reads)
```

| Volume per Batch | Worker Type | Workers | Timeout | Notes |
|---|---|---|---|---|
| < 1 GB | G.1X | 2 | 15 min | |
| 1–10 GB | G.1X | 5 | 30 min | |
| 10–50 GB | G.1X | 10 | 60 min | |
| > 50 GB | G.2X | 10 | 120 min | |
| SLA > 2 hours | Any | Any | Any | Use Flex execution class (35% cost saving) |

### Quarantine Pattern

Invalid rows are **never dropped silently**. They route to `{table}_quarantine` with `_error_reason`, `_quarantined_at`, `_batch_id`. If quarantine rate exceeds 5%, pipeline halts and fires alarm.

### Audit Columns (Raw Layer — every row)

| Column | Purpose |
|--------|---------|
| `_ingested_at` | UTC timestamp of ingestion into Raw |
| `_source_file` | Landing S3 path that was processed |
| `_batch_id` | UUID correlating all rows in one pipeline run |
| `_pipeline_version` | Semantic version of the ETL code |
| `_row_hash` | SHA-256 hash for deduplication |
| `_ingested_date` | Date partition key (derived from `_ingested_at`) |

### Error Taxonomy with Auto-Remediation

| Error Signal | Auto-Remediation |
|---|---|
| `security group must open all ingress ports` | Auto-add self-referencing SG rule (if IAM permits) |
| `Access denied for user` (MySQL/PostgreSQL) | Check Secrets Manager keys, prompt to verify/rotate |
| `TIMEOUT — Waiting to start the job run` | Increase timeout, suggest Flex class, suggest off-peak schedule |
| `not authorized to perform: glue:GetConnection` | Auto-add permission to SFN execution role |
| `SCHEMA_VALIDATION_FAILED: value must be a valid JSONPath` | Fix `States.Format` expression in ASL |
| `G.025X is only supported for job command gluestreaming` | Auto-correct to G.1X for glueetl jobs |
| `UnableToFindVpcEndpoint` | Create S3 gateway endpoint in connection VPC |
| `ORA-01017: invalid username/password` | Verify secret, check Oracle user lock status |
| `No suitable driver found` | Upload driver JAR to S3, add `--extra-jars` |

---

## Security Model

### Approval Matrix

| Source Sensitivity | Auto-Approve? | Required Approvals |
|-------------------|---------------|-------------------|
| Public / non-sensitive | Yes (auto-deploy) | None — automated validation only |
| Internal / business-sensitive | No | 1x Data Steward |
| PII / PHI / regulated | No | 1x Data Steward + 1x Privacy Officer |
| External third-party | No | 1x Data Steward + 1x Legal + 1x Security |

### Security Layers

- **IAM Least Privilege**: Roles scoped to specific buckets, databases, secrets
- **S3 Encryption**: SSE-S3 for non-PII, SSE-KMS (dedicated CMK) for PII
- **Network**: VPC + Security Groups for JDBC; VPC Endpoints for S3/Glue
- **Credentials**: AWS Secrets Manager only; never in code or config
- **Access Control**: Lake Formation column-level tags for PII/PHI fields
- **Audit**: CloudTrail + DynamoDB event logging; 1-year retention

---

## Maturity Model

| Level | Description |
|-------|-------------|
| 1 Initial | Hand-written pipelines, spreadsheet tracking, manual checks |
| 2 Managed | Shared templates, semi-automated inference, basic alerting |
| 3 Defined | Config-driven generation, full inference, versioned contracts, dashboards |
| **4 Governed (Target)** | AI-assisted generation, real-time drift detection, enforced contracts, self-healing |
| 5 Optimised | Zero-touch for known patterns, predictive failure avoidance, intent-based requests |

---

## Best Practices

- **Always define a data contract before building the pipeline** — schema + quality rules are the foundation
- **Use incremental processing** — Glue job bookmarks for S3, watermark columns for JDBC
- **Never drop data** — quarantine invalid rows with clear error reasons
- **Tag everything** — team, source, environment, cost_center on all AWS resources
- **Encrypt at rest** — SSE-S3 for non-PII, SSE-KMS for PII sources
- **Least privilege IAM** — scope Glue roles to specific buckets, databases, secrets
- **Structured logging** — JSON format with source_id, batch_id for correlation
- **Emit CloudWatch metrics** — rows_read, rows_valid, quarantine_rate, duration
- **Test with sample data** — include representative test files in `sample-data/`
- **Use Iceberg table format** — Parquet + ZSTD compression, partitioned by `_ingested_date`
- **Run smoke tests in dev** before promotion to prod
- **Generate data contracts BEFORE deploying** any pipeline to production
- **Store all credentials** in AWS Secrets Manager; never in code or config
- **Partition bronze Iceberg tables** by `_ingested_date` for cost efficiency and time-travel

---

## Troubleshooting Quick Reference

| Symptom | Category | Steering File / Skill |
|---------|----------|---------------|
| "connection refused" / timeout | Connectivity | `skills/connecting-to-data-source` (troubleshooting.md) |
| "authentication failed" / 403 | Credentials | `skills/connecting-to-data-source` (credential-security.md) |
| Quarantine rate > 5% | Data Quality | troubleshooting |
| rows_read = 0 | Source Empty | troubleshooting |
| Duration > 3x average | Performance | troubleshooting |
| Schema drift detected | Schema | schema-inference |
| Contract validation failing | Contract | data-contract-governance |
| No new data processed | Bookmark/Watermark | troubleshooting, s3-file-ingestion |
| MERGE INTO duplicates | Merge Key | jdbc-rds-ingestion |
| PII access denied | Lake Formation | security-and-access |

---

## Integration with Databricks & Unity Catalog

The bronze Iceberg tables produced by this platform can be registered in Databricks Unity Catalog via:
- **External Locations** pointing to the target S3 bucket
- **Glue Catalog federation** (Databricks reads Glue metastore directly)
- **Lake Formation TBAC** for cross-platform column-level PII security

---

## Architecture Decision Records

Key design decisions underpinning this platform:

1. **Config-driven generation over hand-written code** — eliminates 40% plumbing overhead
2. **Data contracts as first-class citizens** — guarantees schema stability and SLAs
3. **S3 + Iceberg as default storage** — open format, ACID transactions, time-travel, schema evolution
4. **Append-only bronze with Iceberg snapshots** — full auditability and efficient downstream consumption
5. **Self-serve with automated validation gates** — speed without compromising quality
6. **Quarantine over silent discard** — no data loss, clear quality visibility
7. **Observability from day one** — CloudWatch metrics detect issues immediately, not weeks later
8. **Schema inference before contract authoring** — reduce manual effort, catch PII early
9. **Pluggable connectors** — same patterns for JDBC, REST, streaming; only the reader changes

---

## Precedence

Base Kiro instructions and the user's explicit direction take precedence. Within ingestion work, if any instruction conflicts with the steering standards or data-contract governance model, stop and flag. Never bypass validation to satisfy speed.

---

## AWS Well-Architected Framework Compliance

This power enforces **52 rules** across all 6 pillars of the AWS Well-Architected Framework.
Rules are defined in `steering/well-architected.md` (inclusion: always — loaded on every generation).

| Pillar | Rules | Key Enforcement |
|--------|-------|-----------------|
| **Security** | 12 | Secrets Manager, least-privilege IAM, Lake Formation tags, encryption, PII de-identification |
| **Reliability** | 10 | 3 retries + backoff, quarantine, idempotent dedup, pre/post hooks, schema drift detection |
| **Operational Excellence** | 10 | CloudWatch metrics auto-generated, SNS alerts, audit trail, structured logging, CDK-only |
| **Performance Efficiency** | 8 | Watermark incremental reads, right-sized workers, Parquet+ZSTD, partitioning, serverless |
| **Cost Optimization** | 8 | Pay-per-use only, no idle resources, right-sized compute, tagged for allocation |
| **Sustainability** | 4 | Process only new data, compressed columnar storage, serverless/ephemeral compute |

### How It Works

```
Engineer writes YAML config
    │
    ▼
Kiro loads POWER.md + well-architected.md (always) + relevant steering files
    │
    ▼
For EACH generated artifact → validate against 52 rules
    │
    ▼
ALL pass → generate compliant code
ANY fail → flag violation, do NOT generate non-compliant code
```

**Key principle:** Engineers don't memorize the framework. It's encoded in `steering/well-architected.md`.
Kiro reads it and enforces all 52 rules automatically. Every generated pipeline is Well-Architected by default.

---

## Key AWS References

| Document | What It Covers |
|---|---|
| [AWS DMS User Guide](https://docs.aws.amazon.com/dms/latest/userguide/) | Database replication, CDC, S3 target config |
| [Amazon AppFlow User Guide](https://docs.aws.amazon.com/appflow/latest/userguide/) | SaaS connectors, flow setup, Parquet output |
| [Kinesis Data Firehose](https://docs.aws.amazon.com/firehose/latest/dev/) | Zero-code streaming to S3, JSON→Parquet conversion |
| [AWS Transfer Family](https://docs.aws.amazon.com/transfer/latest/userguide/) | SFTP/FTP/FTPS managed server, S3 backend |
| [AWS DataSync](https://docs.aws.amazon.com/datasync/latest/userguide/) | On-premises to S3 incremental file sync |
| [Aurora S3 Export](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/exporttasks.html) | Native Parquet snapshot export to S3 |
| [Aurora Zero-ETL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/zero-etl.html) | Aurora → Redshift native replication |
| [AWS Glue Data Quality](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html) | DQDL rules, data quality results, catalog integration |
| [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/) | ETL jobs, connections, crawlers, Data Catalog |
| [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) | Managed Iceberg tables with auto-compaction |
| [Amazon Athena User Guide](https://docs.aws.amazon.com/athena/latest/ug/) | SQL queries across Glue, S3 Tables, Redshift |
| [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/) | Workflow orchestration, ASL validation |
| [AWS CDK Developer Guide](https://docs.aws.amazon.com/cdk/v2/guide/) | Infrastructure as Code (Python) |
| [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/) | Column-level tagging, cross-account sharing |
| [AWS Well-Architected Framework](https://docs.aws.amazon.com/wellarchitected/latest/framework/) | Six pillars of architecture best practices |
| [Lake Formation Cross-Account](https://docs.aws.amazon.com/lake-formation/latest/dg/cross-account-permissions.html) | Data sharing without copying |
| [AWS Data Exchange](https://docs.aws.amazon.com/data-exchange/latest/userguide/) | Third-party dataset subscriptions |

---

## Support & Legal

- **License**: Apache-2.0 License (see [LICENSE](LICENSE))
- **Authors**: Rahul Agarwal, Manish Choudhary
- **Issues**: Please report bugs and feature requests via GitHub Issues

**Disclaimer:** This power provides technical guidance for building data pipelines on AWS. It does not provide legal, regulatory, or compliance advice. Data classification (PII/PHI) is heuristic-based and should be validated by qualified personnel. Always verify compliance requirements with your organization's legal and privacy teams.
