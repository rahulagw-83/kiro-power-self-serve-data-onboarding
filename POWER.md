---
name: "self-serve-data-onboarding"
displayName: "Self-Serve Data Onboarding"
description: "Build and operate self-serve data ingestion pipelines on AWS. Onboard sources (S3, JDBC, SaaS, streaming), infer schemas, generate data contracts, and auto-generate production-grade Glue ETL jobs with CDK infrastructure, observability, and governance built in."
keywords: ["data ingestion", "aws glue", "etl", "data pipeline", "iceberg", "s3", "jdbc", "cdk", "data contract", "schema inference", "self-serve", "data lake"]
well_architected: true
author: "Rahul Agarwal, Manish Choudhary"
---

# Self-Serve Data Onboarding Platform

## Overview

> **✅ AWS Well-Architected Compliant** — This power enforces all 6 pillars (52 rules) via `steering/well-architected.md`. Every generated pipeline passes a Well-Architected review by default.

This power is the **complete, production-grade platform** for building and operating self-serve data ingestion pipelines on AWS. It merges implementation-ready code generation with comprehensive architecture, governance, and operational knowledge into a single unified experience.

**What it delivers:**

1. Accept a data-source onboarding request from any team member (analyst, data scientist, product owner).
2. Test connectivity and infer schema automatically — detecting types, nullability, and sensitive fields.
3. Generate a machine-readable data contract (schema, quality rules, SLAs, evolution policies).
4. Auto-generate production-grade pipeline code (Glue ETL jobs, CDK infrastructure, Step Functions orchestration).
5. Deploy with observability, alerting, and error-quarantine built in.
6. Handle schema evolution, drift detection, and self-healing gracefully over time.

**The goal:** Reduce source onboarding from 2-4 weeks to under 1 hour while maintaining production-grade quality, security, and observability.

---

## Onboarding

**What this power does:** Helps you build and operate self-serve data ingestion pipelines on AWS — from source discovery through production deployment and ongoing operations. It generates production-grade Glue ETL jobs, CDK infrastructure, data contracts, and observability configuration from YAML configs. It covers the full lifecycle: connect, infer, contract, generate, deploy, monitor, evolve, and retire.

**What this power does NOT do:** Provide legal advice on data privacy regulations, replace a qualified Data Protection Officer, or guarantee regulatory compliance. Always verify data classification decisions (especially PII/PHI sensitivity tagging) with your organization's privacy and legal teams. The sensitivity patterns in `schema-inference-and-classification` are heuristic-based — they catch common patterns but are not exhaustive.

**Scope:** This power targets AWS-native data lake architectures using Glue, S3/Iceberg, Athena, Step Functions, and CDK. It does not cover non-AWS platforms, real-time streaming analytics (beyond Glue Streaming ETL), or ML/AI model training pipelines.

---

## Getting Started

Describe what you need. No jargon required — the power will ask clarifying questions and guide you through the appropriate workflow.

Examples:
- "Onboard our PostgreSQL orders database into the data lake"
- "Connect to our Snowflake analytics warehouse"
- "What does the schema of our vendor CSV files look like?"
- "Generate a data contract for the customer_events table"
- "Deploy the orders pipeline to production"
- "Check if our pipelines are healthy"
- "What would it cost to ingest 50 GB daily from Oracle?"
- "The orders pipeline has schema drift — help me handle it"
- "Retire the legacy_reports source"

---

## Example Journeys

**"Onboard our PostgreSQL orders database into the data lake"**

The agent asks about the database endpoint, table name, incremental column, and schedule frequency. It discovers existing Glue connections in your account, tests connectivity with a two-phase check, samples 5000 rows to infer schema and classify sensitivity, generates a data contract with quality rules, produces a Glue ETL job + CDK stack, deploys to dev, runs a smoke test, and promotes through test to prod. You walk away with a production pipeline running on schedule with CloudWatch alerting and a versioned data contract.

**"We're getting a new vendor CSV drop daily in S3 — set it up"**

The agent asks about the S3 bucket/prefix, file format, delimiter, and whether headers exist. It reads a sample file, infers the schema, classifies any PII columns, generates a contract with volume bounds and freshness SLA, produces an incremental pipeline using Glue bookmarks, and deploys with EventBridge scheduling. Total time: under 30 minutes.

**"Our pipelines have been slow lately — what's going on?"**

The agent runs a health check across all active pipelines. It reports freshness SLA status, identifies two pipelines with duration spikes (data volume grew 3x), recommends right-sizing workers from G.1X to G.2X for one and switching to daily schedule for another, and estimates $45/month savings from the optimization.

**"The customer table added new columns — handle the drift"**

The agent detects the drift (2 new nullable columns), classifies it as low-severity additive change, checks the contract's evolution policy (additive_auto), auto-applies the ALTER TABLE to both the main and quarantine Iceberg tables, bumps the contract from v1.2.0 to v1.3.0, and notifies registered consumers.

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

### Discovery Before Action

Before generating any pipeline, identify the context. Ask 1-2 questions at a time:

1. **Source type** — What kind of data source? (S3 file, database, API, streaming?)
2. **Sensitivity** — Does it contain PII, PHI, or financial data?
3. **Schedule** — How fresh does the data need to be? (Real-time, hourly, daily?)
4. **Volume** — Approximately how much data per batch?
5. **Target** — Where should it land? (Existing table, new table, specific namespace?)

Each answer shapes what comes next. If the user says "it has customer emails," the next question is about masking strategy, not schedule frequency.

---

## Target Platform

| Component | Technology |
|-----------|-----------|
| **Compute** | AWS Glue (PySpark ETL jobs, Glue 4.0) |
| **Storage** | Amazon S3 + Apache Iceberg (via Glue Data Catalog) |
| **Orchestration** | AWS Step Functions + Amazon EventBridge |
| **Metadata** | Glue Data Catalog + DynamoDB (source registry) |
| **Governance** | AWS Lake Formation (column-level security, tagging) |
| **Observability** | Amazon CloudWatch + SNS (metrics, alarms, alerts) |
| **Secrets** | AWS Secrets Manager |
| **Deployment** | AWS CDK (Python) with multi-environment CI/CD |
| **Query Layer** | Amazon Athena |
| **Schema Inference** | Glue Crawlers + Spark-based type detection |

---

## Supported Source Types

| Source Type | Connector | Method | Template |
|-------------|-----------|--------|----------|
| S3 Files (CSV, JSON, Parquet) | S3 + Glue Bookmark | Incremental file discovery | `s3_incremental_template` |
| Relational DB (PostgreSQL, MySQL, Oracle, SQL Server) | JDBC + Glue Connection | Watermark-based incremental, MERGE INTO | `jdbc_ingestion_template` |
| Amazon RDS / Aurora | JDBC via VPC | Same as above + VPC networking | `jdbc_ingestion_template` |
| SaaS (Salesforce, HubSpot, Workday, SAP) | REST API + SDK | Paginated fetch, OAuth refresh | `api_ingestion_template` |
| Cloud Storage (ADLS, GCS) | S3-compatible adapter | Event-driven + Crawler | `s3_incremental_template` |
| Streaming (Kafka, Kinesis) | Glue Streaming ETL | Continuous micro-batch, checkpointing | `streaming_template` |
| Amazon DynamoDB | Direct read | Full/incremental export | Custom |
| Snowflake | SNOWFLAKE connection type | Glue native connector | Custom |
| BigQuery | BIGQUERY connection type | Cross-cloud adapter | Custom |

---

## Available Steering Files

This power organizes deep knowledge into on-demand steering files. Load only what is relevant to the current task.

| Steering File | Use When |
|---------------|----------|
| **onboarding-workflow** | Onboarding a new data source end-to-end (the master 17-step workflow) |
| **s3-file-ingestion** | Building S3 file pipelines (CSV, JSON, Parquet) with Glue bookmarks |
| **jdbc-rds-ingestion** | Building JDBC/RDS pipelines with watermark incremental + MERGE INTO + PII |
| **saas-streaming-ingestion** | Building REST API (SaaS) or Kafka/Kinesis streaming pipelines |
| **pipeline-generation** | Understanding the code generator's template model and custom hooks |
| **schema-inference** | Inferring schema, detecting drift, classifying sensitive columns, handling evolution |
| **data-contracts** | Authoring data contracts: schema definitions, quality rules, SLA, evolution policies |
| **data-contract-governance** | Contract lifecycle, versioning rules, enforcement points, MUST rules |
| **cdk-infrastructure** | CDK stack patterns for deploying infrastructure (S3, Glue, Step Functions, etc.) |
| **connection-setup** | Creating and testing Glue connections (JDBC, Snowflake, BigQuery) |
| **observability** | Monitoring, alerting, structured logging, CloudWatch metrics, dashboards |
| **security-and-access** | RBAC, credential management, approval matrices, audit trails |
| **ingestion-standards** | Non-negotiable engineering rules for every generated pipeline |
| **troubleshooting** | Diagnosing failures, runbooks, self-healing recommendations |
| **well-architected** | **ALWAYS** — 52 rules across 6 AWS Well-Architected pillars enforced on every pipeline generation |

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

### Platform Custom Skills (original — authored by MANIOX)

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

1. **Zero hand-written plumbing** — every pipeline is generated from config; custom logic is injected only where business-specific.
2. **Contracts at the boundary** — every source has a data contract defining schema, freshness SLA, volume bounds, and quality rules.
3. **Observe everything** — pipelines emit CloudWatch metrics (rows processed, errors, latency, freshness) from day one.
4. **Fail fast, fail loud** — errors are quarantined, SNS alerts fired, and never silently swallowed.
5. **Immutable bronze** — raw data is append-only Iceberg; never mutate the landing layer.
6. **Self-service does not mean ungoverned** — business users can onboard sources, but every request passes through automated validation and (where needed) human approval.
7. **Schema is a first-class citizen** — inferred, versioned, and evolution-handled automatically.
8. **Incremental by default** — Glue bookmarks for S3, watermarks for JDBC, checkpoints for streaming.
9. **Never drop data** — quarantine invalid rows with clear error reasons.
10. **Least privilege everywhere** — IAM scoped to specific resources, Lake Formation for column-level access.

---

## Capability Model

The platform is organized into seven interlocking capabilities:

| # | Capability | What It Delivers |
|---|-----------|------------------|
| 1 | **Source Registry** | Centralized inventory of all data sources (DynamoDB) with metadata, status, and lineage — discovery powered by `skills/finding-data-lake-assets` and `skills/exploring-data-catalog` |
| 2 | **Connection Manager** | Glue connection setup, credential registration, connectivity testing — powered by `skills/connecting-to-data-source` |
| 3 | **Schema Inference Engine** | Automatic schema detection, typing, sensitive-field classification, drift detection |
| 4 | **Data Contract Engine** | Machine-readable contracts: define, validate, version, and enforce quality agreements |
| 5 | **Pipeline Generator** | Template-driven, config-to-code engine producing AWS Glue ETL jobs + CDK stacks — execution powered by `skills/ingesting-into-data-lake` and `skills/creating-data-lake-table` |
| 6 | **Observability** | CloudWatch metrics, freshness SLAs, SNS alerting, dashboards, and self-healing recommendations — validation via `skills/querying-data-lake` |
| 7 | **Self-Serve Portal** | Business-user-facing request, approve, generate, deploy experience |

### How Capabilities Interlock

```
Business User / Data Team
         |
         v
Self-Serve Portal  -->  Source Registry  -->  Connection Manager
(request + approve)      (register)            (test connectivity)
                              |                       |
                              v                       v
                        Schema Inference  --->  Data Contract Engine
                        (detect + classify)    (validate + version)
                              |                       |
                              v                       v
                        Pipeline Generator  <--- Contract Validation
                        (config -> Glue job + CDK)
                              |
                              v
                        Deployed Pipeline
                        (running in AWS Glue)
                              |
                              v
                        Observability
                        (CloudWatch + SNS + Self-Healing)
```

---

## Onboarding a New Data Source

### High-Level Flow

```
1. Source owner fills out onboarding-request.yaml
   ↓
2. Duplicate check in Source Registry (DynamoDB)
   ↓
3. Register source (status: pending)
   ↓
4. Test connectivity (Glue Connection + Secrets Manager)
   ↓
5. Infer schema + classify sensitivity
   ↓
6. Risk assessment (auto-approve if no PII; Data Steward review if sensitive)
   ↓
7. Author data-contract.yaml (schema, quality rules, SLAs)
   ↓
8. Author pipeline-config.yaml (source connection, execution, environments)
   ↓
9. Generate Glue ETL job from templates
   ↓
10. Deploy CDK stack (buckets, Glue job, Step Functions, alarms)
   ↓
11. Upload Glue script to S3, run smoke test in dev
   ↓
12. Validate results in Athena, check quarantine table
   ↓
13. Promote through dev -> test -> prod
   ↓
14. Activate observability (CloudWatch metrics, alarms, dashboards)
   ↓
15. Update Source Registry (status: active)
   ↓
16. Notify requester
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
{source-name}-ingestion-pipeline/
├── config/
│   ├── onboarding-request.yaml     # Source metadata, owner, sensitivity, SLA
│   ├── data-contract.yaml          # Schema, quality rules, evolution policy
│   └── pipeline-config.yaml        # Connection, execution, environments
├── src/
│   └── {source}_{table}_ingest.py  # Glue ETL job (generated)
│   └── utils/                      # Shared utilities
│       ├── contract_validator.py
│       ├── metrics_emitter.py
│       ├── quarantine_router.py
│       └── watermark_manager.py    # (JDBC sources only)
├── cdk/
│   ├── app.py                      # CDK app entry point
│   ├── cdk.json                    # CDK context (env-specific overrides)
│   ├── ingestion_stack.py          # Full infrastructure stack
│   ├── constructs/                 # Modular CDK constructs
│   │   ├── glue_job.py
│   │   ├── step_function.py
│   │   ├── eventbridge_schedule.py
│   │   └── iam_roles.py
│   └── requirements.txt            # CDK dependencies
├── resources/
│   └── state_machine.json          # Step Functions definition (reference)
├── tests/
│   └── test_{source}_{table}_ingest.py  # Unit tests (generated)
├── reports/
│   └── generation_report.md        # Audit trail of what was generated
├── sample-data/                    # Test data for validation
└── README.md
```

---

## Core Patterns

### Quarantine Pattern

Invalid rows are **never dropped silently**. They are routed to a `{table}_quarantine` Iceberg table with:
- `_error_reason` — which validation rule failed
- `_quarantined_at` — timestamp
- `_batch_id` — correlates to the main pipeline run

If the quarantine rate exceeds a configurable threshold (default 5%), the pipeline **halts** and fires an alarm.

### Audit Columns

Every row in the bronze layer carries:

| Column | Purpose |
|--------|---------|
| `_ingested_at` | UTC timestamp of ingestion |
| `_source_file` | S3 path or JDBC URI identifying the source |
| `_batch_id` | UUID correlating all rows in one pipeline run |
| `_pipeline_version` | Semantic version of the ETL code |
| `_row_hash` | SHA-256 hash for deduplication |
| `_ingested_date` | Date partition key (derived from `_ingested_at`) |

### Deduplication

- **Within-batch**: Hash-based (`_row_hash`) — exact duplicate rows removed
- **Cross-batch** (JDBC/CDC only): MERGE INTO by merge key — updates if newer, inserts if new

### Data Quality Validation

Quality rules from the data contract are enforced in-pipeline:
- Nullability checks on required fields
- Range validations (min/max values)
- Pattern matching (regex for emails, IDs)
- Allowed value sets (enum validation)
- Cross-field validation (e.g., `updated_at >= created_at`)
- Referential integrity where applicable

### Schema Drift Handling

| Drift Type | Impact | Response |
|------------|--------|----------|
| New column (additive) | Low | Auto-add as nullable; bump MINOR |
| Missing column | Medium | Alert; check if temporary or permanent |
| Type widening (INT→BIGINT) | Low | Auto-cast; bump MINOR |
| Type narrowing | High | Quarantine affected rows; pause; MAJOR bump |
| Column rename | High | Treated as drop + add; human review required |

### Environment Promotion

```bash
# Dev (default, EventBridge disabled)
cdk deploy --context env=dev

# Test (integration testing)
cdk deploy --context env=test

# Production (EventBridge enabled, higher worker count)
cdk deploy --context env=prod
```

---

## Pluggable Source Connectors

| Source Type | Connector | Method | Key Features |
|-------------|-----------|--------|-------------|
| Relational DB | JDBC | Glue Connection + Spark JDBC | Watermark incremental, parallel reads, MERGE INTO |
| SaaS (Salesforce, HubSpot, etc.) | REST API + SDK | Paginated fetch → Iceberg | OAuth refresh, rate limiting, pagination |
| Cloud Storage (S3, ADLS, GCS) | S3 Event + Glue Crawler | Incremental file discovery | Bookmark tracking, schema inference |
| Streaming (Kafka, Kinesis) | Glue Streaming ETL | Continuous micro-batch | Checkpointing, exactly-once semantics |
| Flat Files (CSV, JSON, Parquet, XML) | S3 + Glue Crawler | Schema inference + processing | Format auto-detection, encoding handling |
| Snowflake | SNOWFLAKE connection type | Native Glue connector | NOT JDBC (important!) |
| BigQuery | BIGQUERY connection type | Cross-cloud adapter | Service account auth |

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
| [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/) | ETL jobs, connections, crawlers, Data Catalog |
| [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) | Managed Iceberg tables with auto-compaction |
| [Apache Iceberg on AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/) | Iceberg table format patterns on AWS |
| [Amazon Athena User Guide](https://docs.aws.amazon.com/athena/latest/ug/) | SQL queries across Glue, S3 Tables, Redshift |
| [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/) | Workflow orchestration for pipelines |
| [AWS CDK Developer Guide](https://docs.aws.amazon.com/cdk/v2/guide/) | Infrastructure as Code (Python) |
| [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/) | Column-level security, data governance |
| [AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/) | Credential management and rotation |
| [Amazon CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/) | Metrics, alarms, dashboards |
| [AWS Well-Architected Framework](https://docs.aws.amazon.com/wellarchitected/latest/framework/) | Six pillars of architecture best practices |
| [AWS Glue Connection Types](https://docs.aws.amazon.com/glue/latest/dg/connection-types.html) | JDBC, SNOWFLAKE, BIGQUERY, KAFKA connection setup |
| [Amazon EventBridge Scheduler](https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduler.html) | Cron-based pipeline scheduling |

---

## Support & Legal

- **License**: Apache-2.0 License (see [LICENSE](LICENSE))
- **Authors**: Rahul Agarwal, Manish Choudhary
- **Issues**: Please report bugs and feature requests via GitHub Issues

**Disclaimer:** This power provides technical guidance for building data pipelines on AWS. It does not provide legal, regulatory, or compliance advice. Data classification (PII/PHI) is heuristic-based and should be validated by qualified personnel. Always verify compliance requirements with your organization's legal and privacy teams.
