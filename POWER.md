---
name: "self-serve-data-onboarding"
displayName: "Self-Serve Data Onboarding"
description: "Build and operate self-serve data ingestion pipelines on AWS. Onboard sources (S3, JDBC, SaaS, streaming), infer schemas, generate data contracts, and auto-generate production-grade Glue ETL jobs with CDK infrastructure, observability, and governance built in."
keywords: ["data ingestion", "aws glue", "etl", "data pipeline", "iceberg", "s3", "jdbc", "cdk", "data contract", "schema inference", "self-serve", "data lake"]
well_architected: true
author: "MANIOX"
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
| 1 | **Source Registry** | Centralized inventory of all data sources (DynamoDB) with metadata, status, and lineage |
| 2 | **Connection Manager** | Glue connection setup, credential registration, connectivity testing |
| 3 | **Schema Inference Engine** | Automatic schema detection, typing, sensitive-field classification, drift detection |
| 4 | **Data Contract Engine** | Machine-readable contracts: define, validate, version, and enforce quality agreements |
| 5 | **Pipeline Generator** | Template-driven, config-to-code engine producing AWS Glue ETL jobs + CDK stacks |
| 6 | **Observability** | CloudWatch metrics, freshness SLAs, SNS alerting, dashboards, and self-healing recommendations |
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

| Symptom | Category | Steering File |
|---------|----------|---------------|
| "connection refused" / timeout | Connectivity | troubleshooting, connection-setup |
| "authentication failed" / 403 | Credentials | troubleshooting |
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
