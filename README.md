# Self-Serve Data Onboarding — Kiro Power

A production-grade [Kiro Power](https://kiro.dev/powers/) for building and operating self-serve data ingestion pipelines on AWS. Reduce source onboarding from 2-4 weeks to under 1 hour while maintaining production-grade quality, security, and observability.

## What It Does

This Power turns Kiro into a data platform engineer that can:

- **Discover** existing data sources and connections in your AWS account
- **Connect** to JDBC databases, Snowflake, BigQuery, and S3 with tested Glue connections
- **Infer** schema automatically — detecting types, nullability, and sensitive fields (PII/PHI)
- **Generate** machine-readable data contracts with quality rules, SLAs, and evolution policies
- **Build** production-grade Glue ETL pipelines with CDK infrastructure from YAML config
- **Deploy** across environments (dev → test → prod) with gate checks and rollback
- **Monitor** pipeline health, freshness SLAs, and cost trends proactively
- **Evolve** schemas gracefully — auto-handling safe changes, alerting on breaking ones
- **Retire** sources cleanly when they reach end-of-life

## Target Platform

| Component | Technology |
|-----------|-----------|
| Compute | AWS Glue (PySpark ETL, Glue 4.0+) |
| Storage | Amazon S3 + Apache Iceberg (via Glue Data Catalog) |
| Orchestration | AWS Step Functions + Amazon EventBridge |
| Governance | AWS Lake Formation + Data Contracts |
| Observability | Amazon CloudWatch + SNS |
| Deployment | AWS CDK (Python), Terraform (HCL), or CloudFormation (YAML) |
| Query Layer | Amazon Athena |

## Supported Sources

- S3 Files (CSV, JSON, Parquet, Avro, ORC)
- Relational Databases (PostgreSQL, MySQL, Oracle, SQL Server) via JDBC
- Amazon RDS / Aurora
- Amazon Redshift
- Snowflake
- Google BigQuery
- Amazon DynamoDB
- SaaS APIs (Salesforce, HubSpot, Workday)
- Streaming (Kafka, Kinesis)

## Installation

Clone this repository into your Kiro workspace:

```bash
git clone https://github.com/rahulagw-83/kiro-power-self-serve-data-onboarding.git
```

Or install as a Kiro Power via the Kiro Power panel.

## Structure

```
self-serve-data-onboarding/
├── POWER.md                    # Main power definition (start here)
├── steering/                   # 15 deep-knowledge steering files
│   ├── onboarding-workflow.md  # Master 17-step workflow
│   ├── connection-setup.md     # Glue connection patterns
│   ├── schema-inference.md     # Type detection + PII classification
│   ├── data-contracts.md       # Contract authoring reference
│   ├── pipeline-generation.md  # Code generator template model
│   ├── cdk-infrastructure.md   # CDK stack patterns
│   ├── observability.md        # Monitoring + alerting
│   ├── troubleshooting.md      # Diagnostic runbooks
│   ├── well-architected.md     # 52 rules across 6 WA pillars
│   └── ...
├── skills/                     # 13 executable skills
│   ├── connecting-to-data-source/
│   ├── ingesting-into-data-lake/
│   ├── creating-data-lake-table/
│   ├── querying-data-lake/
│   ├── finding-data-lake-assets/
│   ├── exploring-data-catalog/
│   ├── schema-inference-and-classification/
│   ├── data-contract-authoring/
│   ├── deploying-cdk-pipeline/
│   ├── monitoring-pipeline-health/
│   ├── managing-schema-evolution/
│   ├── decommissioning-data-source/
│   └── cost-estimation-and-optimization/
├── hooks/                      # Pre/post-ingestion quality gates
│   ├── pre-ingestion-validation.md
│   └── post-ingestion-checks.md
├── LICENSE                     # Apache-2.0
└── .gitignore
```

## Quick Start

Just describe what you need:

- *"Onboard our PostgreSQL orders database into the data lake"*
- *"Connect to our Snowflake analytics warehouse"*
- *"Check if our pipelines are healthy"*
- *"What would it cost to ingest 50 GB daily from Oracle?"*

The Power will ask clarifying questions and guide you through the appropriate workflow.

## Key Principles

1. **Zero hand-written plumbing** — pipelines generated from config
2. **Contracts at the boundary** — every source has a data contract
3. **Observe everything** — CloudWatch metrics from day one
4. **Never drop data** — quarantine invalid rows with error reasons
5. **Incremental by default** — bookmarks for S3, watermarks for JDBC
6. **Least privilege everywhere** — scoped IAM + Lake Formation

## AWS Well-Architected Compliance

This Power enforces **52 rules** across all 6 pillars of the AWS Well-Architected Framework. Every generated pipeline is compliant by default — engineers don't memorize the framework; it's encoded in `steering/well-architected.md`.

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Authors

**Rahul Agarwal** and **Manish Choudhary**
