# Self-Serve Data Onboarding — Kiro Power

A production-grade [Kiro Power](https://kiro.dev) for building and operating self-serve data ingestion pipelines on AWS. Uses the **right AWS service per source type** with a **two-layer Landing+Raw architecture** to reduce source onboarding from 2-4 weeks to under 1 hour.

## What It Does

This Power turns Kiro into a data platform engineer that can:

- **Select the right tool** — DMS for databases, AppFlow for SaaS, Firehose for streaming, Transfer Family for SFTP. Never defaults to Glue JDBC.
- **Implement two layers** — Landing (fast, cheap, raw S3 copy) → Raw/Bronze (trusted, governed, Iceberg with DQDL quality)
- **Validate before deploying** — mandatory pre-deployment checks (connectivity, credentials, SG rules, IAM, ASL) before any code is generated
- **Show costs upfront** — cost comparison between applicable services before the user commits
- **Enforce quality via DQDL** — data contract rules compile to AWS Glue Data Quality (no custom validator code)
- **Auto-remediate failures** — error taxonomy maps 16+ known failure patterns to automatic fixes
- **Deploy with your IaC** — CDK, Terraform (version-pinned), or CloudFormation — your choice

## Architecture

```
Source System
     │
     ▼  (DMS / AppFlow / Firehose / Transfer Family / DataSync)
┌──────────────────────────────────────────────┐
│  LANDING LAYER — Plain S3, no transforms     │
│  Partition: year/month/day | Retention: configurable  │
└──────────────────────────────────────────────┘
     │  S3 Event → EventBridge → Glue
     ▼
┌──────────────────────────────────────────────┐
│  RAW (BRONZE) — S3 + Iceberg                 │
│  Audit cols + DQDL quality + dedup + tags    │
└──────────────────────────────────────────────┘
```

**Key insight:** Glue ETL reads from Landing S3 only — never directly from the source. This eliminates JDBC timeouts, SG complexity, and cold-start issues while cutting worker count by 50%.

## Supported Sources and Recommended Tools

| Source | Recommended Service | Cost vs Glue JDBC |
|---|---|---|
| RDS / Aurora / Oracle / SQL Server (CDC) | **AWS DMS** | 4× cheaper |
| Aurora → Redshift | **Aurora Zero-ETL** | Free (native) |
| Salesforce, SAP, HubSpot, ServiceNow + 45 more | **Amazon AppFlow** | 10× cheaper |
| Kinesis / Kafka / MSK (no transforms) | **Kinesis Data Firehose** | 20× cheaper |
| SFTP / FTP partner file drops | **AWS Transfer Family** | Purpose-built |
| On-premises file shares (NFS, SMB, HDFS) | **AWS DataSync** | Purpose-built |
| S3 file drops (CSV, JSON, Parquet) | **EventBridge → Glue** | Already landed |
| Cross-account data sharing | **Lake Formation** | No data movement |

## Installation

```bash
git clone https://github.com/rahulagw-83/kiro-power-self-serve-data-onboarding.git
```

Or install as a Kiro Power via the Kiro Power panel.

## Structure

```
self-serve-data-onboarding/
├── POWER.md                    # Main power definition (start here)
├── steering/                   # 19 deep-knowledge steering files
│   ├── onboarding-workflow.md  # Master 16-step workflow (6 phases)
│   ├── landing-layer.md        # Landing layer design + service configs
│   ├── source-tool-selection.md # Right tool per source + cost comparison
│   ├── pre-deployment-validation.md # 6-check mandatory validation
│   ├── glue-data-quality.md    # DQDL rule compilation from contracts
│   ├── ingestion-standards.md  # Non-negotiable rules + sizing + DQDL
│   ├── troubleshooting.md      # Error taxonomy + auto-remediation
│   ├── schema-inference.md     # Type detection + PII classification
│   ├── data-contracts.md       # Contract authoring reference
│   ├── cdk-infrastructure.md   # CDK stack patterns
│   ├── observability.md        # Monitoring + alerting
│   ├── well-architected.md     # 52 rules across 6 WA pillars
│   └── ...
├── skills/                     # 13 executable skills
│   ├── ingesting-into-data-lake/       # Landing+Raw two-layer execution
│   ├── connecting-to-data-source/      # Glue connection setup + test
│   ├── schema-inference-and-classification/
│   ├── data-contract-authoring/
│   ├── deploying-cdk-pipeline/         # CDK/Terraform/CFN deploy + promote
│   ├── monitoring-pipeline-health/     # Landing + Raw health checks
│   ├── cost-estimation-and-optimization/ # Service cost comparison
│   ├── managing-schema-evolution/
│   ├── decommissioning-data-source/
│   ├── creating-data-lake-table/
│   ├── querying-data-lake/
│   ├── finding-data-lake-assets/
│   └── exploring-data-catalog/
├── hooks/                      # Pre/post-ingestion quality gates
├── LICENSE                     # Apache-2.0
└── .gitignore
```

## Quick Start

Just describe what you need:

- *"Onboard our Aurora MySQL orders database"*
- *"Set up daily Salesforce contacts sync"*
- *"We get vendor CSV files via SFTP — automate it"*
- *"Stream Kafka events into the data lake"*
- *"What would DMS cost vs Glue JDBC for our 5 tables?"*

The Power asks 7 discovery questions (1-2 per turn), shows a cost comparison, confirms the approach, runs pre-deployment validation, then generates and deploys.

## Key Principles

1. **Right tool per source** — DMS for databases, AppFlow for SaaS, Firehose for streaming
2. **Two layers: Landing + Raw** — Landing gets data fast; Raw makes it trustworthy
3. **Validate before generating** — 6 mandatory checks before any code is produced
4. **DQDL over custom code** — quality rules from contracts, zero maintenance
5. **Cost comparison upfront** — never deploy without knowing the price
6. **Error taxonomy** — known failures auto-remediate, unknown failures escalate
7. **Glue reads S3 only** — never from source directly (eliminates JDBC pain)

## IaC Support

| Tool | Support Level |
|---|---|
| AWS CDK (Python) | Primary — full template library |
| Terraform (HCL) | Full — modules, version pinning, state locking |
| CloudFormation (YAML) | Full — native AWS templates |

Terraform users can pin to any version their org requires (1.3, 1.5, latest).

## AWS Well-Architected Compliance

Every generated pipeline enforces **52 rules** across all 6 pillars automatically.

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Authors

**Rahul Agarwal** and **Manish Choudhary**
