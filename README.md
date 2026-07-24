# Self-Serve Data Onboarding — Kiro Power

A [Kiro Power](https://kiro.dev) that moves data from any source to S3 Landing using the right AWS service, then profiles it. No transforms, no schema enforcement, no masking — just land the data and describe it.

## What It Does

1. **Select the right tool** — DMS for databases, AppFlow for SaaS, Firehose for streaming, Transfer Family for SFTP, DataSync for on-prem
2. **Land data in S3 as-is** — no transforms, no audit columns, no type casting
3. **Profile the landed data** — column types, PII candidates, statistics, recommendations
4. **Publish a report** — JSON (machine-readable) + Markdown (human-readable)

## What It Does NOT Do

- Medallion architecture (Bronze/Silver/Gold)
- Data transformation
- Schema enforcement or evolution
- PII masking or redaction
- Quality gates that block pipelines
- Data contracts as enforcement
- Iceberg tables or deduplication

Those are downstream concerns. The profiling report gives those teams what they need.

## Architecture

```
Source System
     │
     ▼  (DMS / AppFlow / Firehose / Transfer Family / DataSync)
┌──────────────────────────────────────────────────────┐
│  S3 LANDING — data arrives exactly as-is             │
│  Partitioned: year/month/day |Retention: configurable│
└──────────────────────────────────────────────────────┘
     │
     ▼  (AWS Glue DataBrew)
┌──────────────────────────────────────────────────────┐
│  PROFILING REPORT — stats, PII candidates, recs      │
└──────────────────────────────────────────────────────┘
```

## Supported Sources

| Source | Service | Cost vs Glue JDBC |
|---|---|---|
| RDS / Aurora / Oracle / SQL Server (CDC) | **AWS DMS** | 4× cheaper |
| Salesforce, SAP, HubSpot + 45 more | **Amazon AppFlow** | 10× cheaper |
| Kinesis / Kafka (no transforms) | **Kinesis Firehose** | 20× cheaper |
| SFTP / FTP partner files | **AWS Transfer Family** | Purpose-built |
| On-prem file shares | **AWS DataSync** | Purpose-built |
| Already in S3 | **EventBridge** (detect) | Free |

## Installation

```bash
git clone https://github.com/rahulagw-83/kiro-power-self-serve-data-onboarding.git
```

## Structure

```
self-serve-data-onboarding/
├── POWER.md                    # Main power definition
├── steering/                   # 9 steering files
│   ├── onboarding-workflow.md  # Discover → validate → deploy → profile
│   ├── source-tool-selection.md # Right tool per source type
│   ├── landing-layer.md        # S3 paths, retention, partitioning
│   ├── pre-deployment-validation.md # 7 mandatory checks
│   ├── data-profiling.md       # DataBrew setup, PII patterns, report spec
│   ├── connection-setup.md     # Glue/DMS connections
│   ├── observability.md        # Did data arrive? Is it fresh?
│   ├── troubleshooting.md      # Error taxonomy + auto-remediation
│   └── security-and-access.md  # IAM, Secrets Manager, networking
├── skills/                     # 9 executable skills
│   ├── connecting-to-data-source/
│   ├── provisioning-landing-pipeline/
│   ├── deploying-terraform/
│   ├── data-profiling/
│   ├── exploring-data-catalog/
│   ├── finding-data-lake-assets/
│   ├── monitoring-pipeline-health/
│   ├── cost-estimation-and-optimization/
│   └── decommissioning-data-source/
├── hooks/                      # Pre/post-ingestion hooks
├── terraform/                  # (generated per source, not stored here)
├── LICENSE                     # Apache-2.0
└── .gitignore
```

## Quick Start

Just describe what you need:

- *"Onboard our Aurora MySQL orders database"*
- *"Set up daily Salesforce contacts sync"*
- *"We get vendor CSVs via SFTP"*
- *"Stream Kafka events to S3"*
- *"Profile the data that landed yesterday"*

The Power asks 3-4 questions, recommends a service with cost estimate, validates connectivity, and deploys via Terraform.

## IaC

**Terraform only.** No CDK. No CloudFormation. One tool, consistent structure.

Terraform version: ask your org's pinned version or use latest stable.

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Authors

**Rahul Agarwal** and **Manish Choudhary**
