---
inclusion: fileMatch
fileMatchPattern: '**/onboarding-request*'
---

# Source Tool Selection

## Purpose

This guide determines which AWS ingestion service to use for each source type.
**Do not default to Glue JDBC.** Use the purpose-built service for each source.

---

## Decision Matrix

| Source | CDC Needed? | Transforms at Ingestion? | Recommended Tool | Landing Target |
|---|---|---|---|---|
| Aurora MySQL/PostgreSQL → Redshift only | Yes | No | **Aurora Zero-ETL** | Native (no S3 Landing) |
| Aurora MySQL/PostgreSQL (snapshots) | No | No | **Aurora S3 Export** | S3 Parquet |
| Any RDS / Aurora / self-managed DB | Yes | No | **AWS DMS** (ongoing CDC) | S3 Parquet via DMS |
| Any RDS / Aurora / self-managed DB | Yes | Yes | **AWS DMS** → Landing → **Glue** | DMS → S3 → Glue |
| Any RDS / Aurora / self-managed DB | No (full load only) | Yes | **AWS DMS** (full load) → Landing → **Glue** | DMS → S3 → Glue |
| Salesforce | Any | No | **Amazon AppFlow** | S3 Parquet |
| SAP, ServiceNow, HubSpot, Zendesk, Workday, Marketo + 45 more | Any | No | **Amazon AppFlow** | S3 Parquet |
| SaaS not in AppFlow connector list | Any | Yes | Custom **Glue** (REST API) | Glue → S3 |
| Kinesis Data Streams | Real-time | No | **Kinesis Data Firehose** | S3 Parquet (native JSON→Parquet) |
| Amazon MSK (Kafka) | Real-time | No | **Firehose** (MSK connector) | S3 Parquet |
| Kinesis / MSK (transforms needed) | Real-time | Yes | **Glue Streaming ETL** | Glue → S3 Iceberg |
| S3 file drops (already in S3) | N/A | Yes | S3 Event → **Glue** | Already in Landing |
| SFTP / FTP / FTPS partner files | N/A | Any | **AWS Transfer Family** → S3 | Transfer Family |
| On-prem NFS / SMB / HDFS shares | Any | Any | **AWS DataSync** → S3 | DataSync |
| Large migration > 10 TB | One-time | Any | **Snow Family** + DataSync | Snow → S3 |
| Mainframe (VSAM, DB2, IMS) | Any | Any | **AWS Mainframe Modernization** | M2 → S3 |
| Cross-account Glue tables | N/A | No | **Lake Formation sharing** | No data movement |
| Third-party datasets | N/A | No | **AWS Data Exchange** | Data Exchange → S3 |
| Redshift → data lake | Any | No | **Redshift UNLOAD** or Data Sharing | UNLOAD → S3 |
| Snowflake | Any | Yes | Glue **SNOWFLAKE** connector | Glue → S3 |
| BigQuery | Any | Yes | Glue **BIGQUERY** connector | Glue → S3 |
| DynamoDB | Any | No | **DynamoDB S3 Export** | Native S3 Export |

---

## Cost Comparison Template

Before recommending a tool, present a cost comparison to the user:

```
For your workload: [{source_type}, {num_tables} tables, ~{volume}/day, {sync_pattern}]

Option A: {recommended_tool}
  Cost: ~${monthly_cost}/month
  Pros: {benefits}
  Cons: {limitations}

Option B: {alternative_tool}
  Cost: ~${monthly_cost}/month
  Pros: {benefits}
  Cons: {limitations}

Option C: {cheapest_option} (if applicable)
  Cost: ~${monthly_cost}/month
  Pros: {benefits}
  Cons: {limitations}

Which approach fits your requirements?
```

### Cost Estimation Formulas

**DMS:**
- Replication instance: t3.medium ~$75/month (on-demand), r5.large ~$250/month
- Storage: $0.115/GB-month for GP2
- Data transfer: $0.02/GB outbound
- **Typical small CDC workload (5 tables, <10 GB/day): ~$45-75/month**

**AppFlow:**
- Flow runs: $0.001 per flow run
- Data processed: $0.025 per GB
- **Typical SaaS sync (daily, 5 objects, 1 GB total): ~$15-25/month**

**Kinesis Data Firehose:**
- Data ingested: $0.029 per GB (first 500 TB)
- Format conversion: $0.018 per GB
- **Typical streaming (5 GB/day): ~$7-10/month**

**Transfer Family:**
- Endpoint: $0.30/hour (~$216/month for always-on)
- Data upload: $0.04 per GB
- **Tip: use on-demand endpoint (start/stop) for scheduled transfers to reduce cost**

**Glue JDBC (for comparison):**
- DPU-hour: $0.44
- Typical 5-table JDBC job (G.1X, 5 workers, 15 min): 5 × 0.25h × $0.44 = $0.55/run
- Daily: ~$16.50/month. **But** VPC, SG, connection maintenance adds operational cost.

**Aurora Zero-ETL (Aurora → Redshift):**
- $0.00 for the integration itself
- Standard Aurora and Redshift pricing applies
- **Cheapest option when target is Redshift**

**Aurora S3 Export:**
- $0.01 per GB exported (Parquet)
- **For snapshots only — no CDC. ~$3-8/month for nightly 300 GB snapshot**

---

## When to Still Use Glue JDBC

Use Glue with a JDBC connection ONLY when ALL of these are true:
1. The source is a database (not SaaS, not streaming, not files)
2. You need complex Spark transforms AT read time (not just CDC/replication)
3. No simpler service fits (DMS, Aurora S3 Export, Aurora Zero-ETL)
4. The volume justifies the cost and complexity

**Even then:** prefer DMS → Landing → Glue (reads from S3) over Glue reading directly from the database via JDBC.

---

## MUST Rules

- MUST present cost comparison before recommending a tool
- MUST ask Q2 (CDC vs snapshot) and Q3 (transforms needed?) before selecting
- MUST NOT default to Glue JDBC for database sources — try DMS first
- MUST NOT use Glue JDBC for SaaS sources — AppFlow handles 50+ platforms
- MUST NOT use Glue Streaming for no-transform streaming — Firehose is 10× cheaper
- MUST recommend Aurora Zero-ETL when target is specifically Redshift
- MUST recommend Aurora S3 Export for snapshot-only workloads (no CDC requirement)
