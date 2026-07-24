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
| Any RDS / Aurora / self-managed DB | Yes | N/A | **AWS DMS** (ongoing CDC) | DMS → S3 Landing |
| Any RDS / Aurora / self-managed DB | No (full load only) | N/A | **AWS DMS** (full load) | DMS → S3 Landing |
| Salesforce | Any | No | **Amazon AppFlow** | S3 Parquet |
| SAP, ServiceNow, HubSpot, Zendesk, Workday, Marketo + 45 more | Any | No | **Amazon AppFlow** | S3 Parquet |
| SaaS not in AppFlow connector list | Any | Yes | Custom **Glue** (REST API) | Glue → S3 |
| Kinesis Data Streams | Real-time | No | **Kinesis Data Firehose** | S3 Parquet (native JSON→Parquet) |
| Amazon MSK (Kafka) | Real-time | No | **Firehose** (MSK connector) | S3 Parquet |
| Kinesis / MSK (transforms needed) | Real-time | Yes | **Glue Streaming ETL** → S3 | Glue → S3 Landing |
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
| **Public/External REST APIs** (simple, < 15 min) | Scheduled | N/A | **Lambda + EventBridge Schedule** | Lambda → S3 |
| **Public APIs** (heavy pagination, multi-step auth) | Scheduled | N/A | **Step Functions + Lambda** | SFN → Lambda → S3 |
| **Public APIs** (large volume, > 15 min pull) | Scheduled | N/A | **ECS Fargate Scheduled Task** | Fargate → S3 |

---

## Public / External REST API Pattern

For sources like EV charging data, weather APIs, government open data, market feeds,
geolocation APIs — where there's no managed AWS connector.

### Decision Tree

```
Is the API call simple (single GET, < 1000 records)?
  → YES: Lambda + EventBridge Schedule (cheapest)
  
Does it require pagination (cursor/offset/token, multiple pages)?
  → YES, < 15 min total: Lambda with pagination loop
  → YES, > 15 min total: Step Functions + Lambda (per-page)
  
Does it require > 15 min continuous execution?
  → YES: ECS Fargate Scheduled Task
```

### Lambda + EventBridge Pattern (Default)

**Terraform generates:**
- Lambda function (Python, requests library via layer)
- EventBridge Scheduler rule (cron or rate)
- IAM role (S3 PutObject + Secrets Manager GetSecretValue)
- Secrets Manager secret (API key or OAuth credentials)
- S3 landing path convention

**Lambda template:**
```python
import json, boto3, urllib3
from datetime import datetime

http = urllib3.PoolManager()
s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")

def handler(event, context):
    # Get API credentials from Secrets Manager
    secret = json.loads(
        secrets.get_secret_value(SecretId="{secret_name}")["SecretString"]
    )
    
    # Call the API
    response = http.request(
        "GET",
        "{api_base_url}/{endpoint}",
        headers={"Authorization": f"Bearer {secret['api_key']}"},
        fields={"limit": "1000"}
    )
    data = json.loads(response.data)
    
    # Write to S3 Landing (partitioned by date)
    now = datetime.utcnow()
    key = (
        f"{domain}/{source_name}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{source_name}_{now.strftime('%Y%m%dT%H%M%S')}.json"
    )
    
    s3.put_object(
        Bucket="{landing_bucket}",
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    
    return {"status": "success", "records": len(data), "key": key}
```

### Step Functions + Lambda Pattern (Paginated APIs)

For APIs that return paginated results (cursor, next_token, offset):

```json
{
  "StartAt": "FetchPage",
  "States": {
    "FetchPage": {
      "Type": "Task",
      "Resource": "{lambda_arn}",
      "Parameters": {
        "cursor.$": "$.next_cursor",
        "page.$": "$.page_number"
      },
      "ResultPath": "$.result",
      "Next": "CheckMorePages"
    },
    "CheckMorePages": {
      "Type": "Choice",
      "Choices": [{
        "Variable": "$.result.has_more",
        "BooleanEquals": true,
        "Next": "FetchPage"
      }],
      "Default": "Done"
    },
    "Done": {"Type": "Succeed"}
  }
}
```

### Cost Estimates

| Approach | Monthly Cost (daily pull, ~1 MB/call) | Monthly Cost (hourly, ~10 MB/call) |
|---|---|---|
| Lambda + EventBridge | ~$0.50 | ~$5 |
| Step Functions + Lambda (paginated) | ~$1-3 | ~$10-20 |
| ECS Fargate (15 min task) | ~$5 | ~$30 |
| Glue Python Shell (for comparison) | ~$13 | ~$160 |

### Pre-Deployment Validation (API-specific)

- Verify API endpoint is reachable: `curl -I {base_url}` returns 200
- Verify credentials: test single API call returns data (not 401/403)
- Verify rate limit: check API docs, configure appropriate schedule
- Verify response format: confirm JSON structure matches expectations

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

**Even then:** prefer DMS → S3 Landing over Glue reading directly from the database via JDBC.

---

## MUST Rules

- MUST present cost comparison before recommending a tool
- MUST ask Q2 (CDC vs snapshot) and Q3 (transforms needed?) before selecting
- MUST NOT default to Glue JDBC for database sources — try DMS first
- MUST NOT use Glue JDBC for SaaS sources — AppFlow handles 50+ platforms
- MUST NOT use Glue Streaming for no-transform streaming — Firehose is 10× cheaper
- MUST recommend Aurora Zero-ETL when target is specifically Redshift
- MUST recommend Aurora S3 Export for snapshot-only workloads (no CDC requirement)
