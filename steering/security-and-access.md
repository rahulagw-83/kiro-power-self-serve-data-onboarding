---
inclusion: always
---

# Security & Access Controls

> Always-on standard. Defines who can request, approve, and deploy ingestion
> pipelines, and how secrets/credentials are managed on AWS.

## 1. Role-Based Access Model

| Role | Can Request | Can Approve | Can Deploy | Can Troubleshoot |
|------|-------------|-------------|------------|------------------|
| **Business Analyst** | Yes | No | No | No (view status only) |
| **Data Scientist** | Yes | No | No | No (view status only) |
| **Data Steward** | Yes | Yes (classification) | No | No |
| **Data Engineer** | Yes | Yes (technical) | Yes | Yes |
| **Platform Admin** | Yes | Yes (all) | Yes | Yes |

## 2. Approval Matrix

| Source Sensitivity | Auto-Approve? | Required Approvals |
|-------------------|---------------|-------------------|
| Public / non-sensitive | Yes (auto-deploy) | None — automated validation only |
| Internal / business-sensitive | No | 1x Data Steward |
| PII / PHI / regulated | No | 1x Data Steward + 1x Privacy Officer |
| External third-party | No | 1x Data Steward + 1x Legal + 1x Security |

## 3. Credential Management

- **MUST** store all source credentials in **AWS Secrets Manager**.
- **MUST NOT** include credentials in pipeline config, source code, or environment variables.
- Secret naming convention: `ingestion/<source_name>/<credential_type>`
  - Example: `ingestion/salesforce_crm/oauth_token`
  - Example: `ingestion/postgres_orders/jdbc_password`
- Secrets rotated per organization policy (default: 90 days). Use Secrets Manager automatic rotation where supported.
- Kiro-generated pipelines reference secrets by name/ARN; Kiro never sees the plaintext value.

### Secret Access Pattern

```python
import boto3
import json

# Correct: retrieve by secret name via boto3
secrets_client = boto3.client('secretsmanager')
response = secrets_client.get_secret_value(SecretId='ingestion/postgres_orders/jdbc_password')
credentials = json.loads(response['SecretString'])

# NEVER: hardcode credentials
# password = "my-secret-password"  # FORBIDDEN
```

### Prefer IAM Database Authentication Where Supported

- Aurora MySQL/PostgreSQL: Yes
- RDS MySQL/PostgreSQL: Yes
- Redshift: Yes (temporary credentials)
- Snowflake: No (use key-pair or OAuth)

## 4. Network Access

- Source connections route through **VPC Endpoints** (Gateway for S3, Interface for Glue, Secrets Manager) or **VPC Peering** where possible.
- Internet-facing APIs (SaaS connectors) use a **NAT Gateway** in a private subnet with egress controls.
- All connections must use TLS 1.2+ encryption in transit.
- Glue jobs run within a VPC using Glue Connection for JDBC sources; ENIs are placed in private subnets.
- IP allowlisting on the source side is configured as part of the onboarding request.

## 5. Data Classification at Ingestion

- Every column is auto-classified during schema inference (rule-based + name heuristics + content inspection).
- Classification drives: Lake Formation tags, tag-based access control (TBAC), column-level permissions.
- Override via the data contract: the source owner can promote/demote a column's classification.

| Tier | Access | Examples |
|------|--------|----------|
| `public` | All authenticated users | Product names, public metrics |
| `internal` | Employees only | Revenue figures, internal IDs |
| `sensitive` | Need-to-know basis | Salary data, strategic plans |
| `restricted` | Regulated access only | SSN, medical records, credit cards |

## 6. IAM Role Configuration

- Each environment uses a dedicated IAM role: `GlueRole-dev`, `GlueRole-test`, `GlueRole-prod`
- Roles scoped to minimum required permissions (source S3/JDBC + target S3 + Glue catalog)
- Role trust policies restrict assumption to the specific Glue service and Step Functions
- Access auto-revoked when a pipeline is decommissioned

### IAM Policy Example (Least Privilege)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::source-data-raw/<source_name>/*",
        "arn:aws:s3:::source-data-raw"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::datalake-bronze/<source_name>/*"
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:<region>:<account>:secret:ingestion/<source_name>/*"
    },
    {
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:UpdateTable", "glue:CreateTable", "glue:GetDatabase"],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/<source_name>_bronze*",
        "arn:aws:glue:<region>:<account>:table/<source_name>_bronze*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData"],
      "Resource": ["*"],
      "Condition": {
        "StringEquals": {"cloudwatch:namespace": "IngestionPipeline/*"}
      }
    }
  ]
}
```

## 7. Encryption

| Data State | Non-PII Sources | PII Sources |
|------------|-----------------|-------------|
| At rest (S3) | SSE-S3 | SSE-KMS (dedicated CMK per tier) |
| In transit | TLS 1.2+ | TLS 1.2+ |
| Logs | No PII values | PII redacted in structured logger |
| Secrets | Secrets Manager | Secrets Manager + rotation |

## 8. Audit Trail

- Every onboarding event logged in DynamoDB (`platform-audit-ingestion-events`).
- CloudTrail captures all AWS API calls for infrastructure-level audit.
- Event types tracked: `REQUEST`, `APPROVE`, `GENERATE`, `DEPLOY`, `DECOMMISSION`, `SCHEMA_CHANGE`
- Audit log retention: minimum 1 year (regulatory compliance).

## 9. MUST Rules

- **MUST** follow least privilege — generated pipelines use IAM roles scoped to the source and target resources only.
- **MUST** log every secret access attempt (CloudTrail logs Secrets Manager calls automatically).
- **MUST** never expose raw credentials in logs, metrics, or error messages.
- **MUST** encrypt data in transit (TLS 1.2+) and at rest (SSE-S3 / SSE-KMS).
- **MUST** remove IAM role permissions if a pipeline is decommissioned.
- **MUST** enforce the approval matrix — no exceptions for "urgency."
- **MUST** retain audit logs for minimum 1 year (regulatory compliance).
- **MUST** use VPC endpoints for S3 and Glue if processing sensitive data in private subnets.
