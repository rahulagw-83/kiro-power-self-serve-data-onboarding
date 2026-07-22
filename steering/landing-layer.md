---
inclusion: fileMatch
fileMatchPattern: '**/pipeline-config*'
---

# Landing Layer Design

## Purpose

The Landing layer gets data to S3 **as-is, as fast and cheaply as possible**. It is an exact copy of the source with no transforms, no validation, and no masking.

**Key principle:**
- Landing = "Did it arrive?"
- Raw = "Is it correct?"
- Silver = "Is it useful?" (out of scope for this power)

---

## Architecture

```
Source System
     │
     ▼  (DMS / AppFlow / Firehose / Transfer Family / DataSync)
┌────────────────────────────────────────────────────┐
│  s3://{bucket}/landing/{source_name}/              │
│  year=YYYY/month=MM/day=DD/                        │
│                                                     │
│  Format: Parquet (preferred) or source-native       │
│  No Iceberg. No transforms. No audit columns.      │
│  Retention: per project policy (default 90 days).   │
└────────────────────────────────────────────────────┘
     │
     ▼  S3 Event → EventBridge → Step Functions → Glue job
┌────────────────────────────────────────────────────┐
│  RAW / BRONZE (Iceberg)                            │
│  s3://{bucket}/raw/{source_name}/{table}/          │
│  Partitioned by _ingested_date                     │
│  Audit columns + DQDL quality + dedup              │
└────────────────────────────────────────────────────┘
```

---

## Landing Layer Rules

| Rule | Rationale |
|------|-----------|
| **No transforms** | Landing is a raw copy — business logic lives in Raw or Silver |
| **No Iceberg** | Plain S3 for simplicity and cost; Iceberg overhead not needed here |
| **Source format or Parquet** | If ingestion service supports Parquet output (AppFlow, Firehose, DMS), prefer it. Otherwise, land in source format (CSV, JSON). |
| **Partition by arrival date** | `year=YYYY/month=MM/day=DD/` enables time-bounded reprocessing |
| **Configurable retention** | Default 90 days; override via `onboarding-request.yaml` field `landing_retention_days`. Regulated industries (healthcare: 7 years, finance: 5 years, GDPR: minimize) should set per compliance requirements. |
| **Never read by analysts** | Landing is internal to the platform; users query Raw layer only |
| **Triggers Raw pipeline** | New files in Landing trigger EventBridge → Step Functions → Glue |

---

## S3 Path Convention

```
s3://{data-lake-bucket}/landing/{source_name}/{table_name}/year=YYYY/month=MM/day=DD/{filename}
```

Examples:
- `s3://data-lake-prod/landing/aurora-orders/orders/year=2025/month=07/day=21/dms-cdc-00001.parquet`
- `s3://data-lake-prod/landing/salesforce-contacts/contacts/year=2025/month=07/day=21/appflow-run-abc123.parquet`
- `s3://data-lake-prod/landing/vendor-sftp/invoices/year=2025/month=07/day=21/INV_20250721.csv`

---

## Landing Service Configuration by Source Type

### DMS → S3 Landing

```json
{
  "S3Settings": {
    "BucketName": "{data-lake-bucket}",
    "BucketFolder": "landing/{source_name}/{table_name}",
    "DataFormat": "parquet",
    "ParquetVersion": "parquet-2-0",
    "TimestampColumnName": "_dms_ingestion_timestamp",
    "DatePartitionEnabled": true,
    "DatePartitionSequence": "YYYYMMDD",
    "DatePartitionDelimiter": "SLASH"
  }
}
```

### AppFlow → S3 Landing

```json
{
  "destinationFlowConfigList": [{
    "connectorType": "S3",
    "destinationConnectorProperties": {
      "S3": {
        "bucketName": "{data-lake-bucket}",
        "bucketPrefix": "landing/{source_name}/{object_name}",
        "s3OutputFormatConfig": {
          "fileType": "PARQUET",
          "prefixConfig": {
            "prefixType": "PATH_AND_FILENAME",
            "prefixFormat": "YEAR/MONTH/DAY"
          }
        }
      }
    }
  }]
}
```

### Firehose → S3 Landing

```json
{
  "S3DestinationConfiguration": {
    "BucketARN": "arn:aws:s3:::{data-lake-bucket}",
    "Prefix": "landing/{stream_name}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
    "ErrorOutputPrefix": "landing-errors/{stream_name}/",
    "BufferingHints": {"SizeInMBs": 128, "IntervalInSeconds": 300},
    "CompressionFormat": "UNCOMPRESSED"
  },
  "DataFormatConversionConfiguration": {
    "Enabled": true,
    "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
    "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {"Compression": "SNAPPY"}}}
  }
}
```

### Transfer Family → S3 Landing

Files land directly at:
```
s3://{data-lake-bucket}/landing/{source_name}/{username}/{filename}
```

EventBridge rule reorganizes into date-partitioned structure via a lightweight Lambda or Step Functions task.

---

## Trigger: Landing → Raw Pipeline

When new files appear in Landing, trigger the Raw layer Glue job:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {"name": ["{data-lake-bucket}"]},
    "object": {"key": [{"prefix": "landing/{source_name}/"}]}
  }
}
```

EventBridge rule → Step Functions state machine → Glue job (reads Landing S3 files).

---

## Retention Policy

Landing retention is **configurable per source** via the `landing_retention_days` field in `onboarding-request.yaml`. The power asks about retention during Q7 (sensitivity) if compliance requirements are detected.

**Industry defaults (ask the user, do not assume):**

| Industry / Regulation | Typical Landing Retention | Rationale |
|---|---|---|
| Unregulated / general | 90 days | Reprocessing buffer without cost bloat |
| Financial services (SOX, GLBA) | 7 years (2555 days) | Regulatory audit requirements |
| Healthcare (HIPAA) | 6-7 years (2190-2555 days) | Medical record retention rules |
| GDPR (EU) | Minimize (30-90 days) | Data minimization principle — keep only as long as needed |
| Insurance | 7-10 years | Claims and policy retention requirements |
| Government / public sector | Per agency mandate | Varies by jurisdiction and data classification |
| Startups / non-regulated | 30-90 days | Cost efficiency, no compliance mandate |

**Configuration in onboarding-request.yaml:**
```yaml
retention:
  landing_retention_days: 90     # override per compliance requirements
  raw_retention_days: 2555       # 7 years default for Raw/Bronze
  quarantine_retention_days: 90  # how long to keep quarantined rows
```

| Configuration | Value |
|---|---|
| S3 Lifecycle Rule | Delete objects after `landing_retention_days` (default: 90) |
| Glacier transition | Not recommended (reprocessing needs fast access) |
| Versioning | Disabled (Landing is append-only) |
| Encryption | SSE-S3 (no per-source KMS needed for Landing — PII protection is applied in Raw via Lake Formation) |

```json
{
  "Rules": [{
    "ID": "landing-retention-90d",
    "Status": "Enabled",
    "Filter": {"Prefix": "landing/"},
    "Expiration": {"Days": 90}
  }]
}
```

---

## Why Glue Never Reads from Source Directly

| Problem with direct source reads | Two-layer solution |
|---|---|
| JDBC connection timeouts | Glue reads from S3 — always available |
| Security group complexity | Only DMS/AppFlow need VPC access; Glue needs S3 only |
| Cold-start delays | S3 reads start instantly, no connection negotiation |
| Source system load | Extraction happens via purpose-built service (DMS), not Spark |
| Reprocessing requires re-extraction | Landing is a retention-period buffer — reprocess from S3 |
| Worker count driven by source latency | S3 reads are 2× faster → 50% fewer workers needed |

---

## MUST Rules

- MUST NOT apply any business transforms in Landing
- MUST NOT use Iceberg in Landing (plain S3 only)
- MUST partition by arrival date (year/month/day)
- MUST set lifecycle expiration per `landing_retention_days` in onboarding-request.yaml (default 90 days if not specified; override for regulated industries)
- MUST trigger Raw pipeline via EventBridge on new file arrival
- MUST NOT expose Landing to end users or analysts
- MUST use Parquet format when the ingestion service supports it
- MUST store Landing and Raw in the same S3 bucket (different prefixes) for cost efficiency
