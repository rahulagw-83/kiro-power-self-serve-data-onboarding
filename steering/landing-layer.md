# Landing Layer Design

## Purpose

Get data to S3 **exactly as-is**, as fast and cheaply as possible.

---

## Rules

| Rule | Rationale |
|------|-----------|
| **Data arrives as-is** | No transforms, no type casting, no column renaming, no audit columns |
| **Append-only** | Never overwrite or mutate landed files |
| **Partitioned by arrival date** | `year=YYYY/month=MM/day=DD/` for time-bounded access |
| **Format: service output** | Parquet if service supports it, otherwise source format |
| **Configurable retention** | Default 90 days; override per project/compliance (ask user) |
| **Not queried by analysts** | Landing is staging; profiling report is the interface |

---

## S3 Path Convention

```
s3://{landing-bucket}/{domain}/{source_name}/{table_or_object}/year=YYYY/month=MM/day=DD/{files}
```

Examples:
- `s3://data-landing-prod/sales/aurora-orders/orders/year=2025/month=07/day=21/`
- `s3://data-landing-prod/marketing/salesforce/contacts/year=2025/month=07/day=21/`
- `s3://data-landing-prod/vendor/sftp-invoices/invoices/year=2025/month=07/day=21/`

---

## Service-Specific Landing Configurations

### DMS → S3

```json
{
  "S3Settings": {
    "BucketName": "{landing-bucket}",
    "BucketFolder": "{domain}/{source_name}/{table_name}",
    "DataFormat": "parquet",
    "ParquetVersion": "parquet-2-0",
    "DatePartitionEnabled": true,
    "DatePartitionSequence": "YYYYMMDD",
    "DatePartitionDelimiter": "SLASH"
  }
}
```

### AppFlow → S3

```json
{
  "destinationFlowConfigList": [{
    "connectorType": "S3",
    "destinationConnectorProperties": {
      "S3": {
        "bucketName": "{landing-bucket}",
        "bucketPrefix": "{domain}/{source_name}/{object_name}",
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

### Firehose → S3

```json
{
  "S3DestinationConfiguration": {
    "BucketARN": "arn:aws:s3:::{landing-bucket}",
    "Prefix": "{domain}/{stream_name}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
    "ErrorOutputPrefix": "errors/{stream_name}/",
    "BufferingHints": {"SizeInMBs": 128, "IntervalInSeconds": 300}
  },
  "DataFormatConversionConfiguration": {
    "Enabled": true,
    "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
    "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {"Compression": "SNAPPY"}}}
  }
}
```

### Transfer Family → S3

Files land at: `s3://{landing-bucket}/{domain}/{source_name}/{username}/{filename}`

EventBridge rule detects arrival for downstream profiling trigger.

---

## Retention

Configurable per source via `source-config.yaml`:

```yaml
retention:
  landing_retention_days: 90  # default; override per compliance
```

| Context | Typical Retention |
|---|---|
| General / unregulated | 90 days |
| Financial (SOX, GLBA) | 7 years |
| Healthcare (HIPAA) | 6-7 years |
| GDPR (minimize) | 30-90 days |
| Long-term raw store | Indefinite (no lifecycle rule) |

S3 lifecycle rule:
```json
{
  "Rules": [{
    "ID": "landing-retention",
    "Status": "Enabled",
    "Filter": {"Prefix": "{domain}/{source_name}/"},
    "Expiration": {"Days": 90}
  }]
}
```
