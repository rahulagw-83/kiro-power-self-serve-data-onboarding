# S3 File Ingestion Pipeline

End-to-end workflow for onboarding S3 file sources (CSV, JSON, Parquet) into append-only Iceberg bronze tables using AWS Glue, Step Functions, and EventBridge.

---

## When to Use This Pattern

Use this pattern when:

- **Source data arrives as files in S3** — batch uploads, partner drops, application exports, log files
- **File formats**: CSV, JSON (newline-delimited or multi-line), Parquet
- **Incremental processing** via Glue job bookmarks (process only new files)
- **Write mode**: append-only into Iceberg bronze tables
- **No PII or sensitive data** requiring VPC endpoints or KMS encryption

Do NOT use this pattern when:

- Source is a relational database (use `jdbc-rds-ingestion` steering)
- Data contains PII requiring encryption at rest with CMK (use `jdbc-rds-ingestion` with VPC)
- You need CDC (Change Data Capture) or merge/upsert semantics
- Real-time streaming is required (use Kinesis/MSK patterns instead)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AWS Account                                       │
│                                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │ Source S3 │───▶│ EventBridge  │───▶│Step Functions│                  │
│  │  Bucket   │    │   Rule       │    │  Workflow    │                  │
│  └──────────┘    └──────────────┘    └──────┬───────┘                  │
│                                             │                           │
│                                             ▼                           │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │  Target   │◀──│  Glue ETL    │◀───│ Glue Catalog │                  │
│  │S3 (Bronze)│    │    Job       │    │  (Iceberg)   │                  │
│  └──────────┘    └──────────────┘    └──────────────┘                  │
│       │                 │                                               │
│       │                 ▼                                               │
│       │          ┌──────────────┐                                       │
│       │          │  CloudWatch   │                                       │
│       │          │Metrics/Alarms │                                       │
│       │          └──────────────┘                                       │
│       ▼                                                                 │
│  ┌──────────┐                                                           │
│  │Quarantine│                                                           │
│  │  Table   │                                                           │
│  └──────────┘                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

**Flow Summary:**
1. Files land in the source S3 bucket (via upload, partner drop, or scheduled export)
2. EventBridge detects the `PutObject`/`CompleteMultipartUpload` event
3. Step Functions workflow orchestrates the pipeline execution
4. Glue ETL job reads new files (bookmark-based), validates, transforms, and writes to Iceberg
5. Valid rows go to the bronze Iceberg table; invalid rows go to a quarantine table
6. CloudWatch metrics and alarms provide observability

---

## Step 1: Create the Onboarding Request

Create the file `config/onboarding-request.yaml` to register a new S3 file source:

```yaml
# config/onboarding-request.yaml
# ---
# Onboarding request for S3 file ingestion into the data lake bronze layer.
# Submit this file to initiate the pipeline provisioning workflow.

request:
  id: "REQ-2024-001"                        # Unique request identifier
  submitted_by: "data-team@company.com"      # Requester email
  submitted_at: "2024-01-15T10:00:00Z"      # ISO 8601 timestamp
  priority: "high"                           # low | medium | high | critical

source:
  name: "ecommerce-orders"                   # Logical source name (kebab-case)
  type: "s3-file"                            # s3-file | jdbc-rds | api
  description: "Daily order exports from the e-commerce platform"
  owner: "commerce-team@company.com"
  format: "csv"                              # csv | json | parquet

  connection:
    bucket: "raw-data-landing-zone"          # Source S3 bucket name
    prefix: "orders/daily/"                  # Key prefix for source files
    file_pattern: "orders_*.csv"             # Glob pattern for file matching
    region: "us-east-1"                      # AWS region of source bucket

tables:
  - name: "orders"                           # Target table name
    description: "E-commerce order transactions"
    schedule: "rate(1 hour)"                 # EventBridge schedule expression
    sensitivity: "internal"                  # public | internal | confidential | restricted
    sla:
      freshness_minutes: 60                  # Max acceptable data delay
      availability_percent: 99.5             # Target uptime percentage
    volume:
      estimated_daily_rows: 50000            # Expected daily row count
      estimated_daily_gb: 0.5                # Expected daily data volume in GB
      peak_files_per_hour: 10                # Max files expected per hour
    consumers:
      - team: "analytics"
        use_case: "Daily order reporting"
      - team: "data-science"
        use_case: "Demand forecasting models"

approval:
  data_owner: "commerce-lead@company.com"
  security_review: "pending"                 # pending | approved | rejected
  architecture_review: "pending"

registry_entry:
  catalog_database: "bronze_ecommerce"       # Glue catalog database
  catalog_table: "orders"                    # Glue catalog table name
  tags:
    domain: "commerce"
    cost_center: "CC-1234"
    environment: "production"
```

### Validation Rules for Onboarding Requests

- `request.id` must be unique across all requests
- `source.name` must be kebab-case, 3-50 characters
- `source.format` must be one of: csv, json, parquet
- `connection.bucket` must exist and be accessible by the pipeline role
- `tables[].schedule` must be a valid EventBridge schedule expression
- `tables[].sensitivity` determines encryption and access controls applied

---

## Step 2: Author the Data Contract

Refer to the `data-contracts.md` steering file for full contract authoring guidance.

For S3 file sources, the data contract defines:
- **Schema**: Column names, types, nullability, and constraints
- **Quality rules**: Row-level validations applied during ingestion
- **SLA**: Freshness and completeness expectations

### Sample Data Contract for S3 Source

```yaml
# config/data-contract.yaml
contract:
  name: "ecommerce-orders-contract"
  version: "1.0.0"
  description: "Data contract for e-commerce order ingestion"
  owner: "commerce-team@company.com"

schema:
  format: "spark-ddl"
  columns:
    - name: "order_id"
      type: "string"
      nullable: false
      description: "Unique order identifier"
      constraints:
        - type: "unique"
        - type: "regex"
          pattern: "^ORD-[0-9]{8,12}$"

    - name: "customer_id"
      type: "string"
      nullable: false
      description: "Customer identifier"

    - name: "order_date"
      type: "timestamp"
      nullable: false
      description: "Order placement timestamp (UTC)"
      constraints:
        - type: "range"
          min: "2020-01-01T00:00:00Z"

    - name: "total_amount"
      type: "decimal(12,2)"
      nullable: false
      description: "Order total in USD"
      constraints:
        - type: "range"
          min: 0.01
          max: 999999.99

    - name: "status"
      type: "string"
      nullable: false
      description: "Order status"
      constraints:
        - type: "enum"
          values: ["pending", "confirmed", "shipped", "delivered", "cancelled"]

    - name: "shipping_country"
      type: "string"
      nullable: true
      description: "ISO 3166-1 alpha-2 country code"

quality_rules:
  completeness:
    min_row_count: 1                         # At least 1 row per batch
    max_null_percent:
      order_id: 0
      customer_id: 0
      total_amount: 0

  freshness:
    max_delay_minutes: 60

  validity:
    duplicate_threshold_percent: 1.0         # Max 1% duplicate order_ids

sla:
  availability: 99.5
  freshness_minutes: 60
  notification_channels:
    - type: "sns"
      topic: "arn:aws:sns:us-east-1:123456789012:data-quality-alerts"
```

### Key Sections for S3 Sources

| Section | Purpose |
|---------|---------|
| `schema.columns` | Define expected columns and types for schema enforcement |
| `schema.columns[].constraints` | Per-column validation rules (regex, range, enum) |
| `quality_rules.completeness` | Ensure files are not empty and critical fields are populated |
| `quality_rules.freshness` | Alert if data arrival exceeds SLA |
| `quality_rules.validity` | Detect duplicates and malformed records |

---

## Step 3: Create the Pipeline Configuration

Create `config/pipeline-config.yaml` to define the full pipeline behavior:

```yaml
# config/pipeline-config.yaml
# ---
# Pipeline configuration for S3 file ingestion.
# This file drives Glue job generation and infrastructure deployment.

pipeline:
  name: "ecommerce-orders-ingest"            # Pipeline identifier (kebab-case)
  version: "1.0.0"                           # Semantic version
  description: "Ingest daily e-commerce orders from S3 CSV into Iceberg bronze"
  type: "s3-file"                            # s3-file | jdbc-rds

source:
  type: "s3"
  connection:
    bucket: "raw-data-landing-zone"
    prefix: "orders/daily/"
    file_pattern: "orders_*.csv"
    region: "us-east-1"

  format: "csv"
  format_options:
    csv:
      has_header: true
      delimiter: ","
      quote_char: "\""
      escape_char: "\\"
      encoding: "utf-8"
      null_value: ""
      skip_lines: 0
      multiLine: false
    json:
      multiLine: false
      jsonPath: ""                            # Optional JSONPath for nested structures
    parquet:
      merge_schema: false                    # Merge schemas across Parquet files

  bookmark:
    enabled: true
    transformation_ctx: "s3_source_orders"   # Unique context for bookmark tracking

target:
  bucket: "data-lake-bronze"
  prefix: "ecommerce/orders/"
  database: "bronze_ecommerce"
  table: "orders"

  iceberg:
    format_version: 2
    write_mode: "append"                     # append | overwrite
    table_properties:
      write.format.default: "parquet"
      write.parquet.compression-codec: "zstd"
      write.metadata.delete-after-commit.enabled: "true"
      write.metadata.previous-versions-max: "100"

  partition:
    strategy: "daily"                        # daily | monthly | hourly | none
    column: "order_date"
    transform: "day"                         # day | month | hour | year | identity

  compression: "zstd"                        # zstd | snappy | gzip | lz4

contract_ref: "config/data-contract.yaml"    # Path to data contract

execution:
  glue_version: "4.0"                        # 3.0 | 4.0
  python_version: "3"
  worker_type: "G.1X"                        # G.025X | G.1X | G.2X | G.4X | G.8X
  number_of_workers: 5
  timeout_minutes: 60
  max_retries: 1
  max_concurrent_runs: 1
  job_parameters:
    "--enable-metrics": "true"
    "--enable-continuous-cloudwatch-log": "true"
    "--enable-spark-ui": "true"
    "--spark-event-logs-path": "s3://glue-spark-logs/ecommerce-orders/"
    "--TempDir": "s3://glue-temp/ecommerce-orders/"
    "--enable-glue-datacatalog": "true"
    "--datalake-formats": "iceberg"
  tags:
    pipeline: "ecommerce-orders-ingest"
    domain: "commerce"
    cost_center: "CC-1234"

observability:
  namespace: "DataLake/Ingestion"            # CloudWatch namespace
  sns_topic: "arn:aws:sns:us-east-1:123456789012:pipeline-alerts"
  alarms:
    - name: "FailedRecordsHigh"
      metric: "QuarantinedRecords"
      threshold: 100
      period_seconds: 300
      evaluation_periods: 1
      comparison: "GreaterThanThreshold"
    - name: "JobDurationHigh"
      metric: "JobDurationSeconds"
      threshold: 3600
      period_seconds: 300
      evaluation_periods: 1
      comparison: "GreaterThanThreshold"
    - name: "NoDataReceived"
      metric: "RecordsProcessed"
      threshold: 0
      period_seconds: 3600
      evaluation_periods: 2
      comparison: "LessThanOrEqualToThreshold"

environments:
  dev:
    source:
      connection:
        bucket: "raw-data-landing-zone-dev"
    target:
      bucket: "data-lake-bronze-dev"
      database: "bronze_ecommerce_dev"
    execution:
      number_of_workers: 2
      timeout_minutes: 30

  test:
    source:
      connection:
        bucket: "raw-data-landing-zone-test"
    target:
      bucket: "data-lake-bronze-test"
      database: "bronze_ecommerce_test"
    execution:
      number_of_workers: 3
      timeout_minutes: 45

  prod:
    source:
      connection:
        bucket: "raw-data-landing-zone"
    target:
      bucket: "data-lake-bronze"
      database: "bronze_ecommerce"
    execution:
      number_of_workers: 5
      timeout_minutes: 60
```

---

## Step 4: Generate the Glue ETL Job

The Glue ETL job follows this pipeline flow:

```
Read Source (Bookmark) → Pre-Transform Hook → Schema Cast → Contract Validate
    → Quarantine Invalid → Post-Transform Hook → Audit Columns
    → Deduplicate → Write Iceberg → Emit Metrics
```

### Full Glue Job Code

```python
# glue_job/ingest_s3_to_iceberg.py
# ---
# AWS Glue ETL job for S3 file ingestion into Iceberg bronze tables.
# Generated by the unified-data-ingestion pipeline factory.

import sys
import hashlib
from datetime import datetime, timezone
from typing import Optional

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    sha2, concat_ws, monotonically_increasing_id,
    when, regexp_extract, to_timestamp, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType, LongType, DoubleType
)

import boto3
import json
import logging

# ─── Configuration ───────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_prefix',
    'target_database',
    'target_table',
    'target_bucket',
    'target_prefix',
    'contract_path',
    'environment',
    'batch_id'
])

JOB_NAME = args['JOB_NAME']
SOURCE_BUCKET = args['source_bucket']
SOURCE_PREFIX = args['source_prefix']
TARGET_DATABASE = args['target_database']
TARGET_TABLE = args['target_table']
TARGET_BUCKET = args['target_bucket']
TARGET_PREFIX = args['target_prefix']
CONTRACT_PATH = args.get('contract_path', '')
ENVIRONMENT = args.get('environment', 'dev')
BATCH_ID = args.get('batch_id', datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S'))

QUARANTINE_TABLE = f"{TARGET_TABLE}_quarantine"
METRICS_NAMESPACE = "DataLake/Ingestion"


# ─── Spark & Glue Initialization ────────────────────────────────────────────

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Configure Spark for Iceberg with Glue Catalog
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

cloudwatch = boto3.client('cloudwatch')
```

```python
# ─── Custom Logic Hooks ──────────────────────────────────────────────────────

def pre_transform(df: DataFrame) -> DataFrame:
    """
    Hook for custom pre-validation transformations.
    Called after source read, before schema casting and contract validation.

    Common uses:
    - Filter out header/footer rows in CSV
    - Rename columns from source naming to target naming
    - Parse nested JSON fields
    - Apply source-specific business logic

    Args:
        df: Raw DataFrame from source read
    Returns:
        Transformed DataFrame ready for schema casting
    """
    # Example: Strip whitespace from string columns
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


def post_transform(df: DataFrame) -> DataFrame:
    """
    Hook for custom post-validation transformations.
    Called after contract validation and quarantine routing, before audit columns.

    Common uses:
    - Derive computed columns
    - Apply business rules that don't affect validation
    - Enrich data with lookup values

    Args:
        df: Validated DataFrame (only valid rows)
    Returns:
        Enriched DataFrame ready for audit column injection
    """
    # Example: No-op pass-through
    return df


# ─── Source Read with Job Bookmark ───────────────────────────────────────────

def read_source() -> DynamicFrame:
    """
    Read source files from S3 using Glue job bookmarks for incremental processing.
    Only new files since the last successful run are processed.
    """
    source_path = f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"
    logger.info(f"Reading source from: {source_path}")

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ",",
            "quoteChar": "\"",
            "escaper": "\\",
            "optimizePerformance": True
        },
        connection_options={
            "paths": [source_path],
            "recurse": True,
            "groupFiles": "inPartition",
            "groupSize": "134217728"          # 128 MB group size
        },
        transformation_ctx="s3_source_orders"  # CRITICAL: enables job bookmark
    )

    record_count = dynamic_frame.count()
    logger.info(f"Read {record_count} records from source")
    emit_metric("SourceRecordsRead", record_count)

    return dynamic_frame


# ─── Schema Casting ──────────────────────────────────────────────────────────

def cast_schema(df: DataFrame) -> DataFrame:
    """
    Cast columns to their target data types as defined in the data contract.
    Returns DataFrame with proper types for downstream validation.
    """
    return df.select(
        col("order_id").cast(StringType()).alias("order_id"),
        col("customer_id").cast(StringType()).alias("customer_id"),
        to_timestamp(col("order_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_date"),
        col("total_amount").cast(DecimalType(12, 2)).alias("total_amount"),
        col("status").cast(StringType()).alias("status"),
        col("shipping_country").cast(StringType()).alias("shipping_country")
    )
```

```python
# ─── Contract Validation ─────────────────────────────────────────────────────

def validate_contract(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Apply data contract validation rules to each row.
    Returns a tuple of (valid_df, invalid_df).

    Validation rules applied:
    1. NOT NULL checks on required columns
    2. Type casting success (non-null after cast)
    3. Range constraints (min/max values)
    4. Enum constraints (allowed values)
    5. Regex pattern constraints
    """
    # Define validation conditions
    valid_condition = (
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_date").isNotNull() &
        col("total_amount").isNotNull() &
        (col("total_amount") > 0) &
        (col("total_amount") < 999999.99) &
        col("status").isin("pending", "confirmed", "shipped", "delivered", "cancelled")
    )

    # Tag rows with validation result
    df_tagged = df.withColumn("_is_valid", valid_condition)

    # Build rejection reason for invalid rows
    df_tagged = df_tagged.withColumn(
        "_rejection_reason",
        when(col("order_id").isNull(), lit("order_id is null"))
        .when(col("customer_id").isNull(), lit("customer_id is null"))
        .when(col("order_date").isNull(), lit("order_date is null or unparseable"))
        .when(col("total_amount").isNull(), lit("total_amount is null or non-numeric"))
        .when(col("total_amount") <= 0, lit("total_amount must be positive"))
        .when(col("total_amount") >= 999999.99, lit("total_amount exceeds maximum"))
        .when(~col("status").isin("pending", "confirmed", "shipped", "delivered", "cancelled"),
              lit("status not in allowed values"))
        .otherwise(lit(None))
    )

    valid_df = df_tagged.filter(col("_is_valid") == True).drop("_is_valid", "_rejection_reason")
    invalid_df = df_tagged.filter(col("_is_valid") == False).drop("_is_valid")

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")
    emit_metric("ValidRecords", valid_count)
    emit_metric("QuarantinedRecords", invalid_count)

    return valid_df, invalid_df


# ─── Quarantine Routing ──────────────────────────────────────────────────────

def write_quarantine(invalid_df: DataFrame) -> None:
    """
    Write invalid/rejected records to the quarantine Iceberg table.
    Includes rejection reason and batch metadata for investigation.
    """
    if invalid_df.count() == 0:
        logger.info("No records to quarantine")
        return

    quarantine_df = invalid_df.withColumn(
        "_quarantine_timestamp", current_timestamp()
    ).withColumn(
        "_batch_id", lit(BATCH_ID)
    ).withColumn(
        "_source_file", input_file_name()
    ).withColumn(
        "_pipeline_name", lit(JOB_NAME)
    )

    quarantine_df.writeTo(
        f"glue_catalog.{TARGET_DATABASE}.{QUARANTINE_TABLE}"
    ).option(
        "fanout-enabled", "true"
    ).append()

    logger.info(f"Wrote {invalid_df.count()} records to quarantine table")
```

```python
# ─── Audit Column Injection ──────────────────────────────────────────────────

def inject_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit/lineage columns to every row for traceability.

    Columns added:
    - _ingestion_timestamp: When the record was ingested (UTC)
    - _source_file: Original S3 file path
    - _row_hash: SHA-256 hash of business key columns for deduplication
    - _batch_id: Unique identifier for this pipeline run
    - _pipeline_name: Name of the ingestion pipeline
    """
    return df.withColumn(
        "_ingestion_timestamp", current_timestamp()
    ).withColumn(
        "_source_file", input_file_name()
    ).withColumn(
        "_row_hash", sha2(
            concat_ws("||",
                col("order_id"),
                col("customer_id"),
                col("order_date").cast(StringType())
            ), 256
        )
    ).withColumn(
        "_batch_id", lit(BATCH_ID)
    ).withColumn(
        "_pipeline_name", lit(JOB_NAME)
    )


# ─── Deduplication ───────────────────────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records within the current batch based on business key.
    Uses _row_hash for efficient comparison. Keeps the first occurrence.
    """
    initial_count = df.count()
    deduped_df = df.dropDuplicates(["_row_hash"])
    final_count = deduped_df.count()

    duplicates_removed = initial_count - final_count
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records")
        emit_metric("DuplicatesRemoved", duplicates_removed)

    return deduped_df


# ─── Write to Iceberg ────────────────────────────────────────────────────────

def write_iceberg(df: DataFrame) -> None:
    """
    Write validated, enriched records to the target Iceberg table.

    Uses append mode with:
    - Parquet file format
    - ZSTD compression
    - Fanout writes for partitioned tables
    - Format version 2 for row-level deletes support
    """
    record_count = df.count()

    if record_count == 0:
        logger.info("No records to write to target table")
        return

    logger.info(f"Writing {record_count} records to Iceberg table")

    df.writeTo(
        f"glue_catalog.{TARGET_DATABASE}.{TARGET_TABLE}"
    ).tableProperty(
        "format-version", "2"
    ).tableProperty(
        "write.format.default", "parquet"
    ).tableProperty(
        "write.parquet.compression-codec", "zstd"
    ).option(
        "fanout-enabled", "true"
    ).append()

    emit_metric("RecordsWritten", record_count)
    logger.info(f"Successfully wrote {record_count} records to {TARGET_DATABASE}.{TARGET_TABLE}")


# ─── Metrics Emission ────────────────────────────────────────────────────────

def emit_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    """Emit a CloudWatch metric for pipeline observability."""
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[{
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Dimensions": [
                    {"Name": "Pipeline", "Value": JOB_NAME},
                    {"Name": "Environment", "Value": ENVIRONMENT},
                    {"Name": "Table", "Value": TARGET_TABLE}
                ]
            }]
        )
    except Exception as e:
        logger.warning(f"Failed to emit metric {metric_name}: {e}")
```

```python
# ─── Main Pipeline Orchestration ─────────────────────────────────────────────

def main():
    """Execute the full ingestion pipeline."""
    import time
    start_time = time.time()

    try:
        logger.info(f"Starting pipeline: {JOB_NAME}, batch: {BATCH_ID}")

        # Step 1: Read source with job bookmark
        source_dyf = read_source()

        if source_dyf.count() == 0:
            logger.info("No new data to process (bookmark up to date)")
            emit_metric("RecordsProcessed", 0)
            job.commit()
            return

        # Step 2: Convert to DataFrame for Spark SQL operations
        source_df = source_dyf.toDF()

        # Step 3: Apply pre-transform hook
        transformed_df = pre_transform(source_df)

        # Step 4: Cast to target schema types
        casted_df = cast_schema(transformed_df)

        # Step 5: Validate against data contract
        valid_df, invalid_df = validate_contract(casted_df)

        # Step 6: Route invalid records to quarantine
        write_quarantine(invalid_df)

        # Step 7: Apply post-transform hook
        enriched_df = post_transform(valid_df)

        # Step 8: Inject audit columns
        audited_df = inject_audit_columns(enriched_df)

        # Step 9: Deduplicate within batch
        final_df = deduplicate(audited_df)

        # Step 10: Write to Iceberg bronze table
        write_iceberg(final_df)

        # Emit duration metric
        duration = time.time() - start_time
        emit_metric("JobDurationSeconds", duration, "Seconds")
        emit_metric("RecordsProcessed", final_df.count())

        logger.info(f"Pipeline completed in {duration:.1f}s")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        emit_metric("PipelineFailures", 1)
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
```

---

## Step 5: Deploy Infrastructure

Refer to the `cdk-infrastructure.md` steering file for full CDK stack details.

### Quick Deployment

```bash
# Deploy all resources for the pipeline
cd infra/
cdk deploy EcommerceOrdersIngestionStack \
  --context environment=dev \
  --context pipeline_name=ecommerce-orders-ingest \
  --require-approval broadening

# Or deploy with the helper script
./deploy.sh dev ecommerce-orders-ingest
```

### Resources Created

| Resource | Type | Purpose |
|----------|------|---------|
| Source S3 Bucket | `aws_s3_bucket` | Landing zone for raw files (if not existing) |
| Target S3 Bucket | `aws_s3_bucket` | Bronze Iceberg table storage |
| Glue Database | `aws_glue_database` | Catalog namespace for bronze tables |
| Glue Table (Iceberg) | `aws_glue_table` | Iceberg table metadata in Glue Catalog |
| Quarantine Table | `aws_glue_table` | Iceberg table for rejected records |
| Glue Job | `aws_glue_job` | ETL job executing the pipeline |
| IAM Role | `aws_iam_role` | Execution role with least-privilege permissions |
| Step Functions | `aws_sfn_state_machine` | Pipeline orchestration workflow |
| EventBridge Rule | `aws_events_rule` | Trigger on S3 file arrival or schedule |
| CloudWatch Alarms | `aws_cloudwatch_alarm` | Alerting on failures and SLA breaches |
| SNS Topic | `aws_sns_topic` | Notification delivery for alerts |
| S3 Event Notification | `aws_s3_notification` | Route S3 events to EventBridge |

### IAM Policy (Least Privilege)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSourceBucket",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::raw-data-landing-zone",
        "arn:aws:s3:::raw-data-landing-zone/orders/daily/*"
      ]
    },
    {
      "Sid": "WriteTargetBucket",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::data-lake-bronze",
        "arn:aws:s3:::data-lake-bronze/ecommerce/orders/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
        "glue:UpdateTable", "glue:CreatePartition", "glue:BatchCreatePartition"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:123456789012:catalog",
        "arn:aws:glue:us-east-1:123456789012:database/bronze_ecommerce",
        "arn:aws:glue:us-east-1:123456789012:table/bronze_ecommerce/*"
      ]
    },
    {
      "Sid": "CloudWatchMetrics",
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData"],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "cloudwatch:namespace": "DataLake/Ingestion"
        }
      }
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/*"
    }
  ]
}
```

---

## Step 6: Run and Validate

### Upload Sample Data

```bash
# Upload test CSV file to source bucket
aws s3 cp test/sample_orders.csv \
  s3://raw-data-landing-zone-dev/orders/daily/orders_20240115_001.csv

# Verify file landed
aws s3 ls s3://raw-data-landing-zone-dev/orders/daily/ --human-readable
```

### Upload Glue Script

```bash
# Upload the generated Glue job script
aws s3 cp glue_job/ingest_s3_to_iceberg.py \
  s3://glue-scripts-bucket/ecommerce-orders/ingest_s3_to_iceberg.py
```

### Execute Pipeline

**Option A: Via Step Functions (recommended)**

```bash
# Start the Step Functions execution
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:ecommerce-orders-ingest-dev" \
  --name "manual-run-$(date +%Y%m%d%H%M%S)" \
  --input '{
    "batch_id": "MANUAL-20240115-001",
    "environment": "dev"
  }'

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:us-east-1:123456789012:execution:ecommerce-orders-ingest-dev:manual-run-20240115"
```

**Option B: Direct Glue Job Run**

```bash
# Start the Glue job directly (bypasses Step Functions orchestration)
aws glue start-job-run \
  --job-name "ecommerce-orders-ingest-dev" \
  --arguments '{
    "--source_bucket": "raw-data-landing-zone-dev",
    "--source_prefix": "orders/daily/",
    "--target_database": "bronze_ecommerce_dev",
    "--target_table": "orders",
    "--target_bucket": "data-lake-bronze-dev",
    "--target_prefix": "ecommerce/orders/",
    "--environment": "dev",
    "--batch_id": "MANUAL-20240115-001"
  }'

# Monitor job run
aws glue get-job-run \
  --job-name "ecommerce-orders-ingest-dev" \
  --run-id "<run-id-from-start-job-run>"
```

### Validate in Athena

```sql
-- 1. Check main table row count and latest data
SELECT COUNT(*) as total_rows,
       MAX(_ingestion_timestamp) as latest_ingestion,
       COUNT(DISTINCT _batch_id) as batch_count
FROM bronze_ecommerce_dev.orders;

-- 2. Check quarantine table for rejected records
SELECT _rejection_reason,
       COUNT(*) as rejected_count,
       _batch_id
FROM bronze_ecommerce_dev.orders_quarantine
GROUP BY _rejection_reason, _batch_id
ORDER BY rejected_count DESC;

-- 3. Verify row counts match expectations
SELECT _batch_id,
       COUNT(*) as rows_in_batch,
       MIN(order_date) as earliest_order,
       MAX(order_date) as latest_order
FROM bronze_ecommerce_dev.orders
WHERE _batch_id = 'MANUAL-20240115-001'
GROUP BY _batch_id;

-- 4. Check for duplicates (should be 0)
SELECT _row_hash, COUNT(*) as occurrences
FROM bronze_ecommerce_dev.orders
WHERE _batch_id = 'MANUAL-20240115-001'
GROUP BY _row_hash
HAVING COUNT(*) > 1;

-- 5. Validate partition structure
SELECT "$path" as file_path, COUNT(*) as records
FROM bronze_ecommerce_dev.orders
WHERE _batch_id = 'MANUAL-20240115-001'
GROUP BY "$path"
LIMIT 10;
```

### Check CloudWatch Metrics

```bash
# Get pipeline metrics for the last hour
aws cloudwatch get-metric-statistics \
  --namespace "DataLake/Ingestion" \
  --metric-name "RecordsProcessed" \
  --dimensions Name=Pipeline,Value=ecommerce-orders-ingest Name=Environment,Value=dev \
  --start-time "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
  --period 300 \
  --statistics Sum
```

---

## CSV/JSON/Parquet Format Considerations

### Format Settings Reference

| Setting | CSV | JSON | Parquet | Description |
|---------|-----|------|---------|-------------|
| `has_header` | Yes | N/A | N/A | First row contains column names |
| `delimiter` | `,` `;` `\t` `\|` | N/A | N/A | Column separator character |
| `quote_char` | `"` `'` | N/A | N/A | Character for quoting fields |
| `escape_char` | `\\` | N/A | N/A | Escape character within quoted fields |
| `encoding` | `utf-8` `latin1` | `utf-8` | N/A | File character encoding |
| `null_value` | `""` `NULL` `\\N` | N/A | N/A | String representing null values |
| `multiLine` | Yes | Yes | N/A | Records span multiple lines |
| `merge_schema` | N/A | N/A | Yes | Merge schemas across files |
| `compression` | N/A | N/A | Detected | Auto-detected for Parquet |

### CSV Reader Configuration

```python
# CSV with custom delimiter and quoting
csv_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": "|",              # Pipe-delimited
        "quoteChar": "\"",
        "escaper": "\\",
        "multiLine": False,
        "optimizePerformance": True,
        "skipFirst": 0                 # Skip N header lines (beyond the column header)
    },
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"],
        "recurse": True,
        "groupFiles": "inPartition",
        "groupSize": "134217728"
    },
    transformation_ctx="csv_source"
)
```

### JSON Reader Configuration

```python
# Newline-delimited JSON (NDJSON)
json_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={
        "multiLine": False,            # True for pretty-printed JSON arrays
        "jsonPath": "",                # Optional: JSONPath to extract nested records
        "optimizePerformance": True
    },
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"],
        "recurse": True,
        "groupFiles": "inPartition",
        "groupSize": "134217728"
    },
    transformation_ctx="json_source"
)

# Multi-line JSON (array of objects)
json_multiline_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={
        "multiLine": True,
        "jsonPath": "$.records[*]"     # Extract from nested path
    },
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"],
        "recurse": True
    },
    transformation_ctx="json_multiline_source"
)
```

### Parquet Reader Configuration

```python
# Parquet files (schema auto-detected)
parquet_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    format_options={
        "mergeSchema": False           # True to union schemas across files
    },
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"],
        "recurse": True,
        "groupFiles": "inPartition",
        "groupSize": "268435456"       # 256 MB for Parquet (larger files)
    },
    transformation_ctx="parquet_source"
)

# Parquet with schema merge (evolving schemas across files)
parquet_merged_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    format_options={
        "mergeSchema": True
    },
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}"],
        "recurse": True
    },
    transformation_ctx="parquet_merged_source"
)
```

### Format-Specific Pre-Transform Examples

```python
# CSV: Handle files with varying column counts
def pre_transform_csv(df: DataFrame) -> DataFrame:
    """Handle CSVs where trailing columns may be absent."""
    expected_columns = ["order_id", "customer_id", "order_date",
                       "total_amount", "status", "shipping_country"]
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(StringType()))
    return df.select(expected_columns)


# JSON: Flatten nested structures
def pre_transform_json(df: DataFrame) -> DataFrame:
    """Flatten nested JSON into flat columns."""
    return df.select(
        col("order.id").alias("order_id"),
        col("customer.id").alias("customer_id"),
        col("order.created_at").alias("order_date"),
        col("order.total.amount").alias("total_amount"),
        col("order.status").alias("status"),
        col("shipping.address.country_code").alias("shipping_country")
    )


# Parquet: Handle schema evolution
def pre_transform_parquet(df: DataFrame) -> DataFrame:
    """Normalize column names and handle schema differences."""
    # Rename columns from camelCase to snake_case
    for col_name in df.columns:
        snake_name = ''.join(['_' + c.lower() if c.isupper() else c
                             for c in col_name]).lstrip('_')
        if snake_name != col_name:
            df = df.withColumnRenamed(col_name, snake_name)
    return df
```

---

## Checklist

Complete this checklist before promoting the pipeline to production:

- [ ] **Onboarding request** — `config/onboarding-request.yaml` reviewed and approved
- [ ] **Data contract** — `config/data-contract.yaml` defines schema, quality rules, and SLA
- [ ] **Pipeline config** — `config/pipeline-config.yaml` has correct source/target/execution settings
- [ ] **Glue job tested** — Job runs successfully in dev with sample data
- [ ] **Job bookmark verified** — Re-running the job processes only new files (no duplicates)
- [ ] **Quarantine validated** — Invalid records route to quarantine table with rejection reasons
- [ ] **Audit columns present** — `_ingestion_timestamp`, `_source_file`, `_row_hash`, `_batch_id` populated
- [ ] **Iceberg partitioning** — Target table partitioned correctly (verify with `SHOW CREATE TABLE`)
- [ ] **CloudWatch alarms** — Alarms configured for failures, SLA breaches, and zero-data conditions
- [ ] **IAM least privilege** — Execution role has only required permissions (no `*` resources in prod)
- [ ] **Multi-environment** — Pipeline deployed and tested in dev → test → prod progression
- [ ] **Runbook documented** — On-call team has documentation for alert response and manual re-runs

---

## Troubleshooting

### Common Issues

| Symptom | Cause | Resolution |
|---------|-------|------------|
| Job processes 0 records | Bookmark already past all files | Reset bookmark: `aws glue reset-job-bookmark --job-name <name>` |
| Schema mismatch errors | CSV header changed or column order differs | Update `cast_schema()` and data contract |
| Quarantine rate > 10% | Source data quality degraded | Contact source owner; check for upstream changes |
| Iceberg write failures | Table not found in catalog | Verify database/table exist: `SHOW TABLES IN <database>` |
| OOM errors | Files too large for worker type | Increase `worker_type` to G.2X or increase `number_of_workers` |
| Slow performance | Too many small files | Enable `groupFiles` and increase `groupSize` in connection options |
| Duplicate records | Bookmark reset without clearing target | Use deduplication or run `DELETE FROM` before re-processing |

### Reset and Re-run

```bash
# Reset the job bookmark (reprocesses all files from scratch)
aws glue reset-job-bookmark --job-name "ecommerce-orders-ingest-dev"

# Delete records from a specific batch (if re-processing needed)
# Run in Athena:
# DELETE FROM bronze_ecommerce_dev.orders WHERE _batch_id = 'BATCH-TO-REMOVE'

# Force re-run with specific files only
aws glue start-job-run \
  --job-name "ecommerce-orders-ingest-dev" \
  --arguments '{
    "--source_prefix": "orders/daily/orders_20240115_001.csv",
    "--batch_id": "RERUN-20240115-001"
  }'
```
