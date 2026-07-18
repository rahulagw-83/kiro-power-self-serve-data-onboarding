# JDBC / RDS Ingestion Pipeline

End-to-end workflow for onboarding PostgreSQL, MySQL, SQL Server, or Oracle databases via JDBC into Iceberg bronze tables with watermark-based incremental extraction, MERGE INTO (upsert), PII handling, and Lake Formation column-level security.

---

## When to Use This Pattern

Use this pattern when:

- **Source is a relational database** accessible via JDBC (PostgreSQL, MySQL, SQL Server, Oracle)
- **Data changes over time** and requires CDC or watermark-based incremental extraction
- **Source contains PII fields** needing Lake Formation column-level masking or redaction
- **Credentials are managed via AWS Secrets Manager** (no hardcoded passwords)
- **Network isolation is required** (VPC + Security Groups for private RDS/Aurora instances)
- **Write mode is MERGE INTO** (upsert by merge key for idempotent, deduplication-safe loads)
- **Audit trail is mandatory** (row-level lineage with ingestion timestamps and source hashes)
- **Multi-environment promotion** is needed (dev → staging → prod with isolated configs)

Do NOT use this pattern when:
- Source is a flat file on S3 (use the S3 file ingestion pattern instead)
- Source is a streaming system like Kafka or Kinesis (use streaming ingestion)
- Full table reload on every run is acceptable and table is small (simple batch may suffice)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS Account                                     │
│                                                                             │
│  ┌──────────────┐     ┌──────────────────────────────────────────────────┐ │
│  │  EventBridge │────▶│           Step Functions Orchestrator             │ │
│  │  (Schedule)  │     └──────────┬───────────────────────────────────────┘ │
│  └──────────────┘                │                                         │
│                                  ▼                                         │
│  ┌────────────────────────────────────────────────────────────────┐       │
│  │                        VPC (Private Subnets)                    │       │
│  │                                                                 │       │
│  │  ┌────────────┐    ┌─────────────────┐    ┌───────────────┐   │       │
│  │  │ RDS/Aurora │───▶│   Glue ETL Job  │───▶│  S3 (Iceberg) │   │       │
│  │  │ (Source DB)│    │  (JDBC Reader)  │    │  Bronze Table  │   │       │
│  │  └────────────┘    └────────┬────────┘    └───────────────┘   │       │
│  │                             │                                   │       │
│  └─────────────────────────────┼───────────────────────────────────┘       │
│                                │                                           │
│  ┌──────────────┐   ┌─────────▼────────┐   ┌──────────────────┐          │
│  │   Secrets    │   │    DynamoDB      │   │  Lake Formation  │          │
│  │   Manager    │   │  (Watermarks)    │   │  (Column ACLs)   │          │
│  └──────────────┘   └──────────────────┘   └──────────────────┘          │
│                                                                             │
│  ┌──────────────┐   ┌──────────────────┐   ┌──────────────────┐          │
│  │     KMS      │   │   CloudWatch     │   │   SNS (Alerts)   │          │
│  │ (Encryption) │   │   (Metrics/Logs) │   │                  │          │
│  └──────────────┘   └──────────────────┘   └──────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Differences from S3 Pattern

| Aspect              | S3 File Pattern                     | JDBC / RDS Pattern                          |
|---------------------|-------------------------------------|---------------------------------------------|
| Source              | S3 bucket (CSV, JSON, Parquet)      | Relational DB via JDBC                      |
| Incremental Method  | S3 event / manifest / modified date | Watermark column (updated_at, id)           |
| Write Mode          | APPEND or OVERWRITE                 | MERGE INTO (upsert by merge key)            |
| Credentials         | IAM role (no secrets)               | Secrets Manager (username/password)         |
| Network             | S3 VPC endpoint (optional)          | VPC + Security Groups (required)            |
| PII Handling        | Column tagging (optional)           | Lake Formation column-level security        |
| Encryption          | SSE-S3 / SSE-KMS on target         | TLS in-transit + SSE-KMS at rest            |
| Access Control      | S3 bucket policy + IAM             | Lake Formation grants + IAM                 |
| Approval            | Data steward only                   | Data steward + DBA + Security               |

---

## Step 1: Create the Onboarding Request

Create `onboarding-request.yaml` in the pipeline directory:

```yaml
# onboarding-request.yaml
apiVersion: ingestion/v1
kind: OnboardingRequest
metadata:
  name: ecommerce-orders-rds
  team: data-platform
  owner: data-engineering@company.com
  created: "2024-01-15"

source:
  name: ecommerce-orders
  type: jdbc
  engine: postgresql          # postgresql | mysql | sqlserver | oracle
  description: "E-commerce order transactions from production PostgreSQL"
  classification: confidential
  data_domain: commerce

connection:
  host: prod-orders.cluster-xxxx.us-east-1.rds.amazonaws.com
  port: 5432
  database: ecommerce
  schema: public
  ssl_mode: verify-full       # disable | require | verify-ca | verify-full
  secret_arn: arn:aws:secretsmanager:us-east-1:111122223333:secret:prod/ecommerce/readonly
  vpc_id: vpc-0abc123def456
  subnet_ids:
    - subnet-0aaa111bbb222
    - subnet-0ccc333ddd444
  security_group_ids:
    - sg-0eee555fff666

tables:
  - name: orders
    watermark_column: updated_at    # Column used for incremental extraction
    watermark_type: timestamp       # timestamp | integer | date
    merge_key:
      - order_id                    # Primary key for MERGE INTO (upsert)
    estimated_rows: 50000000
    fetch_size: 10000
    partition_column: order_id      # Column for parallel JDBC reads

  - name: order_items
    watermark_column: updated_at
    watermark_type: timestamp
    merge_key:
      - order_id
      - item_id
    estimated_rows: 200000000
    fetch_size: 5000
    partition_column: item_id

sensitivity:
  contains_pii: true
  pii_fields:
    - customer_email
    - customer_name
    - shipping_address
    - phone_number
  retention_days: 2555          # 7 years for financial data
  encryption_required: true
  encryption_key_arn: arn:aws:kms:us-east-1:111122223333:key/mrk-xxxx

approval:
  required_approvals:
    - role: data-steward
      team: data-governance
    - role: dba
      team: database-admin
    - role: security-reviewer
      team: infosec
  status: pending
```

---

## Step 2: Author the Data Contract (with PII Tags)

Create `data-contract.yaml` with Lake Formation tags for PII columns:

```yaml
# data-contract.yaml
apiVersion: ingestion/v1
kind: DataContract
metadata:
  name: ecommerce-orders-contract
  version: "1.0.0"
  source: ecommerce-orders-rds

schema:
  tables:
    - name: orders
      columns:
        - name: order_id
          type: bigint
          nullable: false
          description: "Unique order identifier"
          tags: {}

        - name: customer_email
          type: string
          nullable: false
          description: "Customer email address"
          tags:
            pii: "true"
            sensitivity: "high"
            lf_tag: "pii:email"

        - name: customer_name
          type: string
          nullable: true
          description: "Customer full name"
          tags:
            pii: "true"
            sensitivity: "high"
            lf_tag: "pii:name"

        - name: shipping_address
          type: string
          nullable: true
          description: "Shipping address"
          tags:
            pii: "true"
            sensitivity: "high"
            lf_tag: "pii:address"

        - name: phone_number
          type: string
          nullable: true
          description: "Customer phone"
          tags:
            pii: "true"
            sensitivity: "high"
            lf_tag: "pii:phone"

        - name: order_total
          type: decimal(12,2)
          nullable: false
          description: "Total order amount"
          tags: {}

        - name: order_status
          type: string
          nullable: false
          description: "Current order status"
          allowed_values: [pending, confirmed, shipped, delivered, cancelled]

        - name: created_at
          type: timestamp
          nullable: false
          description: "Order creation timestamp"

        - name: updated_at
          type: timestamp
          nullable: false
          description: "Last modification timestamp"

quality:
  rules:
    - column: order_id
      check: not_null
    - column: order_total
      check: range
      min: 0
      max: 1000000
    - column: order_status
      check: allowed_values
    - column: customer_email
      check: regex
      pattern: "^[^@]+@[^@]+\\.[^@]+$"

security:
  access_policy:
    full_access:
      roles:
        - data-science-role
        - data-engineering-role
      columns: ALL
    restricted_access:
      roles:
        - analytics-role
        - reporting-role
      columns: EXCLUDE_PII    # Excludes columns tagged pii:true
    no_access:
      roles:
        - public-role
      columns: NONE

freshness:
  max_delay_minutes: 60
  alert_channel: sns:data-quality-alerts
```

---

## Step 3: Create the Pipeline Configuration

Create `pipeline-config.yaml`:

```yaml
# pipeline-config.yaml
apiVersion: ingestion/v1
kind: PipelineConfig
metadata:
  name: ecommerce-orders-jdbc
  version: "1.0.0"

connection:
  engine: postgresql
  host: prod-orders.cluster-xxxx.us-east-1.rds.amazonaws.com
  port: 5432
  database: ecommerce
  schema: public
  secret_arn: arn:aws:secretsmanager:us-east-1:111122223333:secret:prod/ecommerce/readonly
  glue_connection_name: ecommerce-orders-conn
  ssl_mode: verify-full
  jdbc_url_template: "jdbc:postgresql://{host}:{port}/{database}?ssl=true&sslmode=verify-full"
  connection_properties:
    loginTimeout: "30"
    socketTimeout: "300"
    connectTimeout: "30"

tables:
  - name: orders
    source_schema: public
    source_table: orders
    watermark_column: updated_at
    watermark_type: timestamp
    merge_key:
      - order_id
    fetch_size: 10000
    partition_column: order_id
    num_partitions: 8
    lower_bound: 1
    upper_bound: 100000000
    custom_query: null          # Override with custom SQL if needed
    pre_transform:
      - action: cast
        column: order_total
        to_type: decimal(12,2)
      - action: trim
        columns: [customer_email, customer_name]
    post_transform:
      - action: hash
        columns: [customer_email, customer_name, shipping_address, phone_number]
        algorithm: sha256
        output_suffix: _hash
    validation:
      not_null: [order_id, customer_email, order_total, order_status, updated_at]
      unique: [order_id]
      range:
        order_total: { min: 0, max: 1000000 }

  - name: order_items
    source_schema: public
    source_table: order_items
    watermark_column: updated_at
    watermark_type: timestamp
    merge_key:
      - order_id
      - item_id
    fetch_size: 5000
    partition_column: item_id
    num_partitions: 16
    lower_bound: 1
    upper_bound: 500000000

target:
  catalog: glue_catalog
  database: bronze_ecommerce
  table_prefix: ""
  format: iceberg
  location: s3://data-lake-bronze-111122223333/ecommerce/
  write_mode: merge            # merge | append | overwrite
  partition_by:
    - column: ingestion_date
      transform: day
  table_properties:
    write.format.default: parquet
    write.parquet.compression-codec: zstd
    write.metadata.delete-after-commit.enabled: "true"
    write.metadata.previous-versions-max: "3"

execution:
  schedule: "rate(1 hour)"
  timeout_minutes: 45
  max_retries: 3
  retry_delay_minutes: 5
  glue_version: "4.0"
  worker_type: G.1X
  num_workers: 10
  max_workers: 50              # Auto-scaling upper bound
  job_bookmark: disabled       # Using watermark instead
  extra_py_files: []
  extra_jars: []

network:
  vpc_id: vpc-0abc123def456
  subnet_ids:
    - subnet-0aaa111bbb222
    - subnet-0ccc333ddd444
  security_group_ids:
    - sg-0eee555fff666
  availability_zones:
    - us-east-1a
    - us-east-1b

observability:
  cloudwatch_log_group: /aws/glue/ecommerce-orders-jdbc
  metrics_namespace: DataPipeline/JDBC
  alarm_topic_arn: arn:aws:sns:us-east-1:111122223333:data-pipeline-alerts
  freshness_sla_minutes: 60
  row_count_anomaly_threshold: 0.5    # Alert if rows differ >50% from avg

environments:
  dev:
    connection:
      host: dev-orders.cluster-yyyy.us-east-1.rds.amazonaws.com
      secret_arn: arn:aws:secretsmanager:us-east-1:111122223333:secret:dev/ecommerce/readonly
    target:
      location: s3://data-lake-bronze-dev-111122223333/ecommerce/
    execution:
      num_workers: 2
      schedule: "rate(4 hours)"
  staging:
    connection:
      host: stg-orders.cluster-zzzz.us-east-1.rds.amazonaws.com
      secret_arn: arn:aws:secretsmanager:us-east-1:111122223333:secret:stg/ecommerce/readonly
    target:
      location: s3://data-lake-bronze-stg-111122223333/ecommerce/
    execution:
      num_workers: 5
      schedule: "rate(2 hours)"
```

---

## Step 4: Generate the Glue ETL Job

### Pipeline Flow

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│     Get      │──▶│     Get      │──▶│  JDBC Read   │──▶│    Pre-      │
│ Credentials  │   │  Watermark   │   │ (Incremental)│   │  Transform   │
└──────────────┘   └──────────────┘   └──────────────┘   └──────┬───────┘
                                                                  │
┌──────────────┐   ┌──────────────┐   ┌──────────────┐          │
│  Quarantine  │◀──│   Validate   │◀──────────────────────────────┘
│  (Bad Rows)  │   │   Quality    │
└──────────────┘   └──────┬───────┘
                          │ (valid rows)
┌──────────────┐   ┌──────▼───────┐   ┌──────────────┐   ┌──────────────┐
│    Audit     │◀──│    Post-     │   │  Deduplicate │──▶│  MERGE INTO  │
│   Columns    │   │  Transform   │──▶│  (by merge   │   │   (Upsert)   │
└──────┬───────┘   └──────────────┘   │    key)      │   └──────┬───────┘
       │                               └──────────────┘          │
       ▼                                                         ▼
┌──────────────┐                                      ┌──────────────┐
│    Update    │◀─────────────────────────────────────│    Emit      │
│  Watermark   │                                      │   Metrics    │
└──────────────┘                                      └──────────────┘
```

### Secrets Manager Integration

```python
import json
import boto3
from botocore.exceptions import ClientError

def get_jdbc_credentials(secret_arn: str, region: str = "us-east-1") -> dict:
    """
    Retrieve JDBC credentials from AWS Secrets Manager.
    Returns dict with keys: username, password, host, port, database.
    """
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        return {
            "username": secret["username"],
            "password": secret["password"],
            "host": secret.get("host"),
            "port": secret.get("port"),
            "database": secret.get("dbname", secret.get("database")),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise RuntimeError(f"Secret not found: {secret_arn}")
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            raise RuntimeError(f"Access denied to secret: {secret_arn}")
        else:
            raise
```

### WatermarkManager Class

```python
import boto3
from datetime import datetime, timezone
from decimal import Decimal

class WatermarkManager:
    """
    Manages high-watermark state in DynamoDB for incremental extraction.
    Table schema: pipeline_id (PK), table_name (SK), watermark_value, updated_at
    """

    def __init__(self, table_name: str = "pipeline-watermarks", region: str = "us-east-1"):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def get_watermark(self, pipeline_id: str, table_name: str) -> str | None:
        """Get the last successful watermark value for a table."""
        try:
            response = self.table.get_item(
                Key={"pipeline_id": pipeline_id, "table_name": table_name}
            )
            item = response.get("Item")
            if item:
                return item.get("watermark_value")
            return None
        except Exception as e:
            raise RuntimeError(f"Failed to get watermark: {e}")

    def update_watermark(
        self,
        pipeline_id: str,
        table_name: str,
        watermark_value: str,
        rows_processed: int,
    ):
        """Update watermark after successful extraction."""
        self.table.put_item(
            Item={
                "pipeline_id": pipeline_id,
                "table_name": table_name,
                "watermark_value": watermark_value,
                "rows_processed": rows_processed,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def get_watermark_history(self, pipeline_id: str, table_name: str, limit: int = 10):
        """Retrieve recent watermark history for debugging."""
        response = self.table.query(
            KeyConditionExpression="pipeline_id = :pid AND table_name = :tbl",
            ExpressionAttributeValues={
                ":pid": pipeline_id,
                ":tbl": table_name,
            },
            ScanIndexForward=False,
            Limit=limit,
        )
        return response.get("Items", [])
```

### JDBC Incremental Read

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

def build_jdbc_url(engine: str, host: str, port: int, database: str, ssl_mode: str = "verify-full") -> str:
    """Build JDBC URL based on database engine."""
    url_templates = {
        "postgresql": f"jdbc:postgresql://{host}:{port}/{database}?ssl=true&sslmode={ssl_mode}",
        "mysql": f"jdbc:mysql://{host}:{port}/{database}?useSSL=true&requireSSL=true",
        "sqlserver": f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false",
        "oracle": f"jdbc:oracle:thin:@//{host}:{port}/{database}",
    }
    if engine not in url_templates:
        raise ValueError(f"Unsupported engine: {engine}. Use: {list(url_templates.keys())}")
    return url_templates[engine]


def read_jdbc_incremental(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    credentials: dict,
    watermark_column: str,
    watermark_value: str | None,
    fetch_size: int = 10000,
    partition_column: str | None = None,
    num_partitions: int = 8,
    lower_bound: int = 1,
    upper_bound: int = 100000000,
) -> DataFrame:
    """
    Read from JDBC source incrementally using watermark.
    If watermark_value is None, performs full initial load.
    """
    # Build query with watermark filter
    if watermark_value:
        query = f"(SELECT * FROM {table_name} WHERE {watermark_column} > '{watermark_value}') AS incremental"
    else:
        query = f"(SELECT * FROM {table_name}) AS full_load"

    reader = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("user", credentials["username"]) \
        .option("password", credentials["password"]) \
        .option("fetchsize", str(fetch_size)) \
        .option("isolationLevel", "READ_COMMITTED")

    # Add partitioning for parallel reads
    if partition_column and num_partitions > 1:
        reader = reader \
            .option("partitionColumn", partition_column) \
            .option("numPartitions", str(num_partitions)) \
            .option("lowerBound", str(lower_bound)) \
            .option("upperBound", str(upper_bound))

    return reader.load()
```

### MERGE INTO (Upsert)

```python
from pyspark.sql import DataFrame, SparkSession

def merge_into_iceberg(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: list[str],
    partition_columns: list[str] | None = None,
):
    """
    Perform MERGE INTO (upsert) on Iceberg table.
    Inserts new rows and updates existing rows based on merge keys.
    """
    # Register source as temp view
    source_df.createOrReplaceTempView("source_data")

    # Build merge condition
    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in merge_keys]
    )

    # Build column list for UPDATE SET and INSERT
    columns = source_df.columns
    update_set = ", ".join(
        [f"target.{c} = source.{c}" for c in columns if c not in merge_keys]
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"source.{c}" for c in columns])

    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING source_data AS source
    ON {merge_condition}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    spark.sql(merge_sql)
```

### Watermark Update After Successful Write

```python
def update_watermark_post_merge(
    watermark_manager: WatermarkManager,
    pipeline_id: str,
    table_name: str,
    source_df: DataFrame,
    watermark_column: str,
):
    """Update DynamoDB watermark after successful MERGE INTO."""
    # Get the max watermark value from the batch just processed
    max_watermark = source_df.agg({watermark_column: "max"}).collect()[0][0]
    row_count = source_df.count()

    if max_watermark is not None:
        watermark_manager.update_watermark(
            pipeline_id=pipeline_id,
            table_name=table_name,
            watermark_value=str(max_watermark),
            rows_processed=row_count,
        )
        return str(max_watermark)
    return None
```

### PII-Safe Logging

```python
import json
import hashlib
from datetime import datetime, timezone

class StructuredLogger:
    """
    Logger that redacts PII fields from log output.
    All logs are JSON-structured for CloudWatch Logs Insights queries.
    """

    def __init__(self, pipeline_id: str, pii_fields: list[str]):
        self.pipeline_id = pipeline_id
        self.pii_fields = set(pii_fields)

    def _redact(self, data: dict) -> dict:
        """Redact PII fields from log payload."""
        redacted = {}
        for key, value in data.items():
            if key in self.pii_fields:
                redacted[key] = "***REDACTED***"
            elif isinstance(value, dict):
                redacted[key] = self._redact(value)
            else:
                redacted[key] = value
        return redacted

    def info(self, message: str, **kwargs):
        log_entry = {
            "level": "INFO",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_id": self.pipeline_id,
            "message": message,
            **self._redact(kwargs),
        }
        print(json.dumps(log_entry))

    def error(self, message: str, **kwargs):
        log_entry = {
            "level": "ERROR",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_id": self.pipeline_id,
            "message": message,
            **self._redact(kwargs),
        }
        print(json.dumps(log_entry))

    def metric(self, metric_name: str, value: float, unit: str = "Count", **dimensions):
        log_entry = {
            "level": "METRIC",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_id": self.pipeline_id,
            "metric_name": metric_name,
            "value": value,
            "unit": unit,
            "dimensions": self._redact(dimensions),
        }
        print(json.dumps(log_entry))
```

### Row Hash (Excluding PII Columns)

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws, col

def add_row_hash(df: DataFrame, pii_columns: list[str], hash_column: str = "_row_hash") -> DataFrame:
    """
    Add a SHA-256 hash of non-PII columns for change detection.
    PII columns are excluded from the hash to avoid storing derived PII.
    """
    non_pii_columns = [c for c in df.columns if c not in pii_columns and not c.startswith("_")]
    hash_input = concat_ws("||", *[col(c).cast("string") for c in sorted(non_pii_columns)])
    return df.withColumn(hash_column, sha2(hash_input, 256))
```

---

## Step 5: Set Up Database Prerequisites

### PostgreSQL: Create Read-Only User

```sql
-- Create a dedicated read-only user for Glue extraction
CREATE ROLE glue_reader WITH LOGIN PASSWORD 'CHANGE_ME_USE_SECRETS_MANAGER';

-- Grant connect privilege
GRANT CONNECT ON DATABASE ecommerce TO glue_reader;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO glue_reader;

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO glue_reader;

-- Grant SELECT on future tables automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO glue_reader;

-- Set connection limit to prevent runaway sessions
ALTER ROLE glue_reader CONNECTION LIMIT 10;

-- Set statement timeout to prevent long-running queries from blocking
ALTER ROLE glue_reader SET statement_timeout = '600000';  -- 10 minutes
```

### Create Watermark Index

```sql
-- Create index on watermark column for efficient incremental reads
-- This prevents full table scans on each extraction run

CREATE INDEX CONCURRENTLY idx_orders_updated_at
    ON public.orders (updated_at);

CREATE INDEX CONCURRENTLY idx_order_items_updated_at
    ON public.order_items (updated_at);

-- Verify index usage
EXPLAIN ANALYZE
SELECT * FROM public.orders
WHERE updated_at > '2024-01-01 00:00:00'::timestamp;
```

### Create Update Trigger (Ensure Watermark Advances)

```sql
-- Trigger function to auto-update the updated_at column on row modification
CREATE OR REPLACE FUNCTION update_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to orders table
CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON public.orders
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_timestamp();

-- Apply trigger to order_items table
CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON public.order_items
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_timestamp();
```

---

## Step 6: Deploy Infrastructure

### Additional Resources for JDBC Pattern

| Resource                  | Purpose                                      | Config Key                    |
|---------------------------|----------------------------------------------|-------------------------------|
| KMS Key                   | Encrypt S3 target data and Secrets Manager   | `sensitivity.encryption_key_arn` |
| Secrets Manager Secret    | Store JDBC credentials (username/password)   | `connection.secret_arn`       |
| VPC Security Group        | Allow Glue ENIs to reach RDS on port 5432    | `network.security_group_ids`  |
| Glue Connection           | JDBC connection with VPC config              | `connection.glue_connection_name` |
| DynamoDB Table            | Store watermark state per table              | `pipeline-watermarks`         |
| Lake Formation Tags       | Column-level PII access control              | `security.access_policy`      |
| CloudWatch Alarm          | Alert on data freshness SLA breach           | `observability.freshness_sla_minutes` |

### Deploy Command

```bash
# Deploy with VPC context (required for JDBC pattern)
cdk deploy DataIngestionStack \
  --context environment=prod \
  --context pipeline=ecommerce-orders-jdbc \
  --context vpc_id=vpc-0abc123def456 \
  --context enable_lake_formation=true \
  --context enable_watermark_table=true \
  --require-approval broadening

# Verify Glue connection after deploy
aws glue get-connection \
  --name ecommerce-orders-conn \
  --query 'Connection.{Name:Name,Status:LastUpdatedTime,VPC:PhysicalConnectionRequirements.SubnetId}'
```

### Update JDBC Secret Post-Deploy

```bash
# Store credentials in Secrets Manager (run once, then rotate via rotation lambda)
aws secretsmanager put-secret-value \
  --secret-id prod/ecommerce/readonly \
  --secret-string '{
    "username": "glue_reader",
    "password": "GENERATED_PASSWORD_HERE",
    "host": "prod-orders.cluster-xxxx.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "ecommerce",
    "engine": "postgresql"
  }'

# Enable automatic rotation (recommended)
aws secretsmanager rotate-secret \
  --secret-id prod/ecommerce/readonly \
  --rotation-lambda-arn arn:aws:lambda:us-east-1:111122223333:function:secret-rotation \
  --rotation-rules AutomaticallyAfterDays=30
```

---

## Step 7: Run and Validate

### Upload Glue Script

```bash
# Upload ETL script to S3
aws s3 cp glue_job/ingest_orders_jdbc.py \
  s3://glue-scripts-111122223333/ecommerce-orders-jdbc/ingest_orders_jdbc.py

# Verify upload
aws s3 ls s3://glue-scripts-111122223333/ecommerce-orders-jdbc/
```

### Execute the Pipeline

```bash
# Run Glue job manually for initial load
aws glue start-job-run \
  --job-name ecommerce-orders-jdbc-ingest \
  --arguments '{
    "--pipeline_id": "ecommerce-orders-jdbc",
    "--table_name": "orders",
    "--environment": "prod",
    "--full_load": "true"
  }'

# Monitor job execution
aws glue get-job-run \
  --job-name ecommerce-orders-jdbc-ingest \
  --run-id <run-id> \
  --query 'JobRun.{Status:JobRunState,Duration:ExecutionTime,DPUs:AllocatedCapacity}'
```

### Validate with Athena Queries

```sql
-- 1. Query non-PII columns (available to all roles)
SELECT order_id, order_total, order_status, created_at, updated_at
FROM bronze_ecommerce.orders
WHERE ingestion_date = current_date
ORDER BY updated_at DESC
LIMIT 20;

-- 2. Query PII columns (requires data-science-role or data-engineering-role)
SELECT order_id, customer_email, customer_name, shipping_address
FROM bronze_ecommerce.orders
WHERE ingestion_date = current_date
LIMIT 5;

-- 3. Check quarantine table for rejected rows
SELECT *
FROM bronze_ecommerce.orders_quarantine
WHERE ingestion_date = current_date;

-- 4. Verify MERGE INTO behavior (no duplicates on merge key)
SELECT order_id, COUNT(*) as cnt
FROM bronze_ecommerce.orders
GROUP BY order_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows (no duplicates)

-- 5. Verify watermark advancement
SELECT *
FROM bronze_ecommerce.orders
WHERE updated_at > TIMESTAMP '2024-01-15 00:00:00'
ORDER BY updated_at DESC
LIMIT 10;

-- 6. Row count comparison (source vs target)
-- Run against source: SELECT COUNT(*) FROM public.orders;
-- Compare with:
SELECT COUNT(*) FROM bronze_ecommerce.orders;
```

---

## PII & Lake Formation Configuration

### Define Lake Formation Tags

```python
import boto3

def add_lf_tags_to_resource(
    database: str,
    table: str,
    pii_columns: list[str],
    catalog_id: str,
    region: str = "us-east-1",
):
    """
    Apply Lake Formation tags to PII columns for column-level access control.
    Tags enable fine-grained security without modifying IAM policies.
    """
    lf_client = boto3.client("lakeformation", region_name=region)

    # Create the PII tag if it doesn't exist
    try:
        lf_client.create_lf_tag(
            CatalogId=catalog_id,
            TagKey="pii",
            TagValues=["true", "false"],
        )
    except lf_client.exceptions.AlreadyExistsException:
        pass  # Tag already exists

    # Tag PII columns
    for column in pii_columns:
        lf_client.add_lf_tags_to_resource(
            CatalogId=catalog_id,
            Resource={
                "TableWithColumns": {
                    "DatabaseName": database,
                    "Name": table,
                    "ColumnNames": [column],
                }
            },
            LFTags=[
                {"CatalogId": catalog_id, "TagKey": "pii", "TagValues": ["true"]}
            ],
        )
        print(f"Tagged column {database}.{table}.{column} with pii=true")
```

### Grant Access by Role

```python
def grant_lf_permissions(
    database: str,
    table: str,
    pii_columns: list[str],
    all_columns: list[str],
    catalog_id: str,
    region: str = "us-east-1",
):
    """
    Configure Lake Formation grants for different access levels.
    - Full access: data science and engineering (all columns including PII)
    - Restricted access: analytics and reporting (non-PII columns only)
    """
    lf_client = boto3.client("lakeformation", region_name=region)
    non_pii_columns = [c for c in all_columns if c not in pii_columns]

    # Full access for data-science-role (all columns)
    lf_client.grant_permissions(
        CatalogId=catalog_id,
        Principal={
            "DataLakePrincipalIdentifier": f"arn:aws:iam::{catalog_id}:role/data-science-role"
        },
        Resource={
            "TableWithColumns": {
                "DatabaseName": database,
                "Name": table,
                "ColumnNames": all_columns,
            }
        },
        Permissions=["SELECT"],
        PermissionsWithGrantOption=[],
    )
    print(f"Granted FULL access to data-science-role on {database}.{table}")

    # Column-level access for analytics-role (excludes PII)
    lf_client.grant_permissions(
        CatalogId=catalog_id,
        Principal={
            "DataLakePrincipalIdentifier": f"arn:aws:iam::{catalog_id}:role/analytics-role"
        },
        Resource={
            "TableWithColumns": {
                "DatabaseName": database,
                "Name": table,
                "ColumnNames": non_pii_columns,
            }
        },
        Permissions=["SELECT"],
        PermissionsWithGrantOption=[],
    )
    print(f"Granted RESTRICTED access to analytics-role on {database}.{table} (excluded: {pii_columns})")
```

---

## Engine-Specific Notes

### PostgreSQL

```
JDBC URL:    jdbc:postgresql://{host}:{port}/{database}?ssl=true&sslmode=verify-full
Driver:      org.postgresql.Driver
Default Port: 5432
SSL Options: disable, require, verify-ca, verify-full
Notes:
  - Use `sslmode=verify-full` in production
  - Set `statement_timeout` on the reader role to prevent long queries
  - Use `READ_COMMITTED` isolation to avoid blocking writers
  - Supports partition column with numeric types for parallel reads
```

### MySQL

```
JDBC URL:    jdbc:mysql://{host}:{port}/{database}?useSSL=true&requireSSL=true&verifyServerCertificate=true
Driver:      com.mysql.cj.jdbc.Driver
Default Port: 3306
SSL Options: useSSL=true, requireSSL=true, verifyServerCertificate=true
Notes:
  - Use `com.mysql.cj.jdbc.Driver` (not deprecated `com.mysql.jdbc.Driver`)
  - Set `sessionVariables=net_read_timeout=600,net_write_timeout=600`
  - Use `REPEATABLE_READ` for consistent snapshots
  - TIMESTAMP columns are UTC; DATETIME columns preserve local time
```

### SQL Server

```
JDBC URL:    jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false
Driver:      com.microsoft.sqlserver.jdbc.SQLServerDriver
Default Port: 1433
SSL Options: encrypt=true, trustServerCertificate=false
Notes:
  - Use `WITH (NOLOCK)` hint in custom queries to avoid blocking
  - Set `selectMethod=cursor` for large result sets
  - SQL Server uses `GETDATE()` instead of `NOW()` for triggers
  - Use `OFFSET/FETCH` for pagination in custom queries
```

### Oracle

```
JDBC URL:    jdbc:oracle:thin:@//{host}:{port}/{service_name}
Driver:      oracle.jdbc.OracleDriver
Default Port: 1521
SSL Options: Configure via Oracle Wallet or TCPS
Notes:
  - Use service name (not SID) in JDBC URL for RAC support
  - Set `oracle.jdbc.timezoneAsRegion=false` to avoid timezone issues
  - Use `SYSTIMESTAMP` for watermark columns (includes timezone)
  - Set `defaultRowPrefetch=1000` for better fetch performance
  - Oracle DATE type includes time; use TIMESTAMP for sub-second precision
```

---

## Security Checklist

- [ ] **Secrets Manager**: JDBC credentials stored in Secrets Manager (never in code or config files)
- [ ] **Secret Rotation**: Automatic rotation enabled with 30-day cycle
- [ ] **VPC Isolation**: Glue job runs in private subnets with no internet access (NAT for AWS APIs)
- [ ] **Security Groups**: Inbound rules allow only Glue ENIs on the database port
- [ ] **TLS in Transit**: SSL/TLS enforced on JDBC connection (`sslmode=verify-full`)
- [ ] **KMS Encryption**: S3 target encrypted with customer-managed KMS key
- [ ] **Lake Formation**: PII columns tagged and access restricted by IAM role
- [ ] **IAM Least Privilege**: Glue role has only required permissions (S3 path-specific, single secret)
- [ ] **Audit Logging**: CloudTrail enabled for Secrets Manager access and Lake Formation grants
- [ ] **Network ACLs**: Deny all except required CIDR ranges for database subnet
- [ ] **PII Logging**: Structured logger redacts PII fields from CloudWatch logs

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| `Communications link failure` | Security group blocks Glue ENI → RDS | Add inbound rule for SG of Glue connection on port 5432 |
| `Connection timed out` | Subnet has no route to RDS | Verify route tables; RDS and Glue must share VPC/subnets |
| `FATAL: password authentication failed` | Secret value doesn't match DB password | Sync Secrets Manager with actual DB credentials |
| `SSL connection is required` | DB enforces SSL but JDBC URL missing ssl params | Add `?ssl=true&sslmode=verify-full` to JDBC URL |
| `Watermark not advancing` | `updated_at` trigger missing on source table | Create `BEFORE UPDATE` trigger (see Step 5) |
| `Duplicate rows after MERGE` | Merge key contains NULL values | Add NOT NULL constraint or COALESCE in merge condition |
| `OutOfMemoryError` in Glue | Fetch size too large or partition skew | Reduce `fetch_size`, increase `num_partitions`, check data distribution |
| `Timeout reading from server` | Query exceeds `statement_timeout` | Increase timeout on reader role or reduce batch window |
| `Access Denied` on Lake Formation | Role not granted column permissions | Run `grant_lf_permissions()` for the role |
| `Table not found in Glue Catalog` | Iceberg table not registered | Run `CREATE TABLE` DDL via Athena or Spark catalog |
| `Secret rotation broke pipeline` | New password not propagated to Glue | Use `get_secret_value` at job start (not cached) |
| `JDBC driver not found` | Missing driver JAR in Glue job | Add `--extra-jars` pointing to S3 path of driver JAR |

---

## Checklist

- [ ] Onboarding request approved by data steward, DBA, and security reviewer
- [ ] Data contract authored with PII tags and quality rules
- [ ] Pipeline configuration created with watermark and merge key settings
- [ ] Database read-only user created with connection limit and statement timeout
- [ ] Watermark index created on source table (`updated_at` column)
- [ ] Update trigger installed to ensure watermark advances on row modification
- [ ] Secrets Manager secret created with credentials and rotation enabled
- [ ] VPC security group configured (Glue ENI → RDS inbound rule)
- [ ] Glue connection created and tested (`aws glue test-connection`)
- [ ] DynamoDB watermark table provisioned with pipeline_id + table_name key
- [ ] Lake Formation tags applied to PII columns
- [ ] Lake Formation grants configured for each access tier
- [ ] Initial full-load run completed successfully with row count validation
- [ ] Incremental run validated (watermark advances, no duplicates after MERGE)
