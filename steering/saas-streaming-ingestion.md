# SaaS & Streaming Ingestion

## When to Use

### SaaS (REST API) Ingestion
Use this pattern when ingesting data from cloud SaaS platforms via REST APIs:
- **Salesforce** — Accounts, Contacts, Opportunities, Custom Objects
- **HubSpot** — Contacts, Companies, Deals, Engagements
- **Zendesk** — Tickets, Users, Organizations
- **Stripe** — Payments, Subscriptions, Invoices
- **ServiceNow** — Incidents, Changes, CMDB
- **Workday** — Employees, Positions, Compensation

### Streaming Ingestion
Use this pattern for real-time or near-real-time event data:
- **Apache Kafka** — Event streams from microservices, CDC topics
- **Amazon Kinesis Data Streams** — Clickstream, IoT telemetry, application logs
- **Amazon MSK** — Managed Kafka clusters

---

## Architecture

### SaaS REST API Pattern

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  SaaS API    │───▶│  Glue Job    │───▶│  S3 Bronze   │───▶│  Iceberg     │
│  (OAuth2)    │    │  (Python)    │    │  (Parquet)   │    │  Table       │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                          │                                        │
                          ▼                                        ▼
                    ┌──────────────┐                      ┌──────────────┐
                    │  Secrets Mgr │                      │  Glue Catalog│
                    │  (OAuth creds)│                      │              │
                    └──────────────┘                      └──────────────┘
```

### Streaming Pattern

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Kafka/MSK   │───▶│  Glue        │───▶│  S3 Bronze   │───▶│  Iceberg     │
│  Topic       │    │  Streaming   │    │  (Parquet)   │    │  Table       │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                          │
                          ▼
                    ┌──────────────┐
                    │  Checkpoint  │
                    │  (S3/DynamoDB)│
                    └──────────────┘
```

---

## SaaS Pipeline Details

### Authentication
All SaaS connectors use OAuth2 with credentials stored in Secrets Manager:
- **Client Credentials Flow** — service-to-service (Salesforce, HubSpot)
- **Refresh Token Flow** — delegated user access
- Token refresh handled automatically before expiry

### Pagination
- **Cursor-based** — preferred (Salesforce nextRecordsUrl, HubSpot after param)
- **Offset-based** — fallback (skip/limit patterns)
- Page size configured per source (default: 200 records)

### Rate Limiting
- Token bucket algorithm with configurable requests/second
- Exponential backoff on 429 responses
- Per-endpoint rate limits from source API docs

### Endpoint Configuration
```yaml
endpoints:
  - name: accounts
    path: /services/data/v58.0/query
    method: GET
    pagination: cursor
    rate_limit_rps: 25
    params:
      q: "SELECT Id, Name, Industry FROM Account WHERE LastModifiedDate > '{last_run}'"
```

---

## Streaming Pipeline Details

### Glue Streaming ETL
- Runs as a continuous Glue job with `--job-type streaming`
- Reads from Kafka/Kinesis using Spark Structured Streaming
- Writes micro-batches to Iceberg tables

### Checkpointing
- Kafka offsets stored in S3 checkpoint location
- Enables exactly-once semantics with Iceberg atomic commits
- Checkpoint interval configurable (default: 60 seconds)

### Micro-Batch Configuration
- Window size: 60-300 seconds depending on latency requirements
- Max records per batch: configurable per topic
- Trigger: processingTime interval

---

## Pipeline Config YAML — Salesforce Example

```yaml
pipeline:
  name: salesforce-accounts-ingest
  version: "1.0"
  template: saas_rest_api

source:
  type: salesforce
  auth:
    method: oauth2_client_credentials
    secret_name: salesforce/prod/api-credentials
    token_url: https://login.salesforce.com/services/oauth2/token
  base_url: https://mycompany.my.salesforce.com
  api_version: v58.0
  endpoints:
    - name: accounts
      path: /services/data/v58.0/query
      method: GET
      pagination:
        type: cursor
        next_field: nextRecordsUrl
      rate_limit:
        requests_per_second: 25
        burst: 50
      incremental:
        field: LastModifiedDate
        format: iso8601
      query: >
        SELECT Id, Name, Industry, AnnualRevenue, CreatedDate, LastModifiedDate
        FROM Account
        WHERE LastModifiedDate > '{last_watermark}'

target:
  database: bronze
  table: salesforce_accounts
  format: iceberg
  partition_by:
    - column: ingestion_date
      transform: day
  location: s3://data-lake-bronze/salesforce/accounts/

schedule:
  type: rate
  interval: 1 hour

quality:
  null_check:
    - column: Id
      threshold: 0.0
  freshness_sla_hours: 2
```

---

## Code Examples

### OAuth Token Refresh

```python
import boto3
import requests
import json
from datetime import datetime, timedelta

class OAuthTokenManager:
    """Manages OAuth2 token lifecycle with Secrets Manager."""

    def __init__(self, secret_name: str, region: str = "us-east-1"):
        self.secret_name = secret_name
        self.sm_client = boto3.client("secretsmanager", region_name=region)
        self._token = None
        self._expires_at = None

    def get_token(self) -> str:
        if self._token and self._expires_at > datetime.utcnow():
            return self._token
        return self._refresh_token()

    def _refresh_token(self) -> str:
        secret = json.loads(
            self.sm_client.get_secret_value(SecretId=self.secret_name)["SecretString"]
        )
        response = requests.post(
            secret["token_url"],
            data={
                "grant_type": "client_credentials",
                "client_id": secret["client_id"],
                "client_secret": secret["client_secret"],
            },
            timeout=30,
        )
        response.raise_for_status()
        token_data = response.json()
        self._token = token_data["access_token"]
        self._expires_at = datetime.utcnow() + timedelta(
            seconds=token_data.get("expires_in", 3600) - 300
        )
        return self._token
```

### Paginated Fetch

```python
from typing import Generator, Dict, Any

def fetch_paginated(
    base_url: str,
    endpoint: str,
    token_manager: OAuthTokenManager,
    page_size: int = 200,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all records using cursor-based pagination."""
    url = f"{base_url}{endpoint}"
    headers = {"Authorization": f"Bearer {token_manager.get_token()}"}

    while url:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()

        for record in data.get("records", []):
            yield record

        next_url = data.get("nextRecordsUrl")
        url = f"{base_url}{next_url}" if next_url else None
```

### Rate Limiter

```python
import time
import threading

class TokenBucketRateLimiter:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_refill = now

            if self.tokens >= 1:
                self.tokens -= 1
                return
        # Wait and retry
        time.sleep(1.0 / self.rate)
        self.acquire()
```

### Streaming Checkpoint (Glue Streaming Job)

```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Read from Kafka with checkpointing
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9092")
    .option("subscribe", "events-topic")
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SSL")
    .load()
)

# Parse and transform
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType().add("event_id", StringType()).add("event_time", TimestampType())
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to Iceberg with checkpointing
query = (
    parsed_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3://data-lake-checkpoints/kafka/events-topic/")
    .trigger(processingTime="60 seconds")
    .toTable("bronze.kafka_events")
)
query.awaitTermination()
```

---

## Kafka Consumer Config for Glue

```python
kafka_options = {
    "kafka.bootstrap.servers": "b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9092",
    "subscribe": "events-topic",
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/tmp/kafka.client.truststore.jks",
    "kafka.consumer.group.id": "glue-streaming-consumer",
    "failOnDataLoss": "false",
}
```

---

## Template Selection Guidance

| Source Type | Template | Use When |
|---|---|---|
| `saas_rest_api` | SaaS REST | Pulling from any REST API with OAuth2 |
| `glue_streaming_kafka` | Streaming Kafka | Consuming from Kafka/MSK topics |
| `glue_streaming_kinesis` | Streaming Kinesis | Consuming from Kinesis Data Streams |
| `s3_file_landing` | S3 File | Files land in S3 (not API or stream) |
| `jdbc_batch` | JDBC Batch | Direct database extraction via JDBC |

---

## Comparison Table

| Dimension | SaaS REST API | Streaming (Kafka/Kinesis) | JDBC Batch |
|---|---|---|---|
| Latency | Minutes to hours | Seconds to minutes | Minutes to hours |
| Volume | Low-medium (API limits) | High (millions/sec) | Medium-high |
| Complexity | Medium (auth, pagination) | High (offsets, ordering) | Low-medium |
| Cost Driver | API call limits | Continuous compute | Connection time |
| Incremental | Watermark field | Offset tracking | Watermark column |
| Error Handling | Retry with backoff | Dead letter topic | Job retry |
| Best For | CRM, marketing, ticketing | Events, CDC, IoT | Databases, warehouses |
