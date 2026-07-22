# Schema Inference & Evolution

## Purpose

This steering file governs the automatic detection of source schemas, data type inference,
sensitivity classification, and schema evolution handling across all ingestion pipelines.

**Two-layer context:** Schema inference samples data from the Landing layer (if already
available) or directly from the source. The inferred schema drives the Raw/Bronze Iceberg
table creation and the data contract DQDL rules. PII/PHI columns are tagged in Lake
Formation at the Raw layer — they are NOT masked (masking belongs in Silver).

The system must:
- Auto-detect column names, types, nullability, and constraints from any supported source
- Classify each column for sensitivity (PII, PHI, financial, public)
- Version every schema as an immutable artifact
- Detect drift between runs and route changes by severity
- Never auto-apply breaking changes without human approval
- Compile quality rules to DQDL syntax for Glue Data Quality

---

## Schema Inference Process

```
┌─────────────────┐     ┌─────────────┐     ┌────────────────┐     ┌───────────────────────┐     ┌─────────────────┐
│ Source Connection│────▶│ Sample Data │────▶│ Type Inference │────▶│ Sensitivity Classifier│────▶│ Schema Document │
└─────────────────┘     └─────────────┘     └────────────────┘     └───────────────────────┘     └─────────────────┘
        │                      │                     │                          │                         │
   Connect to source    Pull N rows/records    Map native types         Apply regex + NLP          Store versioned
   with credentials     (min 1000)             to canonical types       pattern matching           schema in DynamoDB
```

### Steps

1. **Connect** — Establish authenticated connection to the source system.
2. **Sample** — Retrieve a representative sample (minimum 1000 rows or records).
3. **Infer Types** — Map source-native types to canonical platform types.
4. **Classify Sensitivity** — Run pattern matching and NLP rules on column names and sample values.
5. **Emit Schema Document** — Persist a versioned, immutable schema artifact.

---

## Inference by Source Type

| Source Type       | Inference Method                  | Notes                                      |
|-------------------|-----------------------------------|--------------------------------------------|
| RDBMS (MySQL, PostgreSQL, Oracle, SQL Server) | `INFORMATION_SCHEMA` query | Authoritative; includes constraints, defaults, indexes |
| REST API          | Response JSON parsing + sampling  | Infer from multiple response pages; handle polymorphic fields |
| CSV / Flat File   | AWS Glue Crawler                  | Sample-based; configure classifier for delimiters |
| JSON (nested)     | Spark `schema_of_json` inference  | Flatten nested structures; track path expressions |
| Parquet / Avro    | Embedded schema extraction        | Read footer (Parquet) or header (Avro) directly |
| Kafka             | Confluent Schema Registry lookup  | Use subject-version API; fall back to message sampling |

### RDBMS Example

```sql
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'ecommerce'
  AND table_name = 'orders'
ORDER BY ordinal_position;
```

### JSON Inference Example (Spark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import schema_of_json, col

spark = SparkSession.builder.getOrCreate()

sample_json = spark.read.json("s3://raw-landing/api-responses/sample/")
inferred_schema = sample_json.schema

# Flatten nested structs
from pyspark.sql.types import StructType, StructField

def flatten_schema(schema, prefix=""):
    fields = []
    for field in schema.fields:
        col_name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields += flatten_schema(field.dataType, col_name)
        else:
            fields.append((col_name, str(field.dataType), field.nullable))
    return fields

flat_columns = flatten_schema(inferred_schema)
for name, dtype, nullable in flat_columns:
    print(f"  {name}: {dtype} (nullable={nullable})")
```

---

## Glue Crawler Configuration

```python
import boto3

glue = boto3.client("glue")

def create_schema_inference_crawler(
    source_name: str,
    s3_path: str,
    database_name: str,
    role_arn: str,
    schedule: str = ""
) -> dict:
    """
    Create a Glue Crawler for schema inference on S3-based sources.
    """
    crawler_name = f"schema-infer-{source_name}"

    crawler_config = {
        "Name": crawler_name,
        "Role": role_arn,
        "DatabaseName": database_name,
        "Targets": {
            "S3Targets": [
                {
                    "Path": s3_path,
                    "SampleSize": 100,  # Number of files to sample
                    "Exclusions": [
                        "**/_temporary/**",
                        "**/.spark-staging/**",
                    ],
                }
            ]
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "LOG",        # Don't auto-update; log changes
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY",
        },
        "Configuration": json.dumps({
            "Version": 1.0,
            "Grouping": {
                "TableGroupingPolicy": "CombineCompatibleSchemas"
            },
            "CrawlerOutput": {
                "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
            },
        }),
        "Tags": {
            "platform:component": "schema-inference",
            "platform:source": source_name,
        },
    }

    if schedule:
        crawler_config["Schedule"] = schedule

    response = glue.create_crawler(**crawler_config)
    print(f"Created crawler: {crawler_name}")

    # Start initial run
    glue.start_crawler(Name=crawler_name)
    print(f"Started initial crawl for: {crawler_name}")

    return response
```

---

## Sensitivity Classification Rules

### Classification Tiers

| Tier     | Label         | Handling                              |
|----------|---------------|---------------------------------------|
| Tier 1   | PHI           | Encrypt at rest + in transit, audit access, mask in non-prod |
| Tier 2   | PII           | Encrypt, restrict access, tokenize where possible |
| Tier 3   | Financial     | Encrypt, PCI-DSS controls if card data |
| Tier 4   | Internal      | Standard encryption, role-based access |
| Tier 5   | Public        | No special handling required          |

### Pattern Matching Rules

| Pattern               | Classification | Detection Method     | Regex / Heuristic                              |
|-----------------------|----------------|----------------------|------------------------------------------------|
| SSN                   | PII (Tier 2)   | Content-based        | `\b\d{3}-\d{2}-\d{4}\b`                       |
| Email                 | PII (Tier 2)   | Name + Content       | `\b[\w.+-]+@[\w-]+\.[\w.-]+\b`                 |
| Phone                 | PII (Tier 2)   | Name + Content       | `\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b` |
| Date of Birth (DOB)   | PII (Tier 2)   | Name-based           | Column name matches `dob|date_of_birth|birthdate` |
| Medical Record Number | PHI (Tier 1)   | Name-based           | Column name matches `mrn|medical_record|patient_id` |
| Free text (clinical)  | PHI (Tier 1)   | Content-based + NLP  | Length > 50 chars + medical term dictionary hit |
| Financial (card #)    | Financial (Tier 3) | Content-based    | Luhn algorithm validation + `\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b` |

### Detection Approaches

1. **Rule-based (regex)** — Apply regex patterns to sampled column values. Fast, deterministic, low false-positive rate for structured patterns like SSN and credit cards.

2. **Name-based (column name heuristics)** — Match column names against a curated dictionary of sensitive field names. Catches fields that may not have detectable content patterns (e.g., `date_of_birth` stored as epoch integer).

3. **Content-based (statistical + NLP)** — For free-text columns, run a lightweight NLP classifier trained on clinical and financial terminology. Flag columns where > 5% of sampled values contain sensitive terms.

```python
import re
from typing import List, Tuple

SENSITIVITY_RULES = [
    {
        "name": "SSN",
        "tier": 2,
        "classification": "PII",
        "method": "content",
        "pattern": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "threshold": 0.01,  # Flag if > 1% of values match
    },
    {
        "name": "Email",
        "tier": 2,
        "classification": "PII",
        "method": "name+content",
        "name_pattern": re.compile(r"(?i)(email|e_mail|email_address)"),
        "pattern": re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b"),
        "threshold": 0.1,
    },
    {
        "name": "Phone",
        "tier": 2,
        "classification": "PII",
        "method": "name+content",
        "name_pattern": re.compile(r"(?i)(phone|mobile|cell|fax|tel)"),
        "pattern": re.compile(r"\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
        "threshold": 0.1,
    },
    {
        "name": "DOB",
        "tier": 2,
        "classification": "PII",
        "method": "name",
        "name_pattern": re.compile(r"(?i)(dob|date_of_birth|birthdate|birth_date)"),
    },
    {
        "name": "Medical Record",
        "tier": 1,
        "classification": "PHI",
        "method": "name",
        "name_pattern": re.compile(r"(?i)(mrn|medical_record|patient_id|encounter_id)"),
    },
    {
        "name": "Clinical Free Text",
        "tier": 1,
        "classification": "PHI",
        "method": "content+nlp",
        "min_length": 50,
        "medical_terms_threshold": 0.05,
    },
    {
        "name": "Credit Card",
        "tier": 3,
        "classification": "Financial",
        "method": "content",
        "pattern": re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"),
        "validator": "luhn",
        "threshold": 0.01,
    },
]


def classify_column(column_name: str, sample_values: List[str]) -> Tuple[str, int]:
    """
    Classify a column's sensitivity tier based on name and content patterns.
    Returns (classification_label, tier_number).
    """
    for rule in SENSITIVITY_RULES:
        # Name-based check
        if "name_pattern" in rule:
            if rule["name_pattern"].search(column_name):
                return (rule["classification"], rule["tier"])

        # Content-based check
        if "pattern" in rule and sample_values:
            matches = sum(1 for v in sample_values if rule["pattern"].search(str(v)))
            match_rate = matches / len(sample_values)
            if match_rate >= rule.get("threshold", 0.01):
                return (rule["classification"], rule["tier"])

    return ("Public", 5)
```

---

## Schema Evolution Handling

### Types of Drift

| Drift Type          | Severity | Example                                   | Default Action        |
|---------------------|----------|-------------------------------------------|-----------------------|
| New column          | Low      | Source adds `loyalty_tier` column         | Auto-add to target    |
| Missing column      | Medium   | Source drops `fax_number`                 | Alert + continue      |
| Type widening       | Low      | `INT` → `BIGINT`                          | Auto-widen target     |
| Type narrowing      | High     | `VARCHAR(255)` → `VARCHAR(50)`            | Pause + escalate      |
| Column rename       | High     | `cust_name` → `customer_full_name`       | Pause + escalate      |
| Volume spike (>3x)  | Medium   | Daily rows jump from 10K to 50K          | Alert + continue      |
| Volume drop (>50%)  | Medium   | Daily rows drop from 10K to 2K           | Alert + continue      |

### Routing by Severity

#### Low Severity (Additive) — Auto-Handle

Steps:
1. Detect new column or type widening in inferred schema.
2. Compare against current schema version in DynamoDB.
3. Generate and execute `ALTER TABLE ADD COLUMNS` or type migration.
4. Update schema version (increment minor version).
5. Log evolution event to `platform-schema-evolution-log`.
6. Notify pipeline owner via SNS (informational).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .getOrCreate()

def handle_additive_drift(
    catalog: str,
    database: str,
    table: str,
    new_columns: list
):
    """
    Auto-handle additive schema changes (new columns, type widening).
    """
    for col in new_columns:
        col_name = col["name"]
        col_type = col["type"]
        comment = col.get("comment", "Auto-added by schema evolution handler")

        alter_sql = f"""
            ALTER TABLE {catalog}.{database}.{table}
            ADD COLUMNS (
                {col_name} {col_type} COMMENT '{comment}'
            )
        """
        print(f"Executing: {alter_sql.strip()}")
        spark.sql(alter_sql)

    print(f"Added {len(new_columns)} column(s) to {database}.{table}")


# Example usage
handle_additive_drift(
    catalog="glue_catalog",
    database="ecommerce_bronze",
    table="orders",
    new_columns=[
        {"name": "loyalty_tier", "type": "STRING", "comment": "Customer loyalty tier"},
        {"name": "referral_code", "type": "STRING", "comment": "Referral tracking code"},
    ]
)
```

#### Medium Severity — Alert + Continue

Steps:
1. Detect missing column or volume anomaly.
2. Log warning to `platform-schema-evolution-log` with severity=MEDIUM.
3. Send SNS alert to pipeline owner and data steward.
4. Continue pipeline execution using last-known-good schema (fill missing columns with NULL).
5. Create Jira ticket for human review within 48 hours.

#### High Severity (Breaking) — Pause + Escalate

Steps:
1. Detect type narrowing, column rename, or incompatible structural change.
2. Immediately pause the pipeline (set Glue workflow to STOPPED).
3. Log critical event to `platform-schema-evolution-log` with severity=HIGH.
4. Send PagerDuty alert to on-call data engineer.
5. Present resolution options:

**Option A — Accept and Migrate:**
- Create migration script to transform existing data.
- Apply new schema as next major version.
- Backfill affected partitions.

**Option B — Reject and Pin:**
- Pin pipeline to previous schema version.
- Apply transformation in ETL to coerce source data to expected format.
- Schedule review for next sprint.

**Option C — Fork:**
- Create new table version (e.g., `orders_v2`).
- Run both old and new pipelines in parallel during transition.
- Migrate consumers incrementally.

### Consumer Impact Analysis

```python
import boto3
from typing import List, Dict

dynamodb = boto3.resource("dynamodb")


def find_affected_consumers(
    database: str,
    table: str,
    changed_columns: List[str]
) -> List[Dict]:
    """
    Query the consumer registry to find downstream systems
    affected by schema changes.
    """
    consumer_table = dynamodb.Table("platform-data-consumers")

    response = consumer_table.query(
        IndexName="source-table-index",
        KeyConditionExpression="source_table = :st",
        ExpressionAttributeValues={
            ":st": f"{database}.{table}"
        }
    )

    affected = []
    for consumer in response["Items"]:
        consumed_columns = set(consumer.get("consumed_columns", []))
        impacted_columns = consumed_columns.intersection(set(changed_columns))

        if impacted_columns:
            affected.append({
                "consumer_id": consumer["consumer_id"],
                "team": consumer["team"],
                "contact": consumer["contact_email"],
                "impacted_columns": list(impacted_columns),
                "pipeline_name": consumer.get("pipeline_name", "unknown"),
                "sla_tier": consumer.get("sla_tier", "standard"),
            })

    # Sort by SLA tier (critical first)
    sla_order = {"critical": 0, "high": 1, "standard": 2, "low": 3}
    affected.sort(key=lambda x: sla_order.get(x["sla_tier"], 99))

    print(f"Found {len(affected)} affected consumer(s) for {database}.{table}")
    for c in affected:
        print(f"  - {c['consumer_id']} ({c['team']}): columns {c['impacted_columns']}")

    return affected
```

---

## Schema Storage

### DynamoDB Table: `platform-schema-versions`

| Attribute             | Type   | Key     | Description                                |
|-----------------------|--------|---------|--------------------------------------------|
| `source_id`           | String | PK      | Unique source identifier (e.g., `rds.ecommerce.orders`) |
| `version`             | Number | SK      | Auto-incrementing schema version           |
| `columns`             | List   | —       | Array of column definitions                |
| `columns[].name`      | String | —       | Column name                                |
| `columns[].type`      | String | —       | Canonical data type                        |
| `columns[].nullable`  | Boolean| —       | Whether column allows NULL                 |
| `columns[].sensitivity` | String | —     | Classification tier (PHI, PII, Financial, Internal, Public) |
| `inferred_at`         | String | —       | ISO-8601 timestamp of inference run        |
| `sample_size`         | Number | —       | Number of rows/records sampled             |
| `checksum`            | String | —       | SHA-256 hash of column definitions         |
| `status`              | String | —       | `active`, `deprecated`, `superseded`       |
| `created_by`          | String | —       | Pipeline or user that created this version |

---

## Schema Evolution Log

### DynamoDB Table: `platform-schema-evolution-log`

| Attribute             | Type   | Key     | Description                                |
|-----------------------|--------|---------|--------------------------------------------|
| `source_id`           | String | PK      | Source identifier                          |
| `event_id`            | String | SK      | ULID for ordering                          |
| `event_type`          | String | GSI-PK  | `NEW_COLUMN`, `MISSING_COLUMN`, `TYPE_CHANGE`, `RENAME`, `VOLUME_ANOMALY` |
| `severity`            | String | GSI-SK  | `LOW`, `MEDIUM`, `HIGH`                    |
| `detected_at`         | String | —       | ISO-8601 timestamp                         |
| `previous_version`    | Number | —       | Schema version before change               |
| `new_version`         | Number | —       | Schema version after change (if applied)   |
| `drift_details`       | Map    | —       | Structured diff of what changed            |
| `drift_details.column`| String | —       | Affected column name                       |
| `drift_details.before`| String | —       | Previous state (type, name, etc.)          |
| `drift_details.after` | String | —       | New state                                  |
| `action_taken`        | String | —       | `AUTO_APPLIED`, `ALERTED`, `PAUSED`, `PENDING` |
| `resolved_by`         | String | —       | User or automation that resolved           |
| `resolved_at`         | String | —       | ISO-8601 resolution timestamp              |
| `pipeline_run_id`     | String | —       | Associated pipeline execution ID           |
| `ttl`                 | Number | —       | TTL epoch for auto-expiration (90 days)    |

---

## MUST Rules

1. **Sample at least 1000 rows** — Every schema inference run must sample a minimum of 1000 rows (or all available rows if fewer exist). Smaller samples produce unreliable type inference and miss sparse sensitive values.

2. **Classify every column** — No column may pass through inference without a sensitivity classification. Default to `Internal` (Tier 4) if no pattern matches, but always assign explicitly.

3. **Store as versioned artifact** — Every inferred schema must be persisted to `platform-schema-versions` with an incremented version number. Schemas are immutable once stored; corrections create new versions.

4. **Alert on drift within 1 run** — Schema comparison must happen within the same pipeline execution that detects the change. Do not defer drift detection to a separate batch process. Latency between detection and alert must be under 5 minutes.

5. **Never auto-apply breaking changes** — Any change classified as HIGH severity (type narrowing, column rename, structural incompatibility) must pause the pipeline and require human approval before proceeding. No exception, no override flag.

6. **Route clinical free-text to NLP scanning** — Any column with average value length > 50 characters that contains medical terminology (matched against the UMLS Metathesaurus term list) must be escalated to the NLP sensitivity scanner. Regex alone is insufficient for unstructured clinical text.
