# Data Contracts

## Why Data Contracts

Data contracts formalize the agreement between data producers and consumers, providing:

1. **Schema Enforcement** — Prevent breaking changes from propagating downstream
2. **Quality Guarantees** — Define and enforce data quality rules at ingestion time
3. **Evolution Control** — Manage schema changes with explicit policies and impact analysis
4. **Security Classification** — Tag sensitive fields for automated access control (Lake Formation)
5. **Discoverability** — Self-documenting data assets with ownership and lineage metadata
6. **Consumer Awareness** — Track who uses what data and notify on breaking changes

---

## Contract Structure

A data contract is a YAML file with seven top-level sections:

```yaml
contract:       # Identity and versioning
source:         # Where data comes from
schema:         # Field definitions and classifications
quality:        # Data quality rules and SLAs
evolution_policy: # How schema changes are handled
consumers:      # Downstream dependencies
metadata:       # Ownership, tags, documentation
security:       # Access control and encryption
```

---

## Section Details

### 1. contract

```yaml
contract:
  name: crm_accounts
  version: "2.1.0"
  status: active          # draft | active | deprecated
  owner: data-platform-team
  domain: customer
  description: "Customer account master data from Salesforce CRM"
```

### 2. source

```yaml
source:
  type: s3_file           # s3_file | jdbc | salesforce | kafka | kinesis
  format: csv             # csv | json | parquet | avro
  location: s3://raw-data-bucket/crm/accounts/
  delimiter: ","
  header: true
  encoding: utf-8
  # For JDBC sources:
  # glue_connection_name: prod-oracle-connection
  # schema: CRM
  # table: ACCOUNTS
  # incremental_column: LAST_MODIFIED_DATE
```

### 3. schema

```yaml
schema:
  fields:
    - name: account_id
      type: string
      nullable: false
      description: "Unique account identifier"
      classification: internal
      constraints:
        - type: unique

    - name: account_name
      type: string
      nullable: false
      description: "Company name"
      classification: internal
      constraints:
        - type: max_length
          value: 255

    - name: email
      type: string
      nullable: true
      description: "Primary contact email"
      classification: pii
      constraints:
        - type: pattern
          value: "^[\\w.-]+@[\\w.-]+\\.\\w+$"

    - name: annual_revenue
      type: decimal
      precision: 15
      scale: 2
      nullable: true
      classification: internal
      constraints:
        - type: min_value
          value: 0

    - name: created_date
      type: timestamp
      nullable: false
      classification: internal
```

### 4. quality

```yaml
quality:
  freshness_sla_hours: 4
  volume_bounds:
    min_rows: 1000
    max_rows: 5000000
    max_variance_pct: 50    # Alert if volume changes >50% from previous run
  completeness:
    - column: account_id
      threshold: 1.0        # 100% non-null required
    - column: email
      threshold: 0.85       # 85% non-null expected
  accuracy:
    - name: revenue_positive
      sql: "annual_revenue >= 0 OR annual_revenue IS NULL"
      threshold: 0.99
    - name: valid_status
      sql: "status IN ('active', 'inactive', 'prospect')"
      threshold: 1.0
```

### 5. evolution_policy

```yaml
evolution_policy:
  strategy: additive_auto   # additive_auto | strict | permissive
  actions:
    on_new_column: add_nullable
    on_type_widen: allow
    on_column_remove: quarantine
    on_type_narrow: reject
  notification:
    channels:
      - slack://data-platform-alerts
      - email://data-owners@company.com
```

### 6. consumers

```yaml
consumers:
  - name: analytics-dashboard
    team: business-intelligence
    contact: bi-team@company.com
    sla: 6h
    columns_used:
      - account_id
      - account_name
      - annual_revenue
      - created_date

  - name: ml-churn-model
    team: data-science
    contact: ds-team@company.com
    sla: 24h
    columns_used:
      - account_id
      - annual_revenue
      - status
      - last_activity_date
```

### 7. metadata

```yaml
metadata:
  created_at: "2024-01-15"
  updated_at: "2024-06-01"
  tags:
    cost_center: CC-1234
    project: customer-360
    environment: production
  documentation_url: https://wiki.company.com/data/crm-accounts
  lineage:
    upstream: salesforce.Account
    downstream:
      - gold.dim_customer
      - gold.fact_revenue
```

### 8. security

```yaml
security:
  encryption:
    at_rest: SSE-KMS
    kms_key_alias: alias/data-lake-key
  access_control:
    engine: lake_formation
    tags:
      - key: classification
        values: [internal, pii]
      - key: domain
        values: [customer]
  row_filter: null
  column_masking:
    - column: email
      mask_for: [analyst-role]
      mask_type: sha256_hash
```

---

## Supported Types

| Type | Description | Example | Spark/Glue Mapping |
|---|---|---|---|
| `bigint` | 64-bit integer | 9223372036854775807 | LongType |
| `integer` | 32-bit integer | 2147483647 | IntegerType |
| `string` | Variable-length text | "hello world" | StringType |
| `decimal` | Fixed precision number | 12345.67 | DecimalType(p,s) |
| `double` | 64-bit floating point | 3.14159 | DoubleType |
| `date` | Calendar date | 2024-01-15 | DateType |
| `timestamp` | Date with time | 2024-01-15T10:30:00Z | TimestampType |
| `boolean` | True/false | true | BooleanType |
| `binary` | Raw bytes | (blob) | BinaryType |

---

## Classification Levels

| Level | Description | Handling |
|---|---|---|
| `public` | Non-sensitive, freely shareable | No restrictions |
| `internal` | Business data, company internal | Role-based access |
| `pii` | Personally identifiable information | Lake Formation tag, column masking |
| `pci` | Payment card data | Encrypted, audit logged, restricted |
| `phi` | Protected health information | HIPAA controls, encryption, audit |

---

## Constraint Reference

| Constraint | Parameters | Example |
|---|---|---|
| `unique` | none | Ensures no duplicate values |
| `min_value` | `value` | `min_value: 0` |
| `max_value` | `value` | `max_value: 999999` |
| `pattern` | `value` (regex) | `pattern: "^[A-Z]{2}\\d{6}$"` |
| `allowed_values` | `values` (list) | `allowed_values: [active, inactive]` |
| `max_length` | `value` | `max_length: 100` |
| `format` | `value` | `format: email`, `format: uuid` |

---

## Quality Section Deep Dive

### Freshness SLA
```yaml
freshness_sla_hours: 4  # Alert if no successful run within 4 hours
```

### Volume Bounds
```yaml
volume_bounds:
  min_rows: 1000          # Alert if fewer than 1000 rows
  max_rows: 5000000       # Alert if more than 5M rows (possible duplicate run)
  max_variance_pct: 50    # Alert if row count deviates >50% from rolling average
```

### Completeness Rules
```yaml
completeness:
  - column: account_id
    threshold: 1.0         # Hard requirement: 0% nulls allowed
  - column: phone_number
    threshold: 0.70        # Soft requirement: expect 70%+ populated
```

### Accuracy Rules (Spark SQL Syntax)
```yaml
accuracy:
  - name: valid_country_code
    sql: "LENGTH(country_code) = 2 AND country_code = UPPER(country_code)"
    threshold: 0.99

  - name: date_not_future
    sql: "created_date <= current_date()"
    threshold: 1.0

  - name: revenue_range
    sql: "annual_revenue BETWEEN 0 AND 1000000000 OR annual_revenue IS NULL"
    threshold: 0.995
```

---

## Evolution Policy Strategies

| Strategy | New Column | Type Widen | Column Remove | Type Narrow |
|---|---|---|---|---|
| `additive_auto` | Add as nullable | Allow | Quarantine + alert | Reject run |
| `strict` | Reject (manual review) | Reject | Reject | Reject |
| `permissive` | Add as nullable | Allow | Mark deprecated | Attempt cast |

### Actions

- **add_nullable** — Column added with NULL default, consumers unaffected
- **allow** — Change applied automatically (e.g., INT → BIGINT)
- **quarantine** — Records with issues sent to quarantine table, run continues
- **reject** — Pipeline run fails, manual intervention required
- **mark_deprecated** — Column retained but flagged, removed after consumer migration

---

## Consumers Section — Impact Analysis

When a contract change is proposed, the pipeline evaluates consumer impact:

```
Contract Change: Remove column "fax_number"
Impact Analysis:
  ✓ analytics-dashboard — does NOT use fax_number
  ✗ legacy-crm-sync — USES fax_number (BLOCKING)
Resolution: Contact legacy-crm-sync team before proceeding
```

---

## Complete Examples

### S3 File Source (No PII)

```yaml
contract:
  name: web_clickstream
  version: "1.0.0"
  status: active
  owner: analytics-engineering
  domain: digital
  description: "Website clickstream events from CDN logs"

source:
  type: s3_file
  format: json
  location: s3://raw-data-bucket/clickstream/
  compression: gzip
  partition_pattern: "year={yyyy}/month={MM}/day={dd}/"

schema:
  fields:
    - name: event_id
      type: string
      nullable: false
      classification: internal
      constraints:
        - type: unique
    - name: session_id
      type: string
      nullable: false
      classification: internal
    - name: page_url
      type: string
      nullable: false
      classification: internal
      constraints:
        - type: max_length
          value: 2048
    - name: event_timestamp
      type: timestamp
      nullable: false
      classification: internal
    - name: user_agent
      type: string
      nullable: true
      classification: internal
    - name: response_code
      type: integer
      nullable: false
      classification: internal
      constraints:
        - type: min_value
          value: 100
        - type: max_value
          value: 599

quality:
  freshness_sla_hours: 1
  volume_bounds:
    min_rows: 10000
    max_rows: 50000000
    max_variance_pct: 100
  completeness:
    - column: event_id
      threshold: 1.0
    - column: session_id
      threshold: 1.0
  accuracy:
    - name: valid_http_code
      sql: "response_code BETWEEN 100 AND 599"
      threshold: 1.0

evolution_policy:
  strategy: additive_auto
  actions:
    on_new_column: add_nullable
    on_type_widen: allow
    on_column_remove: quarantine
    on_type_narrow: reject

consumers:
  - name: web-analytics-dashboard
    team: digital-analytics
    contact: digital@company.com
    sla: 2h
    columns_used: [event_id, session_id, page_url, event_timestamp]

metadata:
  created_at: "2024-03-01"
  tags:
    cost_center: CC-5678
    project: web-analytics
  lineage:
    upstream: cdn.access_logs
    downstream: [gold.fact_page_views]

security:
  encryption:
    at_rest: SSE-S3
  access_control:
    engine: lake_formation
    tags:
      - key: classification
        values: [internal]
```

### JDBC Source (With PII + Lake Formation Tags)

```yaml
contract:
  name: hr_employees
  version: "1.2.0"
  status: active
  owner: hr-data-team
  domain: human_resources
  description: "Employee master data from Oracle HR system"

source:
  type: jdbc
  glue_connection_name: prod-oracle-hr
  schema: HR
  table: EMPLOYEES
  incremental_column: LAST_MODIFIED_DATE
  fetch_size: 5000

schema:
  fields:
    - name: employee_id
      type: integer
      nullable: false
      classification: internal
      constraints:
        - type: unique
    - name: full_name
      type: string
      nullable: false
      classification: pii
      constraints:
        - type: max_length
          value: 200
    - name: email
      type: string
      nullable: false
      classification: pii
      constraints:
        - type: pattern
          value: "^[\\w.-]+@company\\.com$"
    - name: ssn
      type: string
      nullable: false
      classification: pii
      constraints:
        - type: pattern
          value: "^\\d{3}-\\d{2}-\\d{4}$"
    - name: salary
      type: decimal
      precision: 10
      scale: 2
      nullable: false
      classification: pci
      constraints:
        - type: min_value
          value: 0
    - name: department
      type: string
      nullable: false
      classification: internal
    - name: hire_date
      type: date
      nullable: false
      classification: internal
    - name: last_modified_date
      type: timestamp
      nullable: false
      classification: internal

quality:
  freshness_sla_hours: 8
  volume_bounds:
    min_rows: 500
    max_rows: 100000
    max_variance_pct: 20
  completeness:
    - column: employee_id
      threshold: 1.0
    - column: full_name
      threshold: 1.0
    - column: ssn
      threshold: 1.0
  accuracy:
    - name: valid_ssn_format
      sql: "ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'"
      threshold: 1.0
    - name: hire_date_reasonable
      sql: "hire_date BETWEEN '1970-01-01' AND current_date()"
      threshold: 1.0

evolution_policy:
  strategy: strict
  actions:
    on_new_column: reject
    on_type_widen: reject
    on_column_remove: reject
    on_type_narrow: reject
  notification:
    channels:
      - email://hr-data-owners@company.com

consumers:
  - name: payroll-system
    team: payroll
    contact: payroll@company.com
    sla: 12h
    columns_used: [employee_id, full_name, salary, department]
  - name: hr-reporting
    team: people-analytics
    contact: people-analytics@company.com
    sla: 24h
    columns_used: [employee_id, department, hire_date]

metadata:
  created_at: "2024-02-01"
  updated_at: "2024-05-15"
  tags:
    cost_center: CC-HR-001
    project: hr-modernization
    compliance: sox
  lineage:
    upstream: oracle_hr.EMPLOYEES
    downstream: [gold.dim_employee, gold.fact_headcount]

security:
  encryption:
    at_rest: SSE-KMS
    kms_key_alias: alias/hr-data-key
  access_control:
    engine: lake_formation
    tags:
      - key: classification
        values: [pii, pci]
      - key: domain
        values: [human_resources]
      - key: compliance
        values: [sox]
  column_masking:
    - column: ssn
      mask_for: [analyst-role, bi-role]
      mask_type: sha256_hash
    - column: salary
      mask_for: [analyst-role]
      mask_type: nullify
```

---

## Translating Contracts to Pipeline Code

| Contract Section | Pipeline Artifact |
|---|---|
| `source.type` | Template selection (s3_file, jdbc, saas) |
| `schema.fields` | Spark StructType definition |
| `schema.fields[].classification: pii` | Lake Formation column-level tags |
| `quality.completeness` | Post-load null-check assertions |
| `quality.accuracy` | Spark SQL quality rule evaluation |
| `quality.freshness_sla_hours` | CloudWatch freshness alarm threshold |
| `quality.volume_bounds` | Row count validation after load |
| `evolution_policy.strategy` | Schema merge behavior in Glue job |
| `security.encryption` | S3 bucket encryption + KMS key |
| `security.access_control.tags` | Lake Formation tag associations |

---

## Contract Validation in Pipeline

```python
from dataclasses import dataclass
from typing import List, Dict, Any
import yaml

@dataclass
class ValidationResult:
    passed: bool
    rule_name: str
    details: str

def validate_contract(df, contract_path: str) -> List[ValidationResult]:
    """Validate a DataFrame against its data contract."""
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    results = []

    # Completeness checks
    for rule in contract.get("quality", {}).get("completeness", []):
        col_name = rule["column"]
        threshold = rule["threshold"]
        total = df.count()
        non_null = df.filter(df[col_name].isNotNull()).count()
        actual = non_null / total if total > 0 else 0
        results.append(ValidationResult(
            passed=actual >= threshold,
            rule_name=f"completeness_{col_name}",
            details=f"Expected >={threshold}, got {actual:.4f}"
        ))

    # Accuracy checks
    for rule in contract.get("quality", {}).get("accuracy", []):
        total = df.count()
        passing = df.filter(rule["sql"]).count()
        actual = passing / total if total > 0 else 0
        results.append(ValidationResult(
            passed=actual >= rule["threshold"],
            rule_name=rule["name"],
            details=f"Expected >={rule['threshold']}, got {actual:.4f}"
        ))

    # Volume bounds
    bounds = contract.get("quality", {}).get("volume_bounds", {})
    row_count = df.count()
    if bounds.get("min_rows"):
        results.append(ValidationResult(
            passed=row_count >= bounds["min_rows"],
            rule_name="volume_min",
            details=f"Got {row_count} rows, min={bounds['min_rows']}"
        ))
    if bounds.get("max_rows"):
        results.append(ValidationResult(
            passed=row_count <= bounds["max_rows"],
            rule_name="volume_max",
            details=f"Got {row_count} rows, max={bounds['max_rows']}"
        ))

    return results
```

---

## Best Practices

### Authoring
- Start with `status: draft` until schema stabilizes
- Use semantic versioning: MAJOR (breaking), MINOR (additive), PATCH (docs/quality)
- One contract per target table — never combine unrelated datasets
- Include `description` on every field for discoverability

### Quality Rules
- Set `freshness_sla_hours` to 2x your schedule interval as a starting point
- Use `volume_bounds.max_variance_pct` to catch duplicate runs early
- Write accuracy rules in Spark SQL syntax so they execute directly
- Start lenient (0.95 threshold) and tighten as data matures

### Evolution
- Use `additive_auto` for most pipelines — safe and low-friction
- Reserve `strict` for regulated data (SOX, HIPAA, PCI)
- Always notify consumers before removing columns, even with `permissive`

### PII Handling
- Mark all PII fields with `classification: pii`
- Use Lake Formation tags for automated column-level access control
- Apply `column_masking` for roles that need table access but not PII visibility
- Never log PII field values in pipeline output

### Consumers
- Require `columns_used` to enable precise impact analysis
- Set `sla` to the consumer's actual requirement, not the pipeline schedule
- Review consumer list quarterly — remove stale entries
