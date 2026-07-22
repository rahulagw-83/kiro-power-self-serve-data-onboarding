---
name: schema-inference-and-classification
description: >-
  Automatically detect schema structure, data types, nullability, and sensitivity
  classification from source data samples. Samples from Landing S3 (preferred) or
  directly from source. Produces a versioned schema document with PII/PHI tags that
  drives data contract authoring and DQDL rule compilation. Triggers on: infer schema,
  detect columns, classify data, find PII, schema discovery, sample source, what columns.
  Do NOT use for creating tables (use creating-data-lake-table), writing contracts
  (use data-contract-authoring), or querying existing tables (use querying-data-lake).
version: 2
argument-hint: '[source-name|s3://landing-path|connection-name]'
author: "Rahul Agarwal, Manish Choudhary"
---

# Schema Inference and Classification

Automatically sample a data source, detect its schema, infer canonical types, assess
nullability and cardinality, and classify each column for sensitivity (public, internal,
restricted, PHI). The output is a versioned schema document that feeds directly into
data contract authoring and DQDL rule compilation.

**Two-layer context:** Prefer sampling from Landing S3 files (if already available from
an initial DMS/AppFlow/Firehose load). If Landing is not yet populated, sample directly
from the source. PII/PHI columns are tagged in Lake Formation at the Raw layer — they
are NOT masked here (masking belongs in Silver).

## Philosophy

**Infer first, confirm second.** The system does the heavy lifting of type detection and
sensitivity classification. The human reviews and corrects. This reduces onboarding time
from hours of manual column-by-column analysis to minutes of review.

## Onboarding Workflow Position

This skill executes **Step 6 (Infer Schema)** and **Step 7 (Review Classification)** of the
master onboarding workflow in `steering/onboarding-workflow.md`.

**Inputs:** A connected and verified source (output of `connecting-to-data-source` skill or
validated S3 path).

**Outputs:** A structured schema document stored in the pipeline workspace as
`schema-inference-result.yaml`, ready for `data-contract-authoring` skill.

## Workflow

### 1. Validate Source Access

Before sampling, confirm the source is reachable:

| Source Type | Validation |
|---|---|
| S3 file | `aws s3api head-object --bucket {bucket} --key {key}` succeeds |
| JDBC table | Glue connection test passed (via `connecting-to-data-source`) |
| API endpoint | Health check returns 200 |

If validation fails, do NOT proceed. Redirect to `connecting-to-data-source` for troubleshooting.

### 2. Sample Source Data

Pull a representative sample for inference:

| Source Type | Sample Strategy | Minimum Rows |
|---|---|---|
| S3 file < 10 MB | Read entire file | All rows |
| S3 file >= 10 MB | Read first 5000 rows | 5000 |
| JDBC table | `SELECT * FROM {schema}.{table} LIMIT 5000` | 1000 (minimum) |
| JDBC with watermark | Sample recent data: `WHERE {watermark_col} > NOW() - INTERVAL '7 days' LIMIT 5000` | 1000 |
| API response | Fetch 3 pages of results | 500 |

**Platform rules:**
- MUST sample at least 1000 rows for statistical significance
- MUST sample from recent data when possible (avoids stale schema)
- MUST NOT read entire large tables — cap at 5000 rows
- MUST NOT log or store raw sample data beyond the inference session

### 3. Infer Column Types

Map source-native types to the platform's canonical type system:

| Canonical Type | Maps From |
|---|---|
| `string` | VARCHAR, TEXT, CHAR, NVARCHAR, String, object |
| `integer` | INT, INTEGER, SMALLINT, BIGINT, Number (no decimals) |
| `long` | BIGINT, INT8, Number > 2^31 |
| `decimal(p,s)` | DECIMAL, NUMERIC, FLOAT, DOUBLE, Number (with decimals) |
| `boolean` | BOOLEAN, BIT, true/false strings |
| `date` | DATE |
| `timestamp` | TIMESTAMP, DATETIME, TIMESTAMPTZ |
| `binary` | BLOB, BYTEA, BINARY, VARBINARY |
| `array` | ARRAY, JSON array patterns |
| `struct` | Nested JSON objects, composite types |

**For untyped sources (CSV, JSON):**

Apply inference heuristics on sample values:
1. Try parsing as integer → long → decimal → boolean → timestamp → date
2. If all attempts fail, classify as string
3. If > 90% of non-null values parse as a type, assign that type
4. If mixed, use the widest compatible type (e.g., integer + decimal → decimal)

### 4. Assess Column Properties

For each column, determine:

| Property | Detection Method | Threshold |
|---|---|---|
| **Nullable** | Count NULLs in sample | > 0 NULLs → nullable=true |
| **Cardinality** | COUNT(DISTINCT) / COUNT(*) | < 0.01 → low cardinality (potential enum) |
| **Uniqueness** | COUNT(DISTINCT) == COUNT(*) | If true → potential primary key |
| **Completeness** | (total - nulls) / total * 100 | Report as percentage |
| **Sample values** | 5 representative non-null values | Pick diverse examples |
| **Min/Max** | For numeric and date types | Useful for range validation |
| **Average length** | For string types | Useful for storage estimation |

### 5. Classify Sensitivity

Apply pattern matching rules to column names AND sample values:

**Column name patterns:**

| Pattern (case-insensitive) | Classification |
|---|---|
| `email`, `e_mail`, `email_address` | restricted |
| `ssn`, `social_security`, `sin_number` | restricted |
| `phone`, `mobile`, `cell`, `fax`, `tel` | restricted |
| `address`, `street`, `city`, `zip`, `postal` | restricted |
| `first_name`, `last_name`, `full_name`, `surname` | restricted |
| `dob`, `date_of_birth`, `birth_date` | restricted |
| `credit_card`, `card_number`, `cvv`, `pan` | restricted |
| `passport`, `license_number`, `driver_license` | restricted |
| `ip_address`, `mac_address`, `device_id` | internal |
| `salary`, `compensation`, `revenue`, `profit` | internal |
| `diagnosis`, `medication`, `icd_code`, `procedure_code` | phi |
| `patient_id`, `mrn`, `medical_record` | phi |
| `health_plan`, `insurance_id`, `claim_number` | phi |

**Value patterns (regex on sample data):**

| Pattern | Classification |
|---|---|
| `\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b` (email) | restricted |
| `\b\d{3}-\d{2}-\d{4}\b` (US SSN) | restricted |
| `\b\d{3}[\s.-]?\d{3}[\s.-]?\d{4}\b` (US phone) | restricted |
| `\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b` (credit card) | restricted |
| `\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b` (IPv4) | internal |

**Classification levels:**

| Level | Meaning | Pipeline Implications |
|---|---|---|
| `public` | No restrictions | Standard processing |
| `internal` | Business-sensitive, not regulated | Tag in Lake Formation |
| `restricted` | PII — regulated data | Mask in non-prod, encrypt with KMS, column-level access |
| `phi` | Protected health information | All of restricted + privacy officer approval required |

### 6. Generate Schema Document

Produce a structured YAML output:

```yaml
# schema-inference-result.yaml
schema_inference:
  source_name: "{source_name}"
  source_type: "{s3|jdbc|api}"
  inferred_at: "{ISO 8601 timestamp}"
  sample_size: {rows_sampled}
  confidence: "{high|medium|low}"

  columns:
    - name: "{column_name}"
      canonical_type: "{type}"
      source_type: "{original_native_type}"
      nullable: {true|false}
      completeness_pct: {0-100}
      cardinality: "{high|medium|low}"
      uniqueness_pct: {0-100}
      sensitivity: "{public|internal|restricted|phi}"
      sensitivity_reason: "{pattern that triggered classification}"
      sample_values: ["{val1}", "{val2}", "{val3}"]
      min_value: "{min}"  # numeric/date only
      max_value: "{max}"  # numeric/date only

  primary_key_candidates: ["{col1}", "{col2}"]
  suggested_partition_columns: ["{col}"]
  suggested_merge_keys: ["{col1}", "{col2}"]

  sensitivity_summary:
    public: {count}
    internal: {count}
    restricted: {count}
    phi: {count}

  risk_assessment:
    level: "{low|medium|high}"
    reason: "{explanation}"
    requires_approval: {true|false}
    approvers: ["{role1}", "{role2}"]
```

### 7. Present for Review

Display the schema inference results in a table format for human review:

```
| # | Column | Type | Nullable | Sensitivity | Reason | Action Required |
|---|--------|------|----------|-------------|--------|-----------------|
| 1 | order_id | string | no | public | — | — |
| 2 | customer_email | string | yes | restricted | email pattern | Mask in non-prod |
| 3 | total_amount | decimal(10,2) | no | internal | financial field | — |
| 4 | diagnosis_code | string | yes | phi | diagnosis pattern | Privacy officer review |
```

**Decision gates after review:**
- If ALL columns are public/internal → auto-approve, proceed to contract authoring
- If ANY column is restricted → user must confirm masking strategy before proceeding
- If ANY column is PHI → privacy officer approval required before proceeding
- User may override any classification (up or down) with justification logged

### 8. Store and Version

Store the approved schema document:
- Save `schema-inference-result.yaml` in the pipeline workspace
- Record in Source Registry: update DynamoDB with schema version and sensitivity summary
- If this is a re-inference (schema evolution check), compare against stored version

## Confidence Scoring

| Condition | Confidence |
|---|---|
| Sample >= 5000 rows, typed source (JDBC) | High |
| Sample >= 1000 rows, typed source | High |
| Sample >= 1000 rows, untyped source (CSV) | Medium |
| Sample < 1000 rows | Medium |
| Sample < 100 rows or parsing errors | Low |

Low confidence schemas MUST be flagged for manual review regardless of sensitivity.

## Gotchas

- CSV files with no header: MUST ask user for column names or use positional naming (col_0, col_1...)
- JSON with nested structures: flatten to dot-notation for top-level schema, note nested types
- Multiline JSON: ensure parser handles newline-delimited vs. array-wrapped formats
- Encoding issues: detect and report encoding (UTF-8, Latin-1, etc.) — do not silently corrupt
- Empty columns: if > 95% null, flag as "potentially unused" — do not auto-exclude
- Mixed types in same column: classify as string and flag for manual review

## MUST Rules

- MUST sample at least 1000 rows before declaring a schema
- MUST classify sensitivity on BOTH column names and sample values
- MUST NOT log or persist raw PII/PHI values from samples (use redacted samples in reports)
- MUST version every schema document immutably
- MUST present classification for human review before proceeding to contract
- MUST record the confidence level and sample size in the output
- MUST flag low-confidence inferences for manual review
