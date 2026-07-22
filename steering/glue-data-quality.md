---
inclusion: fileMatch
fileMatchPattern: '**/data-contract*'
---

# Glue Data Quality (DQDL)

## Purpose

Replace hand-rolled Python quality validators with AWS Glue Data Quality.
Data contract rules compile to DQDL syntax at pipeline generation time.

---

## Why DQDL Over Custom Code

| Aspect | Custom Python Validator | Glue Data Quality (DQDL) |
|---|---|---|
| Maintenance | You own the code forever | AWS-managed, zero code |
| Visibility | Custom dashboards needed | Results in Glue Console natively |
| Catalog integration | Manual | Automatic lineage + quality scores |
| Rule syntax | Python classes | Declarative DQDL one-liners |
| Scaling | Manual parallelism | Runs on Glue infrastructure |

---

## Compiling Data Contract → DQDL

Each quality rule in `data-contract.yaml` maps to a DQDL rule:

| Contract Rule Type | DQDL Equivalent | Example |
|---|---|---|
| `not_null` | `Completeness "{col}" = 1.0` | `Completeness "order_id" = 1.0` |
| `completeness` (threshold) | `Completeness "{col}" >= {threshold}` | `Completeness "email" >= 0.95` |
| `unique` | `IsUnique "{col}"` | `IsUnique "order_id"` |
| `range` (min/max) | `ColumnValues "{col}" between {min} and {max}` | `ColumnValues "age" between 0 and 150` |
| `min_value` | `ColumnValues "{col}" >= {min}` | `ColumnValues "amount" >= 0` |
| `max_value` | `ColumnValues "{col}" <= {max}` | `ColumnValues "quantity" <= 10000` |
| `allowed_values` | `ColumnValues "{col}" in [{values}]` | `ColumnValues "status" in ["A","B","C"]` |
| `pattern` (regex) | `ColumnValues "{col}" matches "{regex}"` | `ColumnValues "email" matches ".*@.*\\..*"` |
| `freshness` | `CustomSql "SELECT CASE WHEN ..." = 1` | See below |
| `cross_field` | `CustomSql "SELECT COUNT(*) FROM primary WHERE ..." = 0` | See below |
| `row_count_min` | `RowCount >= {min}` | `RowCount >= 100` |
| `row_count_max` | `RowCount <= {max}` | `RowCount <= 1000000` |

---

## DQDL Rule Examples

### Basic rules (generated from contract):

```
Rules = [
    Completeness "order_id" = 1.0,
    Completeness "customer_email" >= 0.95,
    IsUnique "order_id",
    ColumnValues "total_amount" >= 0,
    ColumnValues "status" in ["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"],
    RowCount >= 100,
    RowCount <= 5000000
]
```

### Cross-field validation:

```
Rules = [
    CustomSql "SELECT COUNT(*) FROM primary WHERE updated_at < created_at" = 0,
    CustomSql "SELECT COUNT(*) FROM primary WHERE end_date < start_date" = 0
]
```

### Freshness check (within SLA window):

```
Rules = [
    CustomSql "SELECT CASE WHEN MAX(updated_at) >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR THEN 1 ELSE 0 END FROM primary" = 1
]
```

---

## Integration with Glue ETL Job

Add DQDL evaluation to the Glue job after reading from Landing S3:

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
gc = GlueContext(sc)

# Read from Landing S3
df = gc.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://{bucket}/landing/{source}/{table}/"]},
    format="parquet"
)

# Apply DQDL rules
from awsglue.transforms import EvaluateDataQuality

dq_results = EvaluateDataQuality.apply(
    frame=df,
    ruleset="""
        Rules = [
            Completeness "order_id" = 1.0,
            IsUnique "order_id",
            ColumnValues "total_amount" >= 0
        ]
    """,
    publishing_options={
        "dataQualityEvaluationContext": "{source_name}_{table_name}_quality",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# Route pass/fail
pass_df = dq_results.filter(lambda r: r["DataQualityEvaluationResult"] == "Pass")
fail_df = dq_results.filter(lambda r: r["DataQualityEvaluationResult"] == "Fail")

# Write pass rows to Raw Iceberg
# Write fail rows to quarantine table
```

---

## Quarantine Integration

Failed DQDL rows are routed to `{table}_quarantine` with:
- Original row data
- `_error_reason`: which DQDL rule failed
- `_quarantined_at`: timestamp of quality check
- `_batch_id`: correlating batch identifier

If `fail_count / total_count > 0.05` (5% threshold), halt pipeline and fire SNS alarm.

---

## Viewing Results

DQDL results appear automatically in:
- **Glue Console** → Data Quality tab on the table
- **CloudWatch Metrics** → namespace `Glue/DataQuality`
- **Glue Data Catalog** → table quality scores and history

No custom dashboard code needed.

---

## MUST Rules

- MUST compile ALL data contract quality rules to DQDL syntax
- MUST NOT maintain custom Python validator classes (use DQDL exclusively)
- MUST enable CloudWatch metrics publishing for every DQDL evaluation
- MUST route DQDL failures to the quarantine table (never drop)
- MUST halt pipeline if quarantine rate exceeds contract threshold (default 5%)
- MUST use `CustomSql` for cross-field and freshness rules that DQDL primitives cannot express
