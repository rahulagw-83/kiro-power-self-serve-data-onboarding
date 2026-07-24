# Data Profiling

## Purpose

After data lands in S3, profile it and publish a report. The report is
**informational only** — it gives downstream teams the statistics and PII
candidates they need to plan their own processing. It does NOT enforce
anything or block pipelines.

---

## Preferred Service: AWS Glue DataBrew (First Source — Default)

For the **first source onboarded** (or first time profiling any source), default to
DataBrew without asking. It provides the richest output with zero code.

- Zero code — auto-generates comprehensive statistics
- Outputs JSON report to S3 + visual in DataBrew console
- Schedulable (run after each ingestion batch)
- Covers: types, nulls, distributions, cardinality, correlations
- Cost: ~$1.00/node per 30-min session

## Alternatives (Offer on 2nd+ Source)

When the user onboards subsequent sources, present cheaper options:

| Option | Cost (5 GB) | PII Detection | Full Statistics | When to Use |
|---|---|---|---|---|
| **DataBrew** | ~$2-4 | ✅ Built-in | ✅ Full | First source, visual stakeholder reports |
| **Athena SQL** | ~$0.03 | ✅ Custom regex | ✅ Custom SQL | Recurring, many sources, budget-constrained |
| **Amazon Macie** | ~$5 | ✅ Best accuracy | ❌ None | Compliance-focused PII scanning |
| **Glue Crawler** | Free | ❌ None | ❌ Schema only | Just need column names + types registered |

### Athena SQL Profiling (cheapest recurring option)

```sql
-- Column stats
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT order_id) as unique_order_ids,
  SUM(CASE WHEN customer_email IS NULL THEN 1 ELSE 0 END) as email_nulls,
  MIN(order_date) as earliest_date,
  MAX(order_date) as latest_date,
  AVG(total_amount) as avg_amount
FROM "default"."landing_orders"
WHERE year = '2025' AND month = '07' AND day = '21';

-- PII detection via regex
SELECT column_name, 
  SUM(CASE WHEN regexp_like(CAST(column_value AS VARCHAR), '[\w.+-]+@[\w-]+\.[\w.-]+') THEN 1 ELSE 0 END) as email_matches
FROM ...
```

### Amazon Macie (best PII accuracy)

```hcl
resource "aws_macie2_classification_job" "landing_scan" {
  name          = "${var.source_name}-pii-scan"
  job_type      = "ONE_TIME"

  s3_job_definition {
    bucket_definitions {
      account_id = data.aws_caller_identity.current.account_id
      buckets    = [var.landing_bucket]
    }
    scoping {
      includes {
        and {
          simple_scope_term {
            key        = "OBJECT_KEY"
            comparator = "STARTS_WITH"
            values     = ["${var.domain}/${var.source_name}/"]
          }
        }
      }
    }
  }
}
```

**Alternative:** Athena SQL queries for lightweight custom profiling when
DataBrew is not available or cost is a concern.

---

## Terraform Configuration

```hcl
resource "aws_glue_catalog_database" "profiling" {
  name = "${var.source_name}_profiling"
}

resource "aws_databrew_dataset" "landing" {
  name = "${var.source_name}-landing-dataset"

  input {
    s3 {
      bucket = var.landing_bucket
      key    = "${var.domain}/${var.source_name}/${var.table_name}/"
    }
  }

  format = "PARQUET"  # or CSV, JSON based on landing format
}

resource "aws_databrew_profile_job" "profile" {
  name         = "${var.source_name}-profile-job"
  dataset_name = aws_databrew_dataset.landing.name
  role_arn     = var.databrew_role_arn

  output_location {
    bucket = var.landing_bucket
    key    = "reports/${var.source_name}/"
  }

  configuration {
    dataset_statistics_configuration {
      included_statistics = ["ALL"]
    }
    column_statistics_configurations {
      statistics {
        included_statistics = ["ALL"]
      }
    }
  }
}
```

---

## Profiling Report Contents

| Section | Details |
|---|---|
| **Schema** | Column names, inferred types, nullable % |
| **PII Candidates** | Flagged columns (see patterns below) |
| **Statistics** | Min, max, mean, stddev, p25/p50/p75/p95 |
| **Cardinality** | Unique count / total count per column |
| **Top-N Values** | Most frequent values (low-cardinality columns) |
| **Nulls** | Count + percentage per column |
| **Samples** | 5 representative non-null values per column |
| **Volume** | Row count, file size, file count |
| **Freshness** | Most recent timestamp value in the data |
| **Recommendations** | Suggested partitions, keys, quality observations |

---

## PII Detection Patterns

Applied by column name pattern AND/OR value regex:

| Category | Column Name Pattern | Value Pattern |
|---|---|---|
| Email | `email`, `e_mail`, `email_address` | `[\w.+-]+@[\w-]+\.[\w.-]+` |
| Phone | `phone`, `mobile`, `cell`, `tel`, `fax` | `(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}` |
| Name | `first_name`, `last_name`, `full_name`, `surname` | — |
| Address | `address`, `street`, `city`, `zip`, `postal` | — |
| SSN | `ssn`, `social_security`, `sin_number` | `\d{3}-\d{2}-\d{4}` |
| Financial | `card_number`, `credit_card`, `cvv`, `pan` | Luhn-valid 13-19 digit sequences |
| DOB | `dob`, `date_of_birth`, `birth_date` | — |

**Output format in report:**
```json
{
  "pii_candidates": [
    {"column": "customer_email", "category": "email", "confidence": "high", "detection": "name+value"},
    {"column": "phone_number", "category": "phone", "confidence": "high", "detection": "name+value"},
    {"column": "addr_line_1", "category": "address", "confidence": "medium", "detection": "name_only"}
  ]
}
```

---

## Quality Observations (Informational — NOT Enforced)

The report notes patterns that downstream teams can use:

| Observation | Example |
|---|---|
| Never-null column | "order_id is never null (0.0% null rate)" |
| Low cardinality | "status has 5 distinct values: [ACTIVE, INACTIVE, PENDING, CLOSED, ARCHIVED]" |
| Always positive | "amount is always >= 0 (min: 0.01, max: 99999.99)" |
| Monotonic ID | "transaction_id appears monotonically increasing" |
| High uniqueness | "customer_id is 99.8% unique (potential primary key)" |
| Date range | "order_date ranges from 2020-01-01 to 2025-07-21" |

These are **suggestions only**. The power does not enforce them.

---

## Report Output

| Format | Location | Purpose |
|---|---|---|
| JSON | `s3://{bucket}/reports/{source_name}/profile-{timestamp}.json` | Machine-readable |
| Markdown | `{workspace}/reports/{source_name}-profile.md` | Human-readable |
| DataBrew Console | AWS Console → DataBrew → Profile results | Visual, interactive |

---

## Scheduling

| Pattern | Configuration |
|---|---|
| After each ingestion | EventBridge rule triggers DataBrew job on S3 PutObject |
| Daily summary | Cron schedule on DataBrew job |
| On-demand | User requests "profile the data that landed" |

---

## MUST Rules

- MUST run profiling on Landing S3 data (not on source directly)
- MUST flag PII candidates using both name AND value patterns
- MUST publish report in JSON + Markdown formats
- MUST NOT enforce any quality rule based on profiling results
- MUST NOT block pipelines based on profiling findings
- MUST NOT transform data as part of profiling
- MUST include volume stats (row count, file size) in every report
