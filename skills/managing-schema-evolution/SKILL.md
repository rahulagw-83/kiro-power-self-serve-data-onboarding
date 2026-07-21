---
name: managing-schema-evolution
description: >-
  Detect schema drift between source and contract, classify change severity, apply
  evolution policies automatically for safe changes, and coordinate human review for
  breaking changes. Handles Iceberg ALTER TABLE operations, contract version bumps,
  and consumer notifications. Triggers on: schema changed, new column, column missing,
  type mismatch, schema drift, evolution, migrate schema, alter table.
  Do NOT use for initial schema inference (use schema-inference-and-classification),
  creating tables (use creating-data-lake-table), or writing contracts (use
  data-contract-authoring).
version: 1
argument-hint: '[source-name|table-name|''check-all'']'
author: "Rahul Agarwal, Manish Choudhary"
---

# Managing Schema Evolution

Detect, classify, and handle schema changes between the source system and the platform's
data contract. Applies the evolution policy defined in the contract to automatically
handle safe changes while escalating breaking changes for human review.

## Philosophy

**Schema change is inevitable — data loss is not.** Sources evolve. Columns get added,
types widen, fields disappear. The platform handles this gracefully: safe changes flow
through automatically, dangerous changes pause the pipeline and alert humans. No data
is ever lost due to unhandled schema drift.

## Onboarding Workflow Position

This skill operates **post-deployment** as an ongoing operational capability. It is
triggered when:
- A pipeline run detects a schema mismatch between source and contract
- A scheduled drift-detection check finds changes
- A user manually requests a schema evolution check

## Workflow

### 1. Detect Drift

Compare the current source schema against the stored contract schema:

**For JDBC sources:**
```bash
# Re-infer current source schema (lightweight — metadata only, no full sample)
aws glue get-table --database-name {db} --name {table} \
  --query "Table.StorageDescriptor.Columns"
```

**For S3 sources:**
Sample the latest file/partition and infer current schema.

**For API sources:**
Fetch the latest response and compare structure.

**Comparison logic:**

```
For each column in SOURCE schema:
  If column exists in CONTRACT schema:
    Compare type → detect type changes
    Compare nullability → detect constraint changes
  If column NOT in CONTRACT schema:
    → New column detected (additive change)

For each column in CONTRACT schema:
  If column NOT in SOURCE schema:
    → Missing column detected (potentially breaking)
```

### 2. Classify Changes

Each detected change is classified by severity:

| Change Type | Severity | Examples |
|---|---|---|
| New nullable column | Low (additive) | Source added `middle_name VARCHAR NULL` |
| Type widening | Low (safe cast) | `INT` → `BIGINT`, `VARCHAR(50)` → `VARCHAR(100)` |
| Nullability relaxed | Low | `NOT NULL` → `NULL` (previously required, now optional) |
| New required column | Medium | Source added `region_code VARCHAR NOT NULL` |
| Nullability tightened | Medium | `NULL` → `NOT NULL` (may fail existing data) |
| Type narrowing | High (data loss risk) | `BIGINT` → `INT`, `DECIMAL(10,2)` → `DECIMAL(5,2)` |
| Column removed | High | Source dropped `legacy_field` |
| Column renamed | High | Treated as remove + add (cannot auto-detect rename) |
| Type incompatible change | Critical | `STRING` → `INT` (existing data may not cast) |

### 3. Apply Evolution Policy

Read the evolution policy from the data contract and apply:

**Policy: `additive_auto`**

| Change Severity | Action |
|---|---|
| Low (additive) | Auto-apply. Bump contract MINOR version. Log change. |
| Medium | Alert data steward. Pause until reviewed. |
| High / Critical | Halt pipeline. Alert data steward + source team. |

**Policy: `additive_review`**

| Change Severity | Action |
|---|---|
| Low (additive) | Alert data steward. Apply after review (24h timeout). |
| Medium | Alert data steward. Pause until explicit approval. |
| High / Critical | Halt pipeline. Alert data steward + source team. |

**Policy: `strict`**

| Change Severity | Action |
|---|---|
| Any change | Halt pipeline. Require data steward + privacy officer approval. |

### 4. Execute Schema Changes

For approved changes, apply them to the target Iceberg table:

**Add nullable column:**
```sql
ALTER TABLE {catalog}.{database}.{table}
ADD COLUMNS ({column_name} {iceberg_type});
```

**Add to quarantine table as well:**
```sql
ALTER TABLE {catalog}.{database}.{table}_quarantine
ADD COLUMNS ({column_name} {iceberg_type});
```

**Type widening (safe cast):**
Iceberg supports type promotion for:
- `int` → `long`
- `float` → `double`
- `decimal(P1,S)` → `decimal(P2,S)` where P2 > P1

For supported promotions:
```sql
ALTER TABLE {catalog}.{database}.{table}
ALTER COLUMN {column_name} TYPE {new_type};
```

For unsupported changes, add a new column and deprecate the old:
```sql
ALTER TABLE {catalog}.{database}.{table}
ADD COLUMNS ({column_name}_v2 {new_type});
-- Mark old column as deprecated in contract metadata
```

### 5. Update Contract Version

After applying changes, bump the contract version:

```yaml
# Before
contract:
  version: "1.2.0"

# After (additive change)
contract:
  version: "1.3.0"
  
# After (breaking change — approved)
contract:
  version: "2.0.0"
```

Update the contract in DynamoDB:
- New version entry with full schema
- Change log entry documenting what changed and why
- Timestamp and approver recorded

### 6. Notify Consumers

If the contract has registered consumers, notify them:

| Change Type | Notification |
|---|---|
| MINOR (additive) | Informational — "New column available: {name}" |
| MAJOR (breaking) | Warning — "Breaking change: {description}. Action required by {date}." |
| Deprecation | Notice — "Column {name} deprecated. Will be removed in version {X}." |

### 7. Handle Unresolvable Drift

When drift cannot be automatically resolved:

1. **Pause the pipeline** — prevent data with new schema from mixing with old
2. **Quarantine new data** — if pipeline was mid-run, route to quarantine
3. **Alert stakeholders** — data steward, source team, affected consumers
4. **Document the issue** — record in Source Registry with status_metadata
5. **Provide remediation options:**
   - Accept the change (update contract, apply ALTER TABLE)
   - Reject the change (coordinate with source team to revert)
   - Transform the change (add mapping logic to handle both schemas)

## Scheduled Drift Detection

For proactive detection, run drift checks on a schedule:

```
Every 6 hours:
  For each active pipeline:
    1. Query source schema (lightweight — no data sampling)
    2. Compare against contract
    3. If drift detected → classify and apply policy
    4. If no drift → log "schema stable" metric
```

This catches changes before the next pipeline run, giving time to handle them
before data quality is impacted.

## Schema Change History

Maintain a full history for audit:

```yaml
schema_changes:
  - version: "1.3.0"
    timestamp: "2024-07-15T10:30:00Z"
    change_type: "additive"
    description: "Added nullable column: loyalty_tier (string)"
    detected_by: "scheduled_drift_check"
    approved_by: "auto (additive_auto policy)"
    applied_at: "2024-07-15T10:30:05Z"
    
  - version: "2.0.0"
    timestamp: "2024-08-01T14:00:00Z"
    change_type: "breaking"
    description: "Column removed: legacy_status"
    detected_by: "pipeline_run_failure"
    approved_by: "data_steward@company.com"
    applied_at: "2024-08-01T16:30:00Z"
    consumer_notification_sent: true
```

## MUST Rules

- MUST compare against the contract schema (not just the table schema)
- MUST classify every change by severity before taking action
- MUST follow the evolution policy defined in the contract — no overrides without approval
- MUST apply changes to BOTH the main table and quarantine table
- MUST bump contract version for every schema change
- MUST notify registered consumers of breaking changes
- MUST NOT auto-apply breaking changes regardless of policy
- MUST NOT drop columns from Iceberg tables (deprecate instead)
- MUST log full change history for audit purposes
- MUST pause pipeline on unresolvable drift — never process data with mismatched schema

## Gotchas

- Iceberg supports limited type promotion — not all "widening" is supported natively
- Column renames cannot be detected automatically — they appear as drop + add
- Nullable → not-null changes require backfill verification before applying
- S3 Tables enforce lowercase — schema changes must respect this constraint
- Concurrent schema changes from multiple detection runs can conflict — use optimistic locking
- Deprecated columns still consume storage — plan removal in a future MAJOR version
