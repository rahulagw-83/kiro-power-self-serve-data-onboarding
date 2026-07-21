---
name: data-contract-authoring
description: >-
  Generate, validate, and manage data-contract.yaml files from inferred schemas.
  Produces machine-readable contracts with schema definitions, quality rules, SLA
  targets, evolution policies, and consumer tracking. Handles contract versioning,
  approval routing, and lifecycle management. Triggers on: create contract, generate
  contract, data contract, quality rules, SLA, schema agreement, define contract.
  Do NOT use for schema inference (use schema-inference-and-classification), querying
  data (use querying-data-lake), or deploying pipelines (use deploying-cdk-pipeline).
version: 1
argument-hint: '[source-name|schema-file-path]'
author: MANIOX
---

# Data Contract Authoring

Generate and manage data contracts — the formal agreements that define what data looks
like, how fresh it must be, what quality standards apply, and how schema changes are
handled. Contracts are the governance foundation of every pipeline on this platform.

## Philosophy

**No pipeline without a contract.** Every data source that enters the platform MUST have
an active, versioned data contract before any production pipeline is deployed. Contracts
are living documents that evolve with the source but always through controlled, versioned
changes.

## Onboarding Workflow Position

This skill executes **Step 8 (Generate Draft Contract)** and **Step 9 (Review & Approve
Contract)** of the master onboarding workflow.

**Inputs:** Schema inference result (`schema-inference-result.yaml`) from the
`schema-inference-and-classification` skill.

**Outputs:** A validated `data-contract.yaml` stored in the pipeline workspace, registered
in DynamoDB with version tracking.

## Workflow

### 1. Load Schema Inference

Read the schema inference output and extract:
- Column definitions (name, type, nullable, sensitivity)
- Primary key candidates
- Suggested partition columns
- Sensitivity summary and risk assessment

If no schema inference exists, redirect to `schema-inference-and-classification` first.

### 2. Gather Contract Metadata

Collect from the `onboarding-request.yaml` or ask the user:

| Field | Source | Default |
|---|---|---|
| Contract name | source_name from request | Required — no default |
| Owner | owner from request | Required — no default |
| Domain | domain from request | Required — no default |
| Description | description from request | Required — no default |
| Freshness SLA (hours) | schedule frequency | 24h for daily, 1h for hourly |
| Retention (days) | domain policy | 2555 (7 years) for raw |
| Write mode | source type heuristic | append for S3, merge for JDBC |

### 3. Generate Quality Rules

Auto-generate quality rules based on schema properties:

**From column properties:**

| Column Property | Generated Rule |
|---|---|
| nullable = false | `not_null` check on that column |
| uniqueness_pct = 100% | `unique` check (primary key candidate) |
| cardinality = low (< 50 distinct values) | `allowed_values` check with discovered values |
| type = timestamp, is incremental column | `freshness` check (max value within SLA window) |
| type = numeric, has min/max in sample | `range` check (min <= value <= max) |
| type = string, matches known pattern | `pattern` check (email regex, phone regex, etc.) |
| sensitivity = restricted or phi | `masking_required` flag for non-prod environments |

**Cross-column rules (inferred from common patterns):**

| Pattern Detected | Generated Rule |
|---|---|
| `created_at` and `updated_at` both exist | `updated_at >= created_at` |
| `start_date` and `end_date` both exist | `end_date >= start_date` |
| `quantity` and `unit_price` and `total` exist | `total ≈ quantity * unit_price` (tolerance 0.01) |

**Volume bounds (from historical data or estimate):**

```yaml
volume:
  min_rows_per_batch: 100        # alert if below (source may be empty)
  max_rows_per_batch: 1000000    # alert if above (unexpected dump)
  anomaly_threshold_pct: 50      # alert if row count changes > 50% from 7-day avg
```

### 4. Define Evolution Policy

Set schema evolution rules based on sensitivity and domain:

| Source Sensitivity | Default Evolution Policy |
|---|---|
| public / internal | `additive_auto` — new nullable columns added automatically |
| restricted | `additive_review` — new columns require data steward review |
| phi | `strict` — any change requires privacy officer + data steward |

Evolution policy structure:

```yaml
evolution_policy:
  mode: "additive_auto"          # additive_auto | additive_review | strict
  allowed_changes:
    - add_nullable_column        # always safe
    - widen_type                 # INT -> BIGINT, VARCHAR(50) -> VARCHAR(100)
  blocked_changes:
    - remove_column              # always requires review
    - narrow_type                # BIGINT -> INT could lose data
    - rename_column              # treated as drop + add
  on_drift_detected:
    additive: "auto_apply"       # apply and bump MINOR version
    breaking: "pause_and_alert"  # halt pipeline, notify steward
  version_bump:
    additive: "minor"            # 1.0.0 -> 1.1.0
    breaking: "major"            # 1.0.0 -> 2.0.0
    metadata_only: "patch"       # 1.0.0 -> 1.0.1
```

### 5. Compose the Contract

Assemble the full `data-contract.yaml`:

```yaml
contract:
  name: "{source_name}"
  version: "1.0.0"
  status: "draft"
  created_at: "{ISO 8601}"
  owner: "{owner}"
  domain: "{domain}"
  description: "{description}"

source:
  type: "{s3|jdbc|saas}"
  system: "{system_name}"
  connection_name: "{glue_connection}"   # if applicable

schema:
  fields:
    - name: "{column_name}"
      type: "{canonical_type}"
      nullable: {true|false}
      sensitivity: "{public|internal|restricted|phi}"
      description: "{auto-generated or user-provided}"
      constraints:
        - type: "{not_null|unique|range|pattern|allowed_values}"
          parameters: {}

quality:
  rules:
    - name: "{rule_name}"
      type: "{not_null|unique|range|pattern|freshness|custom_sql}"
      column: "{column_name}"            # or null for table-level
      parameters: {}
      severity: "{error|warning}"        # error = quarantine, warning = alert only
  thresholds:
    max_null_pct: 5
    max_duplicate_pct: 0
    quarantine_rate_halt_pct: 5

freshness:
  sla_hours: {N}
  alert_after_hours: {N * 0.8}
  critical_after_hours: {N * 1.5}

volume:
  min_rows_per_batch: {N}
  max_rows_per_batch: {N}
  anomaly_threshold_pct: 50

evolution_policy:
  mode: "{additive_auto|additive_review|strict}"
  allowed_changes: [...]
  blocked_changes: [...]

retention:
  raw_days: 2555
  curated_days: 730
  quarantine_days: 90

partitioning:
  columns: ["_ingested_date"]
  strategy: "daily"

consumers: []                           # populated as consumers register

metadata:
  tags:
    environment: "prod"
    cost_center: "{from request}"
    project: "{from request}"
  contacts:
    technical: "{engineer}"
    business: "{stakeholder}"
```

### 6. Validate the Contract

Run validation checks before presenting for approval:

| Check | Pass Condition |
|---|---|
| All required fields populated | No empty required fields |
| At least one quality rule per non-nullable column | not_null rule exists |
| Freshness SLA > 0 | Positive number |
| Volume bounds are reasonable | min < max, both > 0 |
| Evolution policy matches sensitivity | strict for PHI, etc. |
| Partition column exists in schema | `_ingested_date` present |
| No duplicate rule names | All rule names unique |
| Schema version follows semver | `X.Y.Z` format |

If validation fails, present errors and ask user to correct before proceeding.

### 7. Route for Approval

Based on sensitivity and complexity:

| Condition | Approval Path |
|---|---|
| All columns public/internal, single table | Auto-approved immediately |
| Any column restricted | Data Steward must approve |
| Any column PHI | Data Steward + Privacy Officer must approve |
| External third-party source | Data Steward + Legal + Security |

**On approval:**
- Update contract status from `draft` to `active`
- Store in DynamoDB `platform-data-contracts` table
- Update Source Registry status to `contracted`
- Emit notification to requester

**On rejection:**
- Update Source Registry status to `rejected`
- Store rejection reason in status_metadata
- Notify requester with feedback

### 8. Version Management

For existing contracts being updated:

```
Current version: 1.2.0
Change type: additive (new nullable column)
New version: 1.3.0
```

Version rules:
- MAJOR: breaking schema changes, quality rule removals, SLA relaxation
- MINOR: new columns, new quality rules, SLA tightening
- PATCH: description updates, tag changes, consumer additions

Store version history in DynamoDB with full diff for each version bump.

## Contract Lifecycle States

```
draft → active → deprecated → retired
                ↗ (can reactivate)
         suspended (temporary pause)
```

| State | Meaning | Pipeline Behavior |
|---|---|---|
| draft | Under review, not yet approved | No pipeline deployed |
| active | Approved and enforced | Pipeline runs, quality enforced |
| suspended | Temporarily paused | Pipeline paused, data preserved |
| deprecated | Scheduled for retirement | Pipeline runs with warnings |
| retired | No longer valid | Pipeline stopped, table archived |

## MUST Rules

- MUST generate at least one quality rule per non-nullable column
- MUST include freshness SLA for every contract
- MUST set evolution policy appropriate to sensitivity level
- MUST validate contract structure before presenting for approval
- MUST version every contract change immutably
- MUST NOT approve contracts with PHI columns without privacy officer sign-off
- MUST store full contract history for audit purposes
- MUST notify all registered consumers when a contract version changes

## Gotchas

- Contracts with zero quality rules are invalid — always generate at minimum not_null checks
- Freshness SLA must account for schedule frequency (don't set 1h SLA on a daily pipeline)
- Volume bounds that are too tight cause false-positive alerts — use 50% anomaly threshold as default
- Consumer list starts empty — it's populated as downstream teams register interest
- Contract name must match source_name exactly for pipeline config to reference it correctly
