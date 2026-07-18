# Onboarding Workflow: New Data Source (End-to-End)

> **Master Workflow** — This is the canonical workflow Kiro follows when onboarding any
> data source into the governed data lake. It unifies self-serve and engineer-led paths,
> covering S3 file drops, JDBC/RDS databases, and SaaS platform connectors. Every step
> references the responsible tool or sub-workflow so execution is deterministic.

---

## Preconditions

Before starting any onboarding:

| # | Requirement | How to verify |
|---|-------------|---------------|
| 1 | AWS account bootstrapped with CDK | `cdk bootstrap` completed in target account/region |
| 2 | S3 Table Bucket exists for bronze layer | `aws s3tables list-table-buckets` returns at least one bucket |
| 3 | DynamoDB Source Registry table exists | `aws dynamodb describe-table --table-name SourceRegistry` |
| 4 | Glue Data Catalog database for raw zone | `aws glue get-database --name raw_zone` |
| 5 | IAM role for Glue jobs with baseline policy | Role ARN stored in SSM `/data-platform/glue-role-arn` |
| 6 | VPC with NAT + Glue connection (for JDBC) | Connection name stored in SSM `/data-platform/glue-connection` |
| 7 | Secrets Manager secret template (for JDBC) | Secret ARN pattern `data-platform/{source_name}/credentials` |

---

## Estimated Timeline

| Complexity | Source Type | Estimated Duration | Examples |
|------------|-------------|-------------------|----------|
| Low | S3 file (CSV/JSON/Parquet) | < 1 hour | Daily CSV export from vendor |
| Medium | JDBC database (single table) | 2–4 hours | PostgreSQL orders table |
| High | JDBC multi-table / SaaS API | 4–8 hours | Full Salesforce sync, Oracle ERP |

---

## Phase 1: Request & Register

### Step 1 — Capture Requirements

Collect source metadata using the `onboarding-request.yaml` template. The template
adapts based on source type (S3, JDBC, or SaaS). See the full template at the end
of this document.

**Actions:**
1. Present the onboarding-request template to the requester.
2. Validate required fields are populated.
3. Infer complexity (low/medium/high) from source type + table count.
4. Store the completed request as `onboarding-request.yaml` in the pipeline workspace.

### Step 2 — Duplicate Check

Query the DynamoDB Source Registry to prevent re-onboarding an existing source.

```python
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("SourceRegistry")

def check_duplicate(source_name: str, source_type: str) -> bool:
    """Return True if source already registered (any status)."""
    response = table.query(
        KeyConditionExpression=Key("source_name").eq(source_name),
        FilterExpression=Attr("source_type").eq(source_type),
    )
    return response["Count"] > 0
```

**Decision:**
- If duplicate found with `status=active` → abort, inform requester.
- If duplicate found with `status=failed` → offer to retry from last successful phase.
- If no duplicate → proceed to Step 3.

### Step 3 — Register in Source Registry

Create the registry entry with `status=pending` to claim the namespace.

```python
import uuid
from datetime import datetime, timezone

def register_source(request: dict) -> str:
    """Register a new source and return its source_id."""
    source_id = str(uuid.uuid4())
    table.put_item(
        Item={
            "source_id": source_id,
            "source_name": request["source_name"],
            "source_type": request["source_type"],
            "owner": request["owner"],
            "domain": request["domain"],
            "status": "pending",
            "complexity": request.get("complexity", "medium"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "request_metadata": request,
        },
        ConditionExpression="attribute_not_exists(source_name)",
    )
    return source_id
```

---

## Phase 2: Connection & Schema

### Step 4 — Set Up Connection

Connection setup varies by source type:

| Source Type | Action |
|-------------|--------|
| S3 | No connection needed — validate bucket exists and IAM role has read access |
| JDBC | Follow `connection-setup.md` steering — create Glue Connection + Secrets Manager |
| SaaS | Configure OAuth2/API-key credentials in Secrets Manager, test token refresh |

**For JDBC sources:**
1. Create or reuse a Secrets Manager secret with connection credentials.
2. Create or reuse a Glue JDBC Connection in the appropriate VPC/subnet.
3. Validate security group allows egress to the database port.

**For S3 sources:**
1. Verify the source bucket exists: `aws s3api head-bucket --bucket {bucket}`.
2. Verify the Glue role can list/read: test with `aws s3 ls s3://{bucket}/{prefix}/`.
3. Confirm event notification (if incremental): SNS/SQS trigger is in place.

### Step 5 — Test Connectivity

Run a 2-phase connectivity test:

**Phase A — API-level test:**
```bash
# For JDBC
aws glue test-connection --connection-name ${CONNECTION_NAME}

# For S3
aws s3api head-object --bucket ${BUCKET} --key ${PREFIX}/sample_file.csv
```

**Phase B — Engine-level test (Glue job dry run):**
- Submit a minimal Glue job that reads 1 row from the source.
- Timeout: 120 seconds.
- If both phases pass → proceed.
- If Phase A fails → fix network/credentials and retry (max 3 attempts).
- If Phase B fails → escalate to platform team.

### Step 6 — Infer Schema

Sample the source to detect schema automatically:

1. Read a sample of 1,000+ rows (or full file for S3 sources < 10MB).
2. For each column, detect:
   - Data type (string, integer, decimal, timestamp, boolean, binary)
   - Nullable (>0 nulls in sample → nullable)
   - Cardinality (unique count / total count)
   - Sample values (5 representative, non-null values)
3. Classify sensitivity per column:
   - `public` — no restrictions
   - `internal` — business-sensitive but not regulated
   - `restricted` — PII (name, email, phone, address, SSN patterns)
   - `phi` — protected health information

**Sensitivity detection rules:**
- Column name matches `/email|e_mail/i` → restricted
- Column name matches `/ssn|social_security/i` → restricted
- Column name matches `/phone|mobile|cell/i` → restricted
- Column name matches `/diagnosis|medication|icd/i` → phi
- Column values match email regex → restricted
- Column values match SSN pattern `\d{3}-\d{2}-\d{4}` → restricted

### Step 7 — Review Classification

Present the inferred schema + sensitivity classification to the requester.

**Output format:**
```
| Column | Type | Nullable | Sensitivity | Action Required |
|--------|------|----------|-------------|-----------------|
| order_id | string | no | public | — |
| customer_email | string | yes | restricted | Mask in non-prod |
| total_amount | decimal(10,2) | no | internal | — |
```

**Decision gates:**
- If any column is `restricted` → requester must confirm masking strategy.
- If any column is `phi` → privacy officer must approve before proceeding.
- If all columns are `public` or `internal` → auto-approve, proceed.

---

## Phase 3: Data Contract

### Step 8 — Generate Draft Contract

Auto-generate a data contract from the inferred schema and request metadata.

**Contract includes:**
- Source identity (name, type, owner, domain)
- Schema definition (columns, types, nullability, sensitivity)
- Quality rules (not-null constraints, uniqueness, value ranges)
- SLA (freshness target, max acceptable delay)
- Partitioning strategy (date-based by default)
- Retention policy (default: 7 years raw, 2 years curated)
- Lineage metadata (source system, ingestion method, transform version)

**Generated artifact:** `data-contract.yaml` in the pipeline workspace.

**Quality rules auto-generation:**
- Primary key columns → uniqueness check
- Non-nullable columns → not-null check
- Timestamp columns → freshness check (value within SLA window)
- Numeric columns with known range → range check
- String columns with low cardinality → allowed-values check

### Step 9 — Review & Approve Contract

Approval follows the complexity-based matrix:

| Complexity | Approver(s) | SLA |
|------------|-------------|-----|
| Low (public data, single table) | Auto-approved | Immediate |
| Medium (internal data, multi-table) | Data Steward | 1 business day |
| High (restricted/PHI data) | Data Steward + Privacy Officer | 3 business days |

**Actions:**
1. If auto-approved → proceed immediately to Phase 4.
2. If manual approval required → send notification (email/Slack) with contract link.
3. On approval → update registry status to `contracted`.
4. On rejection → update registry status to `rejected`, notify requester with feedback.

```python
def update_source_status(source_id: str, new_status: str, metadata: dict = None):
    """Update the source registry status after contract review."""
    update_expr = "SET #s = :status, updated_at = :ts"
    expr_values = {
        ":status": new_status,
        ":ts": datetime.now(timezone.utc).isoformat(),
    }
    expr_names = {"#s": "status"}

    if metadata:
        update_expr += ", status_metadata = :meta"
        expr_values[":meta"] = metadata

    table.update_item(
        Key={"source_id": source_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )
```

---

## Phase 4: Pipeline Generation & Testing

### Step 10 — Generate Pipeline Config

Produce `pipeline-config.yaml` by merging the Source Registry entry with the
approved data contract.

**Config includes:**
- Source connection details (reference to Secrets Manager / S3 path)
- Target table definition (Iceberg table in S3 Tables or general-purpose bucket)
- Transform rules (column renames, type casts, masking for restricted fields)
- Scheduling (cron expression or event-driven trigger)
- Resource sizing (Glue worker type, number of workers, timeout)
- Quality gate thresholds (max null %, max duplicate %, freshness SLA)

**Sizing heuristic:**
| Data Volume | Worker Type | Workers | Timeout |
|-------------|-------------|---------|---------|
| < 1 GB | G.025X | 2 | 10 min |
| 1–10 GB | G.1X | 5 | 30 min |
| 10–100 GB | G.2X | 10 | 60 min |
| > 100 GB | G.2X | 20 | 120 min |

### Step 11 — Generate Pipeline Code

Generate the full pipeline implementation:

1. **Glue Job Script** (`glue_job/ingest_{source_name}.py`)
   - PySpark or Python Shell depending on volume
   - Reads from source using DynamicFrame or JDBC
   - Applies transforms (type casting, column mapping, masking)
   - Adds audit columns: `_ingested_at`, `_source_file`, `_batch_id`
   - Writes to Iceberg table with APPEND or MERGE
   - Emits CloudWatch metrics on success/failure

2. **CDK Stack** (`infra/stack.py`)
   - Glue Job definition with bookmarks enabled
   - IAM role with least-privilege policy
   - S3 bucket/prefix for staging (if needed)
   - EventBridge rule or Glue Trigger for scheduling
   - CloudWatch alarm for job failures

3. **Step Functions Workflow** (`infra/workflow.asl.json`)
   - Pre-flight check (source availability)
   - Glue job execution with retry (3 attempts, exponential backoff)
   - Post-flight quality validation
   - Notification on success/failure
   - Error handling with DLQ routing

4. **Tests** (`tests/`)
   - Unit tests for transform logic
   - Integration test for end-to-end (uses localstack or mocked clients)
   - Data quality assertion tests

### Step 12 — Run Smoke Test in Dev

Execute a limited-scope test to validate the pipeline works end-to-end:

1. Set batch size to 100 rows (or 1 file for S3).
2. Deploy pipeline to dev environment.
3. Trigger execution.
4. Validate:
   - Job completes without error
   - Output table has expected row count
   - Schema matches contract (column names, types)
   - Audit columns are populated correctly
   - No data quality rule violations
5. If smoke test fails → diagnose, fix, re-run (max 3 attempts).

### Step 13 — Deploy to Dev

Full deployment to dev environment:

```bash
# Deploy CDK stack to dev
cdk deploy DataPipeline-${SOURCE_NAME}-Dev \
  --context env=dev \
  --context source_name=${SOURCE_NAME} \
  --require-approval never

# Verify deployment
aws glue get-job --job-name ingest-${SOURCE_NAME}-dev
aws stepfunctions describe-state-machine \
  --state-machine-arn arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:ingest-${SOURCE_NAME}-dev
```

---

## Phase 5: Promotion & Activation

### Step 14 — Run Integration Tests

Execute full-volume integration tests in the dev environment:

1. Run pipeline with production-equivalent batch size.
2. Validate data quality rules pass at scale:
   - Null percentage within threshold
   - Duplicate percentage within threshold
   - Value ranges respected
   - Freshness SLA met
3. Run Athena query to verify data is queryable.
4. Verify partition structure is correct.
5. Check CloudWatch metrics are being emitted.

**Pass criteria:** All quality gates pass AND job completes within timeout.

### Step 15 — Promote to Prod

Two-gate promotion process:

**Gate 1: Dev → Test**
- [ ] Integration tests pass in dev
- [ ] Data contract approved
- [ ] No restricted data without masking confirmed
- [ ] Resource sizing validated (cost within budget)

```bash
cdk deploy DataPipeline-${SOURCE_NAME}-Test \
  --context env=test \
  --context source_name=${SOURCE_NAME} \
  --require-approval broadening
```

**Gate 2: Test → Prod**
- [ ] Integration tests pass in test environment
- [ ] Performance benchmark meets SLA (< 2x expected duration)
- [ ] Rollback procedure documented and tested
- [ ] On-call team notified of new pipeline
- [ ] Monitoring dashboard created
- [ ] Runbook published

```bash
cdk deploy DataPipeline-${SOURCE_NAME}-Prod \
  --context env=prod \
  --context source_name=${SOURCE_NAME} \
  --require-approval broadening
```

### Step 16 — Activate Observability

Set up production monitoring:

1. **CloudWatch Metrics:**
   - `glue.job.duration` — job execution time
   - `glue.job.rows_processed` — throughput
   - `glue.job.errors` — error count
   - `pipeline.quality.score` — quality gate pass rate
   - `pipeline.freshness.lag` — time since last successful run

2. **CloudWatch Alarms:**
   - Job failure: 1 consecutive failure → P2 alert
   - Job failure: 3 consecutive failures → P1 alert
   - Duration exceeded: > 2x normal → P3 alert
   - Freshness SLA breach → P2 alert
   - Quality score < threshold → P2 alert

3. **Dashboard:**
   - Pipeline health overview (success/failure trend)
   - Data freshness timeline
   - Volume trend (rows/bytes per run)
   - Cost per run (DPU-hours)

### Step 17 — Update Registry & Notify

Finalize the onboarding:

```python
def activate_source(source_id: str, pipeline_arn: str, table_name: str):
    """Mark source as active and record pipeline details."""
    table.update_item(
        Key={"source_id": source_id},
        UpdateExpression=(
            "SET #s = :status, updated_at = :ts, "
            "pipeline_arn = :arn, target_table = :tbl, "
            "activated_at = :ts"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": "active",
            ":ts": datetime.now(timezone.utc).isoformat(),
            ":arn": pipeline_arn,
            ":tbl": table_name,
        },
    )
```

**Notifications:**
- Send completion notification to requester (email/Slack).
- Post to `#data-platform` channel with source summary.
- Update data catalog with lineage metadata.

---

## Self-Serve Portal Flow

For business users who want to onboard a data source without deep technical knowledge,
the self-serve path provides a 4-step simplified experience:

### Step A — Fill Request Form
- Web form or YAML template with guided prompts.
- Required fields only: source name, type, location, owner, domain, schedule.
- Optional fields auto-defaulted based on source type.

### Step B — Automated Validation
- Kiro validates connectivity, infers schema, classifies sensitivity.
- If any issue found → return clear error with remediation steps.
- If validation passes → present schema summary for confirmation.

### Step C — One-Click Approval
- For low-complexity sources: instant auto-approval.
- For medium/high: routes to appropriate approver with pre-filled context.
- Requester sees status tracker (pending → approved → deploying → active).

### Step D — Pipeline Activation
- Pipeline deployed automatically after approval.
- Requester receives confirmation with:
  - Target table name for Athena queries
  - Schedule summary
  - Dashboard link
  - Support contact for issues

**Self-serve eligibility criteria:**
- Source type is S3 (CSV, JSON, Parquet) or pre-approved JDBC template
- Single table / single prefix
- Data sensitivity is public or internal
- Estimated volume < 10 GB per batch

Sources outside these criteria are routed to the engineer-led workflow (full Phase 1–5).

---

## Definition of Done

An onboarding is considered complete when ALL of the following are true:

- [ ] Source registered in DynamoDB Source Registry with `status=active`
- [ ] Data contract YAML committed to version control and approved
- [ ] Pipeline code (Glue job + CDK stack + Step Functions) committed and reviewed
- [ ] Pipeline deployed to production and executing on schedule
- [ ] First successful production run completed with data quality checks passing
- [ ] Target Iceberg table accessible via Athena with correct schema
- [ ] CloudWatch alarms configured and tested (triggered + resolved)
- [ ] Dashboard created showing pipeline health metrics
- [ ] Lineage metadata registered in Glue Data Catalog
- [ ] Requester notified with access instructions
- [ ] Runbook published for on-call team
- [ ] Cost estimate validated against actual first-run cost

---

## Onboarding Request Template

The following YAML template is used to capture all onboarding requirements. Fields are
marked as required (`# REQUIRED`) or optional with defaults.

```yaml
# onboarding-request.yaml
# ─────────────────────────────────────────────────────────────────────────────
# Unified Data Source Onboarding Request
# Fill in the sections relevant to your source type.
# ─────────────────────────────────────────────────────────────────────────────

# ── Identity ──────────────────────────────────────────────────────────────────
source_name: ""              # REQUIRED — unique identifier (snake_case, e.g., shopify_orders)
display_name: ""             # REQUIRED — human-readable name
description: ""              # REQUIRED — what this data represents
domain: ""                   # REQUIRED — business domain (sales, finance, marketing, ops)
owner: ""                    # REQUIRED — team or person responsible
owner_email: ""              # REQUIRED — notification target
classification: "internal"   # REQUIRED — public | internal | restricted | phi

# ── Source Type ───────────────────────────────────────────────────────────────
source_type: ""              # REQUIRED — s3 | jdbc | saas

# ── S3 Source Configuration ───────────────────────────────────────────────────
# (Fill this section only if source_type = s3)
s3:
  bucket: ""                 # REQUIRED for S3 — source bucket name
  prefix: ""                 # REQUIRED for S3 — key prefix (e.g., exports/orders/)
  file_format: "csv"         # csv | json | jsonl | parquet | orc | avro
  compression: "none"        # none | gzip | snappy | zstd | bzip2
  delimiter: ","             # field delimiter (CSV only)
  has_header: true           # whether first row is header (CSV only)
  quote_char: '"'            # quote character (CSV only)
  escape_char: "\\"          # escape character (CSV only)
  multiline: false           # multiline JSON records (JSON only)
  partition_pattern: ""      # e.g., year={yyyy}/month={MM}/day={dd}
  file_pattern: "*.csv"      # glob pattern to match source files
  trigger: "schedule"        # schedule | event (S3 notification)
  event_queue_arn: ""        # SQS ARN if trigger=event

# ── JDBC Source Configuration ─────────────────────────────────────────────────
# (Fill this section only if source_type = jdbc)
jdbc:
  engine: ""                 # REQUIRED for JDBC — postgresql | mysql | oracle | sqlserver | redshift
  host: ""                   # REQUIRED for JDBC — database hostname or endpoint
  port: 5432                 # database port
  database: ""               # REQUIRED for JDBC — database name
  schema: "public"           # schema name
  table: ""                  # REQUIRED for JDBC — table name (or comma-separated list)
  connection_name: ""        # existing Glue connection name (leave empty to create new)
  secret_arn: ""             # existing Secrets Manager ARN (leave empty to create new)
  fetch_size: 10000          # JDBC fetch size for pagination
  partition_column: ""       # column for parallel reads (numeric or date recommended)
  partition_count: 4         # number of parallel partitions
  incremental_column: ""     # column for incremental loads (e.g., updated_at)
  watermark_value: ""        # last processed value (managed automatically after first run)
  custom_query: ""           # optional SQL query override (replaces table read)
  ssl_mode: "require"        # disable | allow | prefer | require | verify-ca | verify-full

# ── SaaS Source Configuration ─────────────────────────────────────────────────
# (Fill this section only if source_type = saas)
saas:
  platform: ""               # REQUIRED for SaaS — salesforce | hubspot | stripe | shopify | zendesk
  auth_type: "oauth2"        # oauth2 | api_key | basic
  secret_arn: ""             # Secrets Manager ARN for credentials
  api_version: ""            # API version (platform-specific)
  objects: []                # list of objects/entities to sync
  sync_mode: "incremental"   # full | incremental
  incremental_field: ""      # field for incremental sync (e.g., SystemModstamp)
  rate_limit_rps: 10         # requests per second limit
  page_size: 200             # records per API page
  custom_fields: true        # include custom fields in schema
  start_date: ""             # historical sync start date (ISO 8601)

# ── Target Configuration ──────────────────────────────────────────────────────
target:
  format: "iceberg"          # iceberg (default) | parquet | delta
  table_bucket: ""           # S3 Tables bucket name (auto-detected if empty)
  namespace: ""              # S3 Tables namespace (defaults to domain)
  table_name: ""             # target table name (defaults to source_name)
  partition_by: ["ingestion_date"]  # partition columns
  write_mode: "append"       # append | merge | overwrite
  merge_keys: []             # primary key columns for merge mode
  compact_files: true        # enable auto-compaction
  snapshot_retention_days: 7 # Iceberg snapshot retention

# ── Schedule ──────────────────────────────────────────────────────────────────
schedule:
  frequency: "daily"         # hourly | daily | weekly | monthly | event_driven
  cron: "0 6 * * ? *"       # cron expression (UTC) — overrides frequency if set
  timezone: "UTC"            # timezone for schedule display
  start_date: ""             # when to start scheduling (ISO 8601, default: immediately)
  backfill: false            # run historical backfill on first execution
  backfill_start: ""         # backfill start date (ISO 8601)

# ── Quality Rules ─────────────────────────────────────────────────────────────
quality:
  freshness_sla_hours: 24    # max acceptable data age in hours
  max_null_percent: 5        # max allowed null percentage per column
  max_duplicate_percent: 0   # max allowed duplicate percentage (0 = no duplicates)
  row_count_anomaly_threshold: 50  # alert if row count changes > X% from baseline
  custom_rules: []           # list of custom SQL-based quality rules
    # - name: "positive_amounts"
    #   sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
    #   threshold: 0

# ── Metadata ──────────────────────────────────────────────────────────────────
metadata:
  tags:
    environment: "prod"
    cost_center: ""
    project: ""
  contacts:
    technical: ""            # engineer responsible for pipeline health
    business: ""             # business stakeholder
  documentation_url: ""      # link to source system documentation
  notes: ""                  # free-text notes
```

---

## Workflow Decision Tree

```
START
  │
  ▼
[Step 1] Capture Requirements
  │
  ▼
[Step 2] Duplicate Check ──── exists + active ──→ ABORT (notify requester)
  │                    │
  │                    └──── exists + failed ──→ RETRY from last phase
  │
  ▼ (no duplicate)
[Step 3] Register (status=pending)
  │
  ├── source_type = s3 ────→ [Step 4a] Validate bucket + IAM
  ├── source_type = jdbc ──→ [Step 4b] Glue Connection + Secrets
  └── source_type = saas ──→ [Step 4c] OAuth/API-key + Secrets
  │
  ▼
[Step 5] Test Connectivity (2-phase)
  │       │
  │       └── FAIL (3 retries) ──→ ESCALATE to platform team
  │
  ▼ (pass)
[Step 6] Infer Schema (sample ≥ 1000 rows)
  │
  ▼
[Step 7] Review Classification
  │       │
  │       ├── restricted ──→ requester confirms masking
  │       └── phi ─────────→ privacy officer approval
  │
  ▼ (approved)
[Step 8] Generate Data Contract
  │
  ▼
[Step 9] Approve Contract
  │       │
  │       ├── low complexity ──→ auto-approve
  │       ├── medium ──────────→ data steward review
  │       └── high ────────────→ steward + privacy review
  │       │
  │       └── REJECTED ──→ UPDATE registry (status=rejected), NOTIFY, END
  │
  ▼ (approved, status=contracted)
[Step 10] Generate Pipeline Config
  │
  ▼
[Step 11] Generate Pipeline Code
  │
  ▼
[Step 12] Smoke Test (100 rows)
  │       │
  │       └── FAIL (3 retries) ──→ diagnose + fix + retry
  │
  ▼ (pass)
[Step 13] Deploy to Dev
  │
  ▼
[Step 14] Integration Tests (full volume)
  │       │
  │       └── FAIL ──→ diagnose + fix + re-run smoke
  │
  ▼ (pass)
[Step 15] Promote (dev→test→prod)
  │
  ▼
[Step 16] Activate Observability
  │
  ▼
[Step 17] Update Registry (status=active) + Notify
  │
  ▼
  DONE
```

---

## Error Recovery

| Phase | Failure Mode | Recovery Action |
|-------|-------------|-----------------|
| 1 | Duplicate detected | Inform requester, offer retry if previous failed |
| 2 | Connection timeout | Check security groups, VPC routing, credentials |
| 2 | Authentication failure | Rotate credentials in Secrets Manager, retry |
| 2 | Schema inference empty | Verify source has data, check permissions |
| 3 | Contract rejected | Address feedback, re-submit for review |
| 4 | Smoke test failure | Check logs, fix transform logic, retry |
| 4 | Deploy failure | Check CDK diff, resolve conflicts, redeploy |
| 5 | Integration test failure | Scale resources, fix quality rules, retry |
| 5 | Promotion blocked | Complete checklist items, get approvals |

---

## Appendix: DynamoDB Source Registry Schema

```
Table: SourceRegistry
  Partition Key: source_id (String)
  GSI-1: source_name-index (source_name → source_id)
  GSI-2: status-index (status → source_id, for dashboard queries)
  GSI-3: domain-index (domain → source_id, for domain filtering)

Attributes:
  source_id        String   — UUID, primary key
  source_name      String   — unique human identifier
  source_type      String   — s3 | jdbc | saas
  owner            String   — responsible team/person
  domain           String   — business domain
  status           String   — pending | contracted | deploying | active | failed | rejected | decommissioned
  complexity       String   — low | medium | high
  created_at       String   — ISO 8601 timestamp
  updated_at       String   — ISO 8601 timestamp
  activated_at     String   — ISO 8601 timestamp (set when status=active)
  pipeline_arn     String   — Step Functions state machine ARN
  target_table     String   — fully qualified Iceberg table name
  request_metadata Map      — full onboarding-request.yaml content
  contract_version Number   — data contract version counter
  status_metadata  Map      — additional context for current status
```
