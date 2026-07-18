---
inclusion: always
---

# Data Contract Governance

> Always-on standard. Every data source entering the platform MUST have a data
> contract. The contract is the binding agreement between producer and consumer
> that enables self-service without chaos.

## 1. What is a Data Contract?

A data contract is a **versioned, machine-readable specification** that defines:
- The schema (fields, types, nullability, constraints)
- Quality expectations (freshness SLA, volume bounds, DQ rules)
- Ownership and accountability
- Classification and access policy
- Evolution rules (what changes are backward-compatible)

## 2. Contract Lifecycle

```
Proposed → Under Review → Active → Evolving → Deprecated → Retired
```

| State | Meaning |
|-------|---------|
| **Proposed** | Drafted (by requester or Kiro); awaiting review |
| **Under Review** | Data steward / owner validating the contract |
| **Active** | Enforced — pipeline validates against this contract every run |
| **Evolving** | A new minor version is being prepared (additive only) |
| **Deprecated** | Consumers notified; migration window open |
| **Retired** | No longer enforced; pipeline decommissioned |

## 3. Versioning Rules

Semantic versioning: `MAJOR.MINOR.PATCH`

| Change Type | Version Bump | Backward Compatible? | Requires |
|-------------|-------------|---------------------|----------|
| Add nullable column | MINOR | Yes | Automated |
| Rename column | MAJOR | No | Consumer notification + migration |
| Remove column | MAJOR | No | Deprecation period + approval |
| Change type (widening) | MINOR | Yes | Automated |
| Change type (narrowing) | MAJOR | No | Approval + consumer validation |
| Add DQ rule | PATCH | Yes | Automated |
| Relax DQ rule | MINOR | Yes | Automated |
| Tighten DQ rule | MAJOR | No | Consumer impact analysis |
| New PII column added | MAJOR | No | Alert + pause + re-approval |

## 4. Enforcement Points

- **At ingestion time**: Pipeline validates incoming data against the active contract.
  - Schema match → pass
  - Extra columns → quarantine or evolve (per policy)
  - Missing required columns → fail batch + alert
  - DQ violations → quarantine rows + alert if threshold exceeded
- **At consumption time**: Consumers see only the contracted interface (Lake Formation permissions).
- **At promotion time**: dev → test → prod promotion validates contract compatibility.

## 5. Contract Generation (from Schema Inference)

When a new source is inferred, the Contract Engine auto-generates a **draft contract**:
1. Maps inferred types to contract field definitions.
2. Applies default constraints based on classification (e.g., PII fields get regex patterns).
3. Sets default freshness SLA based on source type (real-time: 1h, CDC: 2h, batch: 4-24h).
4. Sets volume bounds at +/- 3 sigma of sampled data volume.
5. Marks as `status: proposed` — requires human review before activation.

Default constraint rules by type:

| Field Type | Default Constraints |
|-----------|-------------------|
| STRING (email) | regex `^[^@]+@[^@]+\.[^@]+$` |
| STRING (phone) | regex `^\+?[\d\s\-()]{7,15}$` |
| STRING (id) | non-null + unique |
| NUMERIC | min >= 0 (unless context indicates negative) |
| TIMESTAMP | range: 1970-01-01 to now + 1 year |
| BOOLEAN | only true/false/null |

## 6. Contract Storage

- Contracts stored as versioned YAML in the `config/` directory of the pipeline project.
- Also registered in DynamoDB table `platform-data-contracts` with metadata.
- Queryable via DynamoDB API or Athena (if contracts synced to Iceberg).
- Referenced in Glue Data Catalog table parameters.

## 7. Contract Evolution Workflow

### Additive (MINOR)
New nullable field, type widened, DQ rule relaxed. Auto-applied; consumers notified via SNS.

### Breaking (MAJOR)
Field removed/renamed/narrowed, DQ tightened. Requires:
1. Consumer impact analysis
2. Notification to all listed consumers
3. Deprecation window (default 30 days)
4. New version active only after approval

### Retire
- Trigger: Source decommissioned or all consumers migrated.
- Set status = `deprecated`; notify consumers; after 30 days set `retired`.
- Retain metadata for audit (never delete).

## 8. MUST Rules

1. **MUST** have a contract BEFORE deploying a pipeline to production.
2. **MUST** validate every batch against the active contract version.
3. **MUST** quarantine (not drop) rows failing validation.
4. **MUST** emit contract-validation metrics to CloudWatch.
5. **MUST** block promotion to prod if contract is in `proposed` state.
6. **MUST** notify all downstream consumers of breaking (MAJOR) changes via SNS.
7. **MUST** maintain at least one prior major version during deprecation window (default 30 days).
8. **MUST** include freshness SLA — contract specifies max acceptable delay.
9. **MUST** include volume bounds — unexpected spikes/drops trigger CloudWatch Alarms.
10. **MUST** store validation results for audit compliance.
