# Hook — Post-Ingestion Checks

> Runs AFTER data lands in S3. Verifies that data actually arrived and
> optionally triggers the profiling job.

## When This Hook Runs

- After DMS task completes a batch / AppFlow flow succeeds / Firehose delivers
- Triggered by EventBridge on S3 ObjectCreated event in landing prefix

## Checks Performed

### 1. Data Arrived

- At least one new file exists in the expected Landing S3 prefix
- File size > 0 bytes (not empty)

### 2. Freshness

- Latest file timestamp is within expected SLA window
- If stale → fire CloudWatch alarm

### 3. Trigger Profiling (if configured)

- Start DataBrew profile job on the new data
- Profile job is independent — its success/failure does not block the pipeline

## On Failure

- No data arrived → alert (pipeline may have silently failed)
- Data arrived but stale → warn (check schedule alignment)
- Profiling job failed → log warning (non-blocking, informational only)
