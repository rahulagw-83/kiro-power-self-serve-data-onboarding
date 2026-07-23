# Hook — Pre-Ingestion Validation

> Runs BEFORE the ingestion service starts moving data. Validates that
> preconditions are met so the pipeline doesn't fail mid-run.

## When This Hook Runs

- Before DMS task starts / AppFlow flow triggers / Firehose resumes
- Before any Terraform apply that provisions ingestion resources

## Checks Performed

### 1. Source Connectivity

Verify the source is reachable:
- Database: DMS test-connection or Glue test-connection
- SaaS: AppFlow connector profile validation
- S3: `aws s3 ls` on source prefix
- Streaming: describe stream/topic

### 2. Credentials Valid

- Secrets Manager secret exists and is readable
- Secret keys match expected format
- Secret host matches declared endpoint
- Warn if last rotated > 90 days

### 3. Landing Bucket Accessible

- `aws s3api head-bucket --bucket {landing-bucket}` succeeds
- Service role has `s3:PutObject` permission on landing prefix

### 4. Networking (if VPC required)

- Security group has self-referencing ingress rule
- Subnet has route to source
- S3 VPC endpoint exists (if private subnet)

## On Failure

Stop immediately. Present the specific failing check. Do not proceed to deployment.
