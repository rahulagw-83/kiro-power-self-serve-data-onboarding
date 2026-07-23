# Connection Setup

> **Skill Available:** For interactive connection setup with live discovery, credential registration,
> and two-phase testing, use the `connecting-to-data-source` skill at
> `skills/connecting-to-data-source/SKILL.md`. The skill automates Steps 1-5 below by discovering
> existing connections and RDS/Redshift candidates in the account, interactively registering
> credentials (with IAM DB auth preferred), and running structured two-phase connectivity tests.
>
> **When to use the skill vs. this steering file:**
> - Use the **skill** when onboarding a new source interactively (Steps 4-5 of the master workflow)
> - Use this **steering file** as a reference for connection naming conventions, VPC requirements,
>   environment migration, and integration with pipeline config

## When This File Is Used

Use this guide when:
- Setting up a new data source that requires a Glue connection
- Troubleshooting connectivity failures in existing pipelines
- Migrating connections between environments (dev → test → prod)
- Verifying connection health after VPC or security group changes

---

## Source Classification

| Source | Connection Type | Requires Glue Connection? | Driver |
|---|---|---|---|
| Oracle | JDBC | Yes | ojdbc8 |
| SQL Server | JDBC | Yes | mssql-jdbc |
| PostgreSQL | JDBC | Yes | postgresql |
| MySQL / Aurora MySQL | JDBC | Yes | mysql-connector-java |
| Amazon Redshift | JDBC | Yes | redshift-jdbc42 |
| Snowflake | SNOWFLAKE | Yes (native type) | Built-in |
| BigQuery | BIGQUERY | Yes (native type) | Built-in |
| Amazon S3 | — | No | N/A |
| Apache Kafka / MSK | KAFKA | Yes | Built-in |
| Amazon DynamoDB | — | No | N/A |
| Amazon Kinesis | — | No | N/A |

---

## Connection Setup Procedure

### Step 1: Check Existing Connections

```bash
# List all Glue connections in the account
aws glue get-connections --query "ConnectionList[].{Name:Name,Type:ConnectionType,Status:LastUpdatedTime}" --output table

# Check specific connection
aws glue get-connection --name "prod-oracle-hr" --query "Connection.{Name:Name,Type:ConnectionType,Properties:ConnectionProperties}"
```

### Step 2: Discover Candidates (RDS/Aurora/Redshift)

```bash
# Find RDS instances
aws rds describe-db-instances \
  --query "DBInstances[].{ID:DBInstanceIdentifier,Engine:Engine,Endpoint:Endpoint.Address,Port:Endpoint.Port,VPC:DBSubnetGroup.VpcId}" \
  --output table

# Find Aurora clusters
aws rds describe-db-clusters \
  --query "DBClusters[].{ID:DBClusterIdentifier,Engine:Engine,Endpoint:Endpoint,ReaderEndpoint:ReaderEndpoint,Port:Port}" \
  --output table

# Find Redshift clusters
aws redshift describe-clusters \
  --query "Clusters[].{ID:ClusterIdentifier,Endpoint:Endpoint.Address,Port:Endpoint.Port,VPC:VpcId,Database:DBName}" \
  --output table
```

### Step 3: Register Credentials in Secrets Manager

```bash
# Create secret for database credentials
aws secretsmanager create-secret \
  --name "data-lake/prod/oracle-hr" \
  --description "Oracle HR credentials for Glue ingestion" \
  --secret-string '{
    "username": "glue_reader",
    "password": "CHANGE_ME",
    "engine": "oracle",
    "host": "oracle-hr.abc123.us-east-1.rds.amazonaws.com",
    "port": 1521,
    "dbname": "HRDB"
  }'
```

### Step 4: Create Glue Connection

```bash
# JDBC Connection (Oracle example)
aws glue create-connection --connection-input '{
  "Name": "prod-oracle-hr",
  "ConnectionType": "JDBC",
  "ConnectionProperties": {
    "JDBC_CONNECTION_URL": "jdbc:oracle:thin:@//oracle-hr.abc123.us-east-1.rds.amazonaws.com:1521/HRDB",
    "SECRET_ID": "data-lake/prod/oracle-hr"
  },
  "PhysicalConnectionRequirements": {
    "SubnetId": "subnet-0abc123def456",
    "SecurityGroupIdList": ["sg-0abc123def456"],
    "AvailabilityZone": "us-east-1a"
  }
}'

# Snowflake Connection (MUST use SNOWFLAKE type)
aws glue create-connection --connection-input '{
  "Name": "prod-snowflake-analytics",
  "ConnectionType": "SNOWFLAKE",
  "ConnectionProperties": {
    "CONNECTOR_URL": "https://myaccount.snowflakecomputing.com",
    "SECRET_ID": "data-lake/prod/snowflake-creds"
  },
  "PhysicalConnectionRequirements": {
    "SubnetId": "subnet-0abc123def456",
    "SecurityGroupIdList": ["sg-0abc123def456"],
    "AvailabilityZone": "us-east-1a"
  }
}'
```

### Step 5: Two-Phase Connection Test

**Phase A — API-level test:**
```bash
aws glue test-connection --connection-name "prod-oracle-hr"

# Poll for result
aws glue get-connection --name "prod-oracle-hr" \
  --query "Connection.LastConnectionValidationTime"
```

**Phase B — Engine-level test (run a minimal Glue job):**
```python
# test_connection.py — minimal Glue job to validate connectivity
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
gc = GlueContext(sc)

df = gc.create_dynamic_frame.from_options(
    connection_type="oracle",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "prod-oracle-hr",
        "dbtable": "HR.DUAL",  # Minimal query
    },
)
print(f"Connection OK — got {df.count()} rows")
```

### Step 6: Register in Source Registry

```bash
# Update DynamoDB source registry
aws dynamodb put-item --table-name pipeline-registry-prod --item '{
  "pipeline_name": {"S": "oracle-hr-employees"},
  "run_id": {"S": "CONNECTION_REGISTERED"},
  "connection_name": {"S": "prod-oracle-hr"},
  "source_type": {"S": "jdbc"},
  "verified_at": {"S": "2024-06-01T10:00:00Z"},
  "status": {"S": "active"}
}'
```

---

## Naming Convention

```
{env}-{engine}-{system}[-{qualifier}]
```

Examples:
- `prod-oracle-hr`
- `prod-postgres-orders`
- `dev-snowflake-analytics`
- `test-mysql-inventory-readonly`
- `prod-msk-events-cluster1`

---

## VPC Requirements

All JDBC, Snowflake, BigQuery, and Kafka connections require VPC configuration:

| Requirement | Details |
|---|---|
| **Subnet** | Private subnet with route to database. Must be in same AZ as specified. |
| **Security Group** | Self-referencing inbound rule (all TCP). Outbound to DB port + S3 endpoint. |
| **Availability Zone** | Must match the subnet's AZ exactly. Mismatch causes silent failures. |
| **S3 VPC Endpoint** | Gateway endpoint required for Glue to write results to S3. Without it, jobs hang. |
| **DNS Resolution** | VPC must have DNS hostnames and DNS resolution enabled. |

**Minimum security group rules:**

```
Inbound:
  - Self-reference: All TCP (required by Glue for inter-node communication)

Outbound:
  - Database: TCP port 1521/5432/3306/5439 to DB subnet CIDR
  - S3 Endpoint: TCP 443 to S3 prefix list (pl-xxxxxxxx)
  - Glue Endpoint: TCP 443 to Glue prefix list (if using VPC endpoint for Glue)
```

---

## Troubleshooting

Follow this 5-step diagnostic order when connections fail:

### 1. VPC Routing
```bash
# Verify subnet has route to database
aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=subnet-0abc123"

# Check if subnet is in correct AZ
aws ec2 describe-subnets --subnet-ids subnet-0abc123 --query "Subnets[].AvailabilityZone"
```
**Resolution:** Ensure private subnet routes to database CIDR. Use same AZ for connection and subnet.

### 2. Security Groups
```bash
# List security group rules
aws ec2 describe-security-groups --group-ids sg-0abc123 --query "SecurityGroups[].{Ingress:IpPermissions,Egress:IpPermissionsEgress}"
```
**Resolution:** Add self-referencing inbound rule. Add outbound to DB port and S3 prefix list.

### 3. S3 VPC Endpoint
```bash
# Check for S3 endpoint in VPC
aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=vpc-xxxxx" "Name=service-name,Values=com.amazonaws.us-east-1.s3"
```
**Resolution:** Create gateway endpoint for S3 if missing. Attach to route table of Glue subnet.

### 4. Credentials
```bash
# Verify secret exists and is readable
aws secretsmanager describe-secret --secret-id "data-lake/prod/oracle-hr"

# Test secret value format (DO NOT log the actual password)
aws secretsmanager get-secret-value --secret-id "data-lake/prod/oracle-hr" \
  --query "SecretString" | python3 -c "import json,sys; d=json.load(sys.stdin); print(list(d.keys()))"
```
**Resolution:** Ensure secret has required keys (username, password). Verify Glue role has `secretsmanager:GetSecretValue` permission.

### 5. Driver Compatibility
**Resolution:** Use Glue 4.0 for latest drivers. For custom drivers, upload JAR to S3 and reference via `--extra-jars`.

---

## Gotchas

1. **Snowflake MUST use SNOWFLAKE connection type** — Using JDBC type for Snowflake will fail silently or produce authentication errors. Always use the native `SNOWFLAKE` type.

2. **AZ mismatch causes silent failure** — The `AvailabilityZone` in `PhysicalConnectionRequirements` must exactly match the subnet's AZ. A mismatch produces a generic "connection timed out" error.

3. **IAM DB auth tokens expire in 15 minutes** — If using IAM database authentication (RDS/Aurora), tokens are short-lived. The Glue job must generate a fresh token at runtime, not at connection creation.

4. **S3 VPC endpoint is mandatory** — Without a gateway endpoint for S3, Glue jobs in a VPC cannot write output or read scripts. Jobs will hang indefinitely on S3 operations.

5. **Connection names are immutable** — You cannot rename a Glue connection. To change the name, delete and recreate. Update all pipeline configs that reference the old name.

6. **Test connection vs. actual job** — `aws glue test-connection` only validates network reachability. It does not test query permissions or schema access. Always run a minimal Glue job as Phase B.

---

## Integration with Pipeline Config

Reference the Glue connection name in your pipeline YAML:

```yaml
source:
  type: jdbc
  glue_connection_name: prod-oracle-hr    # Must match exactly
  schema: HR
  table: EMPLOYEES
  incremental_column: LAST_MODIFIED_DATE
  fetch_size: 10000
```

The pipeline generator uses `glue_connection_name` to:
- Configure the Glue job's `--connections` argument
- Look up VPC settings for Terraform module generation
- Validate connectivity before first run

---

## Skill Cross-References

For deeper guidance on specific connection topics, refer to the skill's reference files:

| Topic | Skill Reference |
|-------|-----------------|
| JDBC URL formats and drivers | `skills/connecting-to-data-source/references/jdbc-setup.md` |
| Snowflake native connection | `skills/connecting-to-data-source/references/snowflake-setup.md` |
| BigQuery service account auth | `skills/connecting-to-data-source/references/bigquery-setup.md` |
| Auto-discovering existing sources | `skills/connecting-to-data-source/references/discovery.md` |
| IAM DB auth and Secrets Manager | `skills/connecting-to-data-source/references/credential-security.md` |
| VPC, subnets, and S3 endpoints | `skills/connecting-to-data-source/references/network-setup.md` |
| Structured diagnostic flow | `skills/connecting-to-data-source/references/troubleshooting.md` |
