---
inclusion: fileMatch
fileMatchPattern: '**/pipeline-config*'
---

# Pre-Deployment Validation

## Purpose

Mandatory validation phase that runs BEFORE generating or deploying any pipeline.
If any check fails, stop immediately — do not proceed to code generation or deployment.

---

## Validation Sequence

```
0. Existing Resource Scan      → What already exists? Reuse or create?
1. Connectivity Validation     → Can we reach the source?
2. Credential Validation       → Are credentials correct and fresh?
3. Networking Validation       → Are SGs, subnets, and endpoints configured?
4. IAM Validation              → Do roles have all required permissions?
5. ASL Validation              → Is the state machine definition valid?
6. Worker Type Validation      → Is the Glue worker type valid for this job command?
```

All checks MUST pass before proceeding.

---

## 0. Existing Resource Scan

Before creating anything, check what already exists:

```bash
# DMS instances (reuse saves ~$75/month)
aws dms describe-replication-instances \
  --query "ReplicationInstances[].{Id:ReplicationInstanceIdentifier,Class:ReplicationInstanceClass,Status:ReplicationInstanceStatus}"

# DMS tasks (detect duplicates targeting same source)
aws dms describe-replication-tasks \
  --query "ReplicationTasks[].{Id:ReplicationTaskIdentifier,Source:SourceEndpointArn,Status:Status}"

# S3 buckets with landing/data pattern
aws s3api list-buckets --query "Buckets[?contains(Name,'landing') || contains(Name,'data')].Name"

# AppFlow connector profiles
aws appflow describe-connector-profiles --query "ConnectorProfileDetailList[].{Name:ConnectorProfileName,Type:ConnectorType}"

# Transfer Family servers
aws transfer list-servers --query "Servers[].{Id:ServerId,State:State}"

# Firehose streams
aws firehose list-delivery-streams --query "DeliveryStreamNames"
```

**Decision logic:**
- Existing DMS instance with capacity → offer to reuse (add task to existing instance)
- Existing S3 landing bucket → reuse (different prefix per source)
- Existing AppFlow profile for same SaaS → reuse (don't create duplicate)
- Existing DMS task for same source → WARN (duplicate onboarding detected)
- No existing resources → proceed with full creation

**Present findings to user and get confirmation before generating Terraform.**

---

## 1. Connectivity Validation

| Source Type | Check | Command |
|---|---|---|
| JDBC (Glue connection) | Network + auth | `aws glue test-connection --connection-name {name}` |
| S3 source | Bucket accessible | `aws s3 ls s3://{bucket}/{prefix}/ --max-items 1` |
| DMS endpoint | Reachable | `aws dms test-connection --replication-instance-arn {arn} --endpoint-arn {arn}` |
| AppFlow connector | OAuth valid | `aws appflow describe-connector-profiles --connector-profile-names {name}` |
| Transfer Family | Server active | `aws transfer describe-server --server-id {id}` + check State=ONLINE |

**On failure:** Stop. Present the specific error. Do NOT generate infrastructure.

---

## 2. Credential Validation

```bash
# Verify secret exists
aws secretsmanager describe-secret --secret-id {secret_arn}

# Verify secret has required keys (DO NOT log values)
aws secretsmanager get-secret-value --secret-id {secret_arn} \
  --query SecretString | python3 -c "
import json, sys
d = json.loads(json.load(sys.stdin))
required = ['username', 'password', 'host', 'port']
missing = [k for k in required if k not in d]
if missing:
    print(f'FAIL: Missing keys: {missing}')
    sys.exit(1)
print(f'OK: Keys present: {list(d.keys())}')
"

# Check secret age
aws secretsmanager describe-secret --secret-id {secret_arn} \
  --query "LastChangedDate" --output text
```

**Checks:**
- Secret exists and is readable by the Glue/DMS role
- Required keys are present (`username`, `password`, `host`, `port` for JDBC)
- Secret `host` field matches the declared source endpoint
- WARN if last rotated > 90 days ago
- WARN if secret points to a different endpoint than declared in config

---

## 3. Networking Validation (Glue JDBC / DMS)

### Self-Referencing Security Group Rule

Glue **requires** a self-referencing ingress rule (all TCP to itself) on the security group attached to the connection. Without it, Glue jobs fail at runtime with cryptic timeout errors.

```bash
# Check if self-referencing rule exists
SG_ID=$(aws glue get-connection --name {conn_name} \
  --query 'Connection.PhysicalConnectionRequirements.SecurityGroupIdList[0]' --output text)

aws ec2 describe-security-groups --group-ids $SG_ID \
  --query "SecurityGroups[0].IpPermissions[?UserIdGroupPairs[?GroupId=='$SG_ID']]"
```

**If missing and IAM allows `ec2:AuthorizeSecurityGroupIngress`:**
```bash
# Auto-fix: add self-referencing rule
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 0-65535 \
  --source-group $SG_ID
echo "Auto-added self-referencing SG rule to $SG_ID"
```

### Subnet Routing

```bash
# Verify subnet has route to database CIDR
SUBNET_ID=$(aws glue get-connection --name {conn_name} \
  --query 'Connection.PhysicalConnectionRequirements.SubnetId' --output text)

aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=$SUBNET_ID" \
  --query "RouteTables[0].Routes[].{Dest:DestinationCidrBlock,Target:GatewayId||NatGatewayId||VpcPeeringConnectionId}"
```

### S3 VPC Endpoint

```bash
# Verify S3 gateway endpoint exists in the VPC
VPC_ID=$(aws ec2 describe-subnets --subnet-ids $SUBNET_ID --query 'Subnets[0].VpcId' --output text)

aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=service-name,Values=com.amazonaws.{region}.s3" \
  --query "VpcEndpoints[0].VpcEndpointId"
```

**If missing:** Create it:
```bash
RTB_ID=$(aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=$SUBNET_ID" \
  --query "RouteTables[0].RouteTableId" --output text)

aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --service-name com.amazonaws.{region}.s3 \
  --route-table-ids $RTB_ID
```

---

## 4. IAM Validation

Enumerate API calls made by the generated state machine and Glue job, then verify permissions:

**Step Functions execution role needs:**
- `states:StartExecution` (if nested workflows)
- `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`
- `glue:GetConnection` (if Glue job uses connections)
- `events:PutEvents` (for EventBridge notifications)
- `sns:Publish` (for alerts)
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

**Ingestion service role needs (DMS/AppFlow/Firehose/Transfer Family):**
- `s3:PutObject`, `s3:ListBucket` on Landing bucket
- `secretsmanager:GetSecretValue` on the source secret (if applicable)
- `cloudwatch:PutMetricData` (for custom metrics)
- Service-specific permissions (e.g., `dms:*` for DMS role, `appflow:*` for AppFlow)

```bash
# Simulate role permissions
aws iam simulate-principal-policy \
  --policy-source-arn {role_arn} \
  --action-names glue:StartJobRun s3:GetObject secretsmanager:GetSecretValue \
  --query "EvaluationResults[?EvalDecision!='allowed'].{Action:EvalActionName,Decision:EvalDecision}"
```

**Auto-fix (with user confirmation):** Generate and attach inline policy for missing permissions.

---

## 5. ASL (State Machine) Validation

```bash
aws stepfunctions validate-state-machine-definition \
  --definition file://resources/state_machine.json \
  --type STANDARD
```

**Common issues:**
- `States.Format` strings with invalid JSONPath → fix the expression
- State transitions referencing non-existent states → fix the state name
- Missing `End: true` or `Next` field → add terminal state marker

---

## 6. Worker Type Validation

| Glue Job Command | Valid Worker Types | Invalid |
|---|---|---|
| `glueetl` | G.1X, G.2X, G.4X, G.8X | **G.025X** (streaming only) |
| `gluestreaming` | G.025X, G.1X, G.2X | G.4X, G.8X |
| `pythonshell` | Standard, Standard_2.0 | All G-types |

**Auto-fix:** If `G.025X` specified for `glueetl`, auto-correct to `G.1X` and notify user.

---

## Validation Report

After all checks complete, present a summary:

```
Pre-Deployment Validation Report
═══════════════════════════════════

Source: aurora-orders (DMS CDC → S3 Landing)
Environment: dev

✅ Connectivity: DMS endpoint reachable, test-connection passed
✅ Credentials: Secret exists, keys valid, last rotated 15 days ago
✅ Networking: SG self-reference present, S3 VPC endpoint exists, subnet routes OK
✅ IAM: SFN role has all permissions, Glue role has all permissions
✅ ASL: State machine definition valid (0 errors, 0 warnings)
✅ Worker Type: G.1X valid for glueetl command

All checks passed. Ready to generate and deploy.
```

Or on failure:

```
❌ Networking: Missing self-referencing SG rule on sg-0abc123
   → Auto-fixed: Added self-referencing rule

❌ IAM: Glue role missing secretsmanager:GetSecretValue
   → Action required: Attach policy. Shall I add it?

Blocking issues: 1 (IAM). Cannot proceed until resolved.
```

---

## MUST Rules

- MUST run ALL 6 validation checks before any code generation or deployment
- MUST stop on first blocking failure — do not proceed with partial validation
- MUST auto-fix SG self-referencing rules when IAM permits (non-destructive)
- MUST ask user confirmation before modifying IAM policies
- MUST NOT log credential values — only check key existence
- MUST validate ASL before deploying state machines (catches runtime failures early)
- MUST validate worker type against job command type (G.025X is glueetl trap)
