# CDK Infrastructure

## Overview

This guide provides AWS CDK patterns for deploying data ingestion pipeline infrastructure. All resources follow Well-Architected best practices: least-privilege IAM, encryption at rest, tagging, and monitoring.

## Prerequisites

```bash
# Install AWS CDK CLI
npm install -g aws-cdk

# Install Python dependencies
pip install -r requirements.txt
```

**requirements.txt:**
```
aws-cdk-lib>=2.100.0
constructs>=10.0.0
```

---

## CDK App Entry Point

```python
#!/usr/bin/env python3
# app.py
import aws_cdk as cdk
from stacks.s3_file_ingestion_stack import S3FileIngestionStack
from stacks.jdbc_ingestion_stack import JdbcIngestionStack

app = cdk.App()

env_name = app.node.try_get_context("env") or "dev"
account = app.node.try_get_context("account")
region = app.node.try_get_context("region") or "us-east-1"

env = cdk.Environment(account=account, region=region)

# Deploy S3 file ingestion pipeline
S3FileIngestionStack(
    app,
    f"s3-file-ingestion-{env_name}",
    env=env,
    env_name=env_name,
)

# Deploy JDBC ingestion pipeline (if needed)
JdbcIngestionStack(
    app,
    f"jdbc-ingestion-{env_name}",
    env=env,
    env_name=env_name,
)

app.synth()
```

---

## S3 File Ingestion Stack

```python
# stacks/s3_file_ingestion_stack.py
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_iam as iam,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    CfnOutput,
)


class S3FileIngestionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ─── S3 Buckets ───────────────────────────────────────────────

        # Source bucket — where raw files land
        source_bucket = s3.Bucket(
            self, "SourceBucket",
            bucket_name=f"data-lake-source-{env_name}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="archive-after-90-days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(90),
                        )
                    ],
                    expiration=Duration.days(365),
                ),
            ],
        )

        # Target bucket — Iceberg table storage
        target_bucket = s3.Bucket(
            self, "TargetBucket",
            bucket_name=f"data-lake-bronze-{env_name}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-old-iceberg-snapshots",
                    noncurrent_version_expiration=Duration.days(7),
                ),
            ],
        )

        # ─── DynamoDB Pipeline Registry ───────────────────────────────

        registry_table = dynamodb.Table(
            self, "PipelineRegistry",
            table_name=f"pipeline-registry-{env_name}",
            partition_key=dynamodb.Attribute(
                name="pipeline_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="run_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
            time_to_live_attribute="ttl",
        )

        # ─── SNS Alert Topic ─────────────────────────────────────────

        alert_topic = sns.Topic(
            self, "AlertTopic",
            topic_name=f"pipeline-alerts-{env_name}",
            display_name="Data Pipeline Alerts",
        )

        # ─── IAM Role for Glue Job ───────────────────────────────────

        glue_role = iam.Role(
            self, "GlueJobRole",
            role_name=f"glue-ingestion-role-{env_name}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # Grant least-privilege access using CDK grant methods
        source_bucket.grant_read(glue_role)
        target_bucket.grant_read_write(glue_role)
        registry_table.grant_read_write_data(glue_role)

        # ─── Glue Database ────────────────────────────────────────────

        glue_database = glue.CfnDatabase(
            self, "BronzeDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"bronze_{env_name}",
                description="Bronze layer — raw ingested data as Iceberg tables",
            ),
        )

        # ─── Glue ETL Job ─────────────────────────────────────────────

        glue_job = glue.CfnJob(
            self, "IngestionJob",
            name=f"s3-file-ingest-{env_name}",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://data-lake-scripts-{env_name}/glue/s3_file_ingest.py",
                python_version="3",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=120,
            max_retries=1,
            default_arguments={
                "--enable-iceberg": "true",
                "--datalake-formats": "iceberg",
                "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
                "--TempDir": f"s3://data-lake-temp-{env_name}/glue/",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--ENV": env_name,
                "--TARGET_BUCKET": target_bucket.bucket_name,
                "--DATABASE": f"bronze_{env_name}",
            },
        )

        # ─── Step Functions Orchestration ─────────────────────────────

        start_job_task = tasks.GlueStartJobRun(
            self, "StartGlueJob",
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--SOURCE_PATH.$": "$.source_path",
                "--TABLE_NAME.$": "$.table_name",
            }),
            result_path="$.glue_result",
        )

        notify_success = tasks.SnsPublish(
            self, "NotifySuccess",
            topic=alert_topic,
            message=sfn.TaskInput.from_json_path_at("$.glue_result"),
            subject="Pipeline Succeeded",
        )

        notify_failure = tasks.SnsPublish(
            self, "NotifyFailure",
            topic=alert_topic,
            message=sfn.TaskInput.from_json_path_at("$.error"),
            subject="Pipeline FAILED",
        )

        definition = (
            start_job_task
            .add_catch(notify_failure, result_path="$.error")
            .next(notify_success)
        )

        state_machine = sfn.StateMachine(
            self, "PipelineStateMachine",
            state_machine_name=f"s3-file-pipeline-{env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(3),
        )

        # ─── EventBridge Schedule ─────────────────────────────────────

        schedule_rule = events.Rule(
            self, "ScheduleRule",
            rule_name=f"s3-file-pipeline-schedule-{env_name}",
            schedule=events.Schedule.rate(Duration.hours(1)),
        )
        schedule_rule.add_target(
            targets.SfnStateMachine(
                state_machine,
                input=events.RuleTargetInput.from_object({
                    "source_path": f"s3://{source_bucket.bucket_name}/incoming/",
                    "table_name": "raw_events",
                }),
            )
        )

        # ─── CloudWatch Alarms ────────────────────────────────────────

        job_failure_alarm = cw.Alarm(
            self, "GlueJobFailureAlarm",
            alarm_name=f"glue-job-failure-{env_name}",
            metric=cw.Metric(
                namespace="Glue",
                metric_name="glue.driver.aggregate.numFailedTasks",
                dimensions_map={"JobName": glue_job.name, "Type": "gauge"},
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        job_failure_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        sfn_failure_alarm = cw.Alarm(
            self, "StepFunctionFailureAlarm",
            alarm_name=f"pipeline-sfn-failure-{env_name}",
            metric=state_machine.metric_failed(),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        sfn_failure_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ─── Stack Outputs ────────────────────────────────────────────

        CfnOutput(self, "SourceBucketName", value=source_bucket.bucket_name)
        CfnOutput(self, "TargetBucketName", value=target_bucket.bucket_name)
        CfnOutput(self, "GlueJobName", value=glue_job.name)
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "AlertTopicArn", value=alert_topic.topic_arn)
```

---

## JDBC/RDS Additional Resources

Add these resources when ingesting from databases via JDBC connections:

```python
# stacks/jdbc_ingestion_stack.py (additional constructs)
from aws_cdk import (
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue,
    aws_lakeformation as lakeformation,
)

# ─── KMS Key for Sensitive Data ───────────────────────────────────

data_key = kms.Key(
    self, "DataEncryptionKey",
    alias=f"alias/data-lake-{env_name}",
    description="Encryption key for data lake sensitive data",
    enable_key_rotation=True,
    removal_policy=RemovalPolicy.RETAIN,
)

# ─── VPC Security Group ──────────────────────────────────────────

vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id="vpc-xxxxxxxx")

glue_sg = ec2.SecurityGroup(
    self, "GlueSecurityGroup",
    vpc=vpc,
    security_group_name=f"glue-jdbc-sg-{env_name}",
    description="Security group for Glue JDBC connections",
    allow_all_outbound=False,
)

# Self-referencing rule required for Glue
glue_sg.add_ingress_rule(glue_sg, ec2.Port.all_tcp(), "Glue self-reference")
# Outbound to database
glue_sg.add_egress_rule(
    ec2.Peer.ipv4("10.0.0.0/16"),
    ec2.Port.tcp(1521),  # Oracle; adjust per DB type
    "Outbound to database",
)
# Outbound to S3 via endpoint
glue_sg.add_egress_rule(
    ec2.Peer.prefix_list("pl-xxxxxxxx"),  # S3 prefix list
    ec2.Port.tcp(443),
    "Outbound to S3 endpoint",
)

# ─── Secrets Manager ─────────────────────────────────────────────

db_secret = secretsmanager.Secret(
    self, "DatabaseCredentials",
    secret_name=f"data-lake/{env_name}/oracle-hr",
    description="Oracle HR database credentials for Glue connection",
    encryption_key=data_key,
    generate_secret_string=secretsmanager.SecretStringGenerator(
        secret_string_template='{"username":"glue_reader"}',
        generate_string_key="password",
        exclude_punctuation=True,
        password_length=32,
    ),
)

# ─── Glue Connection ─────────────────────────────────────────────

jdbc_connection = glue.CfnConnection(
    self, "OracleConnection",
    catalog_id=self.account,
    connection_input=glue.CfnConnection.ConnectionInputProperty(
        name=f"oracle-hr-{env_name}",
        connection_type="JDBC",
        connection_properties={
            "JDBC_CONNECTION_URL": "jdbc:oracle:thin:@//db-host:1521/HRDB",
            "SECRET_ID": db_secret.secret_name,
        },
        physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
            subnet_id="subnet-xxxxxxxx",
            security_group_id_list=[glue_sg.security_group_id],
            availability_zone="us-east-1a",
        ),
    ),
)

# ─── DynamoDB Watermark Table ─────────────────────────────────────

watermark_table = dynamodb.Table(
    self, "WatermarkTable",
    table_name=f"pipeline-watermarks-{env_name}",
    partition_key=dynamodb.Attribute(
        name="pipeline_name", type=dynamodb.AttributeType.STRING
    ),
    billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
    removal_policy=RemovalPolicy.RETAIN,
    point_in_time_recovery=True,
)

# ─── Lake Formation Tags ─────────────────────────────────────────

lf_tag = lakeformation.CfnTag(
    self, "ClassificationTag",
    catalog_id=self.account,
    tag_key="classification",
    tag_values=["public", "internal", "pii", "pci", "phi"],
)

lf_tag_domain = lakeformation.CfnTag(
    self, "DomainTag",
    catalog_id=self.account,
    tag_key="domain",
    tag_values=["customer", "finance", "hr", "operations"],
)

# ─── Freshness SLA Alarm ─────────────────────────────────────────

freshness_alarm = cw.Alarm(
    self, "FreshnessSlaAlarm",
    alarm_name=f"pipeline-freshness-sla-{env_name}",
    metric=cw.Metric(
        namespace="DataPipeline",
        metric_name="HoursSinceLastSuccess",
        dimensions_map={"Pipeline": f"jdbc-ingest-{env_name}"},
        period=Duration.hours(1),
        statistic="Maximum",
    ),
    threshold=8,  # From data contract freshness_sla_hours
    evaluation_periods=1,
    comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
    treat_missing_data=cw.TreatMissingData.BREACHING,
)
freshness_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

# ─── Additional IAM Permissions for JDBC ─────────────────────────

# Grant Glue role access to secrets and KMS
db_secret.grant_read(glue_role)
data_key.grant_decrypt(glue_role)
watermark_table.grant_read_write_data(glue_role)

# VPC permissions for Glue
glue_role.add_to_policy(iam.PolicyStatement(
    actions=[
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcAttribute",
    ],
    resources=["*"],
    conditions={
        "StringEquals": {"ec2:Vpc": vpc.vpc_arn}
    },
))
```

---

## Deployment Commands

```bash
# Synthesize CloudFormation templates
cdk synth --context env=dev

# Deploy to dev
cdk deploy s3-file-ingestion-dev --context env=dev --context account=111111111111

# Deploy to test
cdk deploy s3-file-ingestion-test --context env=test --context account=222222222222

# Deploy to production
cdk deploy s3-file-ingestion-prod --context env=prod --context account=333333333333 --require-approval broadening

# Compare changes before deploying
cdk diff s3-file-ingestion-prod --context env=prod

# Destroy (non-production only)
cdk destroy s3-file-ingestion-dev --context env=dev
```

---

## Environment-Specific Configuration

| Resource | Dev | Test | Production |
|---|---|---|---|
| Glue Workers | 2 × G.025X | 4 × G.1X | 10 × G.2X |
| S3 Lifecycle | 30 days expire | 90 days to IA | 365 days to Glacier |
| Alarm Threshold | 3 failures | 2 failures | 1 failure |
| Schedule | Every 4 hours | Every 2 hours | Every 1 hour |
| Removal Policy | DESTROY | RETAIN | RETAIN |
| Require Approval | Never | Broadening | Broadening |
| Monitoring | Basic | Enhanced | Enhanced + dashboards |

---

## Tagging Strategy

```python
# Apply consistent tags to all resources in the stack
cdk.Tags.of(self).add("project", "data-lake")
cdk.Tags.of(self).add("environment", env_name)
cdk.Tags.of(self).add("team", "data-platform")
cdk.Tags.of(self).add("cost-center", "CC-DATA-001")
cdk.Tags.of(self).add("managed-by", "cdk")
cdk.Tags.of(self).add("pipeline-type", "ingestion")
```

---

## Best Practices

1. **One stack per pipeline** — Isolate blast radius. Each data source gets its own stack for independent deployment and rollback.

2. **Use CDK context for environment config** — Never hardcode account IDs, regions, or environment-specific values. Pass them via `--context` or `cdk.json`.

3. **Set RemovalPolicy.RETAIN for data resources** — S3 buckets, DynamoDB tables, and Glue databases should never be accidentally deleted during stack updates.

4. **Use grant methods over inline policies** — `bucket.grant_read(role)` generates minimal IAM policies automatically and stays in sync with resource ARNs.

5. **Tag everything** — Use `cdk.Tags.of(self).add()` at the stack level so all child resources inherit cost allocation and ownership tags.

6. **Enable encryption everywhere** — S3 buckets use SSE-S3 or SSE-KMS, DynamoDB uses AWS-owned keys, Secrets Manager uses customer-managed KMS.

7. **CloudWatch alarms from day one** — Every pipeline needs at minimum: job failure alarm, freshness SLA alarm, and Step Functions failure alarm.

8. **Separate scripts bucket** — Store Glue job scripts in a dedicated S3 bucket with versioning. Reference scripts by S3 path, never inline.

9. **Use VPC endpoints** — For JDBC pipelines, ensure S3 and Glue VPC endpoints exist to avoid NAT gateway costs and improve security.

10. **Test with `cdk diff`** — Always run `cdk diff` before deploying to production to review infrastructure changes.
