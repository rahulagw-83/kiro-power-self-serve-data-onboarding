# Observability

## Scope

Monitor two things:
1. **Did data arrive?** (pipeline success/failure)
2. **Is it fresh?** (within expected SLA)

No quality gates, no quarantine monitoring, no contract enforcement.

---

## CloudWatch Alarms

| Alarm | Condition | Severity |
|---|---|---|
| Pipeline failure | DMS task ERROR / AppFlow flow FAILED / Firehose SUSPENDED / Glue job FAILED | P2 |
| Freshness SLA breach | No new files in Landing prefix for > SLA hours | P2 |
| 3 consecutive failures | Same pipeline fails 3+ times in a row | P1 |

### Terraform Example

```hcl
resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  alarm_name          = "landing-${var.source_name}-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FailedRuns"
  namespace           = "Landing/${var.source_name}"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [var.sns_topic_arn]
}
```

---

## Freshness Check

Query S3 for most recent object in the Landing prefix:

```bash
aws s3api list-objects-v2 \
  --bucket {landing-bucket} \
  --prefix "{domain}/{source_name}/" \
  --query "sort_by(Contents, &LastModified)[-1].LastModified"
```

If `now - last_modified > sla_hours` → fire freshness alarm.

---

## What We Do NOT Monitor

- Data quality scores (no DQDL, no quarantine)
- Schema drift (no schema enforcement)
- Row counts or volume anomalies (profiling report covers this informally)
- Contract SLA adherence (no contracts)
