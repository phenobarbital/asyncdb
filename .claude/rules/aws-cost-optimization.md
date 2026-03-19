---
description: Evaluate AWS costs and generate actionable optimization suggestions. Covers EC2, S3, RDS, Lambda, ECS/EKS, and data transfer. Use when analyzing AWS spending, right-sizing resources, choosing pricing models (RI/Savings Plans/Spot), implementing tagging strategies, setting budget alerts, or auditing infrastructure for cost waste.
globs:
  - "**/*.tf"
  - "**/infrastructure/**"
  - "**/terraform/**"
  - "**/cloudformation/**"
---

# AWS Cost Optimization

Evaluate current AWS costs and produce actionable optimization suggestions across compute, storage, networking, and managed services.

## When NOT to use

- The task targets Azure, GCP, or a non-AWS provider
- The task is about general application performance without a cost dimension

## Instructions

1. Identify the AWS services in scope (EC2, S3, RDS, Lambda, etc.).
2. Apply the **Cost Optimization Framework** top-to-bottom: Visibility → Right-Sizing → Pricing → Architecture.
3. For each service, check the **Service-Specific Quick Reference** below.
4. Generate a prioritized list of suggestions with estimated savings.

---

## Cost Optimization Framework

### 1. Visibility — Know What You Spend

| Action | Tool / Service |
|--------|---------------|
| Cost allocation tags on every resource | AWS Tag Editor, Tag Policies |
| Monthly/daily spend dashboards | Cost Explorer, QuickSight |
| Budget alerts at 50 %, 80 %, 100 % | AWS Budgets |
| Anomaly detection | AWS Cost Anomaly Detection |
| Per-team / per-env cost breakdown | Cost Categories, Linked Accounts |

### 2. Right-Sizing — Stop Over-Provisioning

| Signal | Action |
|--------|--------|
| CPU < 20 % sustained | Downsize instance family or switch to Graviton |
| Memory < 30 % sustained | Use memory-optimized → general purpose |
| EBS IOPS < provisioned | Switch gp3 / reduce provisioned IOPS |
| Idle resource (EIP, ELB, NAT, EBS) | Delete or stop |
| Lambda memory > 2× needed | Run `aws lambda get-function-configuration` and tune |

Use **AWS Compute Optimizer** and **Trusted Advisor** for automated right-sizing recommendations.

### 3. Pricing Models — Pay Less Per Unit

| Model | Savings | Best For | Commitment |
|-------|---------|----------|------------|
| On-Demand | 0 % | Spiky, unpredictable | None |
| Savings Plans (Compute) | up to 66 % | Steady compute (EC2, Fargate, Lambda) | 1 or 3 yr |
| Savings Plans (EC2 Instance) | up to 72 % | Known instance family & region | 1 or 3 yr |
| Reserved Instances (Standard) | up to 72 % | Steady-state, known type | 1 or 3 yr |
| Reserved Instances (Convertible) | up to 54 % | Steady-state, flexible type | 1 or 3 yr |
| Spot Instances | up to 90 % | Fault-tolerant batch, CI/CD, HPC | None (2-min notice) |

**Decision heuristic:**
1. Steady 24/7 → Savings Plan or RI
2. Batch / stateless → Spot with On-Demand fallback
3. Unknown workload → start On-Demand, analyze with Cost Explorer, then commit

### 4. Architecture — Spend Smarter

| Pattern | Why |
|---------|-----|
| Serverless First | Zero idle cost; Lambda, Step Functions, EventBridge |
| Graviton (ARM) instances | 20-40 % cheaper at same performance |
| Multi-tier S3 storage | Auto-transition hot → IA → Glacier → Deep Archive |
| Caching (ElastiCache, CloudFront) | Reduce origin hits and data transfer |
| VPC Endpoints | Eliminate NAT Gateway data processing charges |
| Regional consolidation | Reduce cross-region transfer costs |

---

## Service-Specific Quick Reference

### EC2

- Use **Graviton** (`c7g`, `m7g`, `r7g`) for 20-40 % cost reduction.
- Mix **Spot + On-Demand** via Capacity-Optimized allocation in ASGs.
- Enable **auto-scaling** with target tracking (CPU 60-70 %).
- Schedule dev/staging instances off-hours with Instance Scheduler.

### S3

| Storage Class | Use Case | vs Standard |
|---------------|----------|-------------|
| Standard | Frequently accessed | baseline |
| Standard-IA | Accessed < 1×/month | –45 % |
| One Zone-IA | Non-critical, infrequent | –60 % |
| Glacier Instant Retrieval | Quarterly access, ms retrieval | –68 % |
| Glacier Flexible Retrieval | Annual access, hours retrieval | –78 % |
| Deep Archive | Compliance / 7-yr retention | –95 % |

Implement **S3 Intelligent-Tiering** when access patterns are unpredictable.

### RDS / Aurora

| Environment | Recommended Tier |
|-------------|-----------------|
| Development | `db.t4g.micro` – `db.t4g.small` |
| Staging | `db.t4g.medium` – `db.t4g.large` |
| Production | `db.r7g.xlarge` + read replicas |

- Use **Aurora Serverless v2** for variable traffic.
- Enable **RDS Reserved Instances** for production.
- Use **Aurora I/O-Optimized** if I/O costs > 25 % of total DB cost.

### Lambda

- Right-size memory with **AWS Lambda Power Tuning**.
- Use **Graviton2** (`arm64` architecture) for ~34 % cost reduction.
- Enable **Provisioned Concurrency** only when cold-start SLA < 100 ms.
- Prefer **Step Functions** over chained Lambdas to avoid idle billing.

### ECS / EKS

- Use **Fargate Spot** for fault-tolerant tasks (up to 70 % savings).
- Use **Compute Savings Plans** for steady Fargate workloads.
- For EKS: enable **Karpenter** for intelligent node provisioning.
- Right-size task/pod CPU and memory with Container Insights.

### Data Transfer

| Path | Cost | Mitigation |
|------|------|------------|
| Same AZ | Free | Co-locate services |
| Cross-AZ | $0.01/GB each way | Use AZ-aware routing |
| Internet egress | $0.09/GB first 10 TB | CloudFront, S3 Transfer Acceleration |
| Cross-region | $0.02/GB | Consolidate regions |
| NAT Gateway processing | $0.045/GB | VPC Endpoints for S3/DynamoDB |

---

## Tagging Strategy (Mandatory)

Every AWS resource MUST have these tags:

| Tag Key | Example | Purpose |
|---------|---------|---------|
| `Environment` | `production` | Filter by env |
| `Project` | `navigator` | Cost allocation |
| `CostCenter` | `engineering` | Chargeback |
| `Owner` | `team@example.com` | Accountability |
| `ManagedBy` | `terraform` | Audit |

Enforce via **AWS Organizations Tag Policies** and **SCP** deny rules.

---

## Cost Evaluation Workflow

When asked to evaluate costs, follow this sequence:

1. **Inventory**: List services, instance types, storage volumes, and data flows.
2. **Tag audit**: Check for missing cost-allocation tags.
3. **Utilization check**: Review CloudWatch metrics (CPU, memory, IOPS, network).
4. **Pricing check**: Compare current pricing model vs optimal (RI/SP/Spot).
5. **Architecture review**: Identify unnecessary data transfer, missing caching, idle resources.
6. **Report**: Produce a table of findings with:
   - Resource / Service
   - Current monthly cost (estimated or from Cost Explorer)
   - Suggested action
   - Estimated savings (% and $)
   - Effort (low / medium / high)
   - Risk (low / medium / high)

---

## Key Terraform Examples

### S3 Lifecycle Policy

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "cost_optimized" {
  bucket = aws_s3_bucket.main.id

  rule {
    id     = "tiered-storage"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    transition {
      days          = 180
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = 730
    }
  }
}
```

### Budget Alerts

```hcl
resource "aws_budgets_budget" "monthly" {
  name              = "${var.project}-monthly-budget"
  budget_type       = "COST"
  limit_amount      = var.monthly_budget_usd
  limit_unit        = "USD"
  time_period_start = "2024-01-01_00:00"
  time_unit         = "MONTHLY"

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = var.alert_emails
  }
}
```

### Auto-Scaling with Spot + Graviton

```hcl
resource "aws_autoscaling_group" "app" {
  name                = "${var.project}-asg"
  min_size            = var.asg_min
  max_size            = var.asg_max
  vpc_zone_identifier = var.private_subnet_ids

  mixed_instances_policy {
    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.app.id
        version            = "$Latest"
      }
      override { instance_type = "m7g.large" }
      override { instance_type = "m6g.large" }
    }
    instances_distribution {
      on_demand_base_capacity                  = 1
      on_demand_percentage_above_base_capacity = 25
      spot_allocation_strategy                 = "capacity-optimized"
    }
  }
}
```

### VPC Endpoints (eliminate NAT charges)

```hcl
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.private_route_table_ids
}
```

### Cost Explorer Query (boto3)

```python
import datetime, boto3

def get_monthly_cost_by_service() -> list[dict]:
    """Return cost breakdown by AWS service for the last 30 days."""
    client = boto3.client("ce")
    end = datetime.date.today()
    start = end - datetime.timedelta(days=30)
    response = client.get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    results = []
    for group in response["ResultsByTime"][0]["Groups"]:
        service = group["Keys"][0]
        amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
        if amount > 0.01:
            results.append({"service": service, "cost_usd": round(amount, 2)})
    return sorted(results, key=lambda x: x["cost_usd"], reverse=True)
```

---

## Tools

- **AWS Cost Explorer** — Spend trends, forecasting, RI/SP recommendations
- **AWS Compute Optimizer** — EC2, EBS, Lambda right-sizing
- **AWS Trusted Advisor** — Idle resources, under-utilized instances
- **AWS Cost Anomaly Detection** — ML-based spend anomaly alerts
- **AWS Budgets** — Threshold alerts and auto-actions
- **Kubecost** — Kubernetes cost allocation (EKS)
