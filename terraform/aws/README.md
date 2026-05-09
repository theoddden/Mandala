# Mandala AWS Terraform Module

Enterprise-grade AWS deployment for Mandala using Terraform.

## Features

- **AWS ElastiCache Redis** — Single-node or multi-AZ cluster with automatic failover
- **ECS Fargate** — Two tasks (mandala serve + mandala worker) with auto-scaling
- **Application Load Balancer** — HTTPS webhook endpoint with ACM certificate support
- **AWS Secrets Manager** — Secure storage for API keys (Samsara, Vizion)
- **IAM Roles** — Least-privilege access with managed policies
- **CloudWatch Logs** — Centralized logging with configurable retention

## Usage

```hcl
module "mandala" {
  source  = "theoddden/mandala/aws"
  version = "~> 0.1"

  name        = "mandala-prod"
  environment = "production"

  # Network
  vpc_id              = aws_vpc.main.id
  vpc_cidr_blocks      = ["10.0.0.0/16"]
  private_subnet_ids  = aws_subnet.private[*].id
  public_subnet_ids   = aws_subnet.public[*].id

  # Credentials
  samsara_webhook_secret = var.samsara_key
  vizion_api_key         = var.vizion_key

  # Container
  container_image = "ghcr.io/theoddden/mandala"
  container_tag   = "v0.2.0"

  # Redis
  redis_node_type     = "cache.t3.small"
  redis_num_nodes     = 1
  redis_auto_failover = false
  redis_multi_az      = false

  # ECS
  task_cpu             = 512
  task_memory          = 1024
  service_desired_count = 2

  # ALB
  alb_internal      = true
  alb_allowed_cidrs = ["10.0.0.0/8"]
  acm_certificate_arn = aws_acm_certificate.mandala.arn

  # Logs
  log_retention_days = 30
}
```

## Requirements

- Terraform >= 1.5.0
- AWS provider >= 5.0
- Existing VPC with public and private subnets
- ACM certificate (for HTTPS)

## Inputs

| Name | Description | Type | Default |
|------|-------------|------|---------|
| `name` | Name prefix for all resources | `string` | `mandala` |
| `environment` | Environment name (dev, staging, prod) | `string` | `dev` |
| `aws_region` | AWS region | `string` | `us-east-1` |
| `vpc_id` | VPC ID where resources will be deployed | `string` | - |
| `vpc_cidr_blocks` | VPC CIDR blocks for Redis security group | `list(string)` | `["10.0.0.0/16"]` |
| `private_subnet_ids` | Private subnet IDs for ECS and Redis | `list(string)` | - |
| `public_subnet_ids` | Public subnet IDs for ALB | `list(string)` | - |
| `samsara_webhook_secret` | Samsara webhook secret | `string` | - |
| `vizion_api_key` | Vizion API key for rail intermodal enrichment (optional) | `string` | `""` |
| `container_image` | Docker image for Mandala | `string` | `ghcr.io/theoddden/mandala` |
| `container_tag` | Docker image tag | `string` | `latest` |
| `redis_node_type` | ElastiCache node type | `string` | `cache.t3.micro` |
| `redis_num_nodes` | Number of Redis cache nodes | `number` | `1` |
| `redis_auto_failover` | Enable Redis automatic failover | `bool` | `false` |
| `redis_multi_az` | Enable Redis multi-AZ deployment | `bool` | `false` |
| `task_cpu` | ECS task CPU units (256 = 0.25 vCPU, 1024 = 1 vCPU) | `number` | `256` |
| `task_memory` | ECS task memory in MB | `number` | `512` |
| `service_desired_count` | Desired number of ECS tasks | `number` | `1` |
| `alb_internal` | Create internal ALB | `bool` | `true` |
| `alb_allowed_cidrs` | CIDR blocks allowed to access ALB | `list(string)` | `["0.0.0.0/0"]` |
| `acm_certificate_arn` | ACM certificate ARN for HTTPS | `string` | `""` |
| `log_retention_days` | CloudWatch log retention in days | `number` | `7` |

## Outputs

| Name | Description |
|------|-------------|
| `alb_dns_name` | DNS name of the Application Load Balancer |
| `redis_endpoint` | Redis cluster endpoint |
| `ecs_cluster_name` | ECS cluster name |
| `ecs_service_name` | ECS service name |

## Cost Estimate

Based on `us-east-1` pricing:

- **ElastiCache t3.micro**: ~$15/month
- **ECS Fargate (256 CPU, 512 MB)**: ~$9/month per task × 2 = ~$18/month
- **ALB**: ~$0.0225/hour = ~$16/month
- **CloudWatch Logs**: ~$0.50/GB ingested
- **Secrets Manager**: ~$0.40/month per secret

**Total**: ~$50-60/month for basic deployment

## Security

- IAM roles follow least-privilege principle
- Secrets stored in AWS Secrets Manager
- Security groups restrict access to VPC CIDR
- ALB supports HTTPS with ACM certificates
- No public access to Redis or ECS tasks

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
