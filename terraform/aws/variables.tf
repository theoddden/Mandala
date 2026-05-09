variable "name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "mandala"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID where resources will be deployed"
  type        = string
}

variable "vpc_cidr_blocks" {
  description = "VPC CIDR blocks for Redis security group"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS and Redis"
  type        = list(string)
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for ALB"
  type        = list(string)
}

variable "samsara_webhook_secret" {
  description = "Samsara webhook secret"
  type        = string
  sensitive   = true
}

variable "vizion_api_key" {
  description = "Vizion API key for rail intermodal enrichment (optional)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "container_image" {
  description = "Docker image for Mandala"
  type        = string
  default     = "ghcr.io/theoddden/mandala"
}

variable "container_tag" {
  description = "Docker image tag"
  type        = string
  default     = "latest"
}

variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.t3.micro"
}

variable "redis_num_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 1
}

variable "redis_auto_failover" {
  description = "Enable Redis automatic failover"
  type        = bool
  default     = false
}

variable "redis_multi_az" {
  description = "Enable Redis multi-AZ deployment"
  type        = bool
  default     = false
}

variable "task_cpu" {
  description = "ECS task CPU units (256 = 0.25 vCPU, 1024 = 1 vCPU)"
  type        = number
  default     = 256
}

variable "task_memory" {
  description = "ECS task memory in MB"
  type        = number
  default     = 512
}

variable "service_desired_count" {
  description = "Desired number of ECS tasks"
  type        = number
  default     = 1
}

variable "alb_internal" {
  description = "Create internal ALB"
  type        = bool
  default     = true
}

variable "alb_allowed_cidrs" {
  description = "CIDR blocks allowed to access ALB"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN for HTTPS"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}
