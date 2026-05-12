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

variable "redis_ha_enabled" {
  description = "Enable Redis HA with automatic failover and encryption"
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

variable "samsara_api_token" {
  description = "Samsara API token for outbound enrichment"
  type        = string
  default     = ""
  sensitive   = true
}

variable "samsara_base_url" {
  description = "Samsara API base URL"
  type        = string
  default     = "https://api.samsara.com"
}

variable "samsara_outbound_enabled" {
  description = "Enable Samsara outbound enrichment"
  type        = string
  default     = "0"
}

variable "descartes_webhook_secret" {
  description = "Descartes webhook secret"
  type        = string
  default     = ""
  sensitive   = true
}

variable "descartes_api_key" {
  description = "Descartes API key"
  type        = string
  default     = ""
  sensitive   = true
}

variable "descartes_base_url" {
  description = "Descartes API base URL"
  type        = string
  default     = "https://gln.descartes.com"
}

variable "cargowise_webhook_secret" {
  description = "CargoWise webhook secret"
  type        = string
  default     = ""
  sensitive   = true
}

variable "cargowise_eadaptor_url" {
  description = "CargoWise eAdaptor URL"
  type        = string
  default     = ""
}

variable "cargowise_username" {
  description = "CargoWise username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "cargowise_password" {
  description = "CargoWise password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "cargowise_organization_code" {
  description = "CargoWise organization code"
  type        = string
  default     = ""
}

variable "loadboard_enabled" {
  description = "Enable load-board auto-posting"
  type        = string
  default     = "0"
}

variable "loadboard_post_default_radius_mi" {
  description = "Default radius for load-board posts in miles"
  type        = number
  default     = 250
}

variable "loadboard_post_ttl_hours" {
  description = "TTL for load-board posts in hours"
  type        = number
  default     = 24
}

variable "dat_client_id" {
  description = "DAT One client ID"
  type        = string
  default     = ""
  sensitive   = true
}

variable "dat_client_secret" {
  description = "DAT One client secret"
  type        = string
  default     = ""
  sensitive   = true
}

variable "otlp_endpoint" {
  description = "OTLP endpoint for trace export"
  type        = string
  default     = ""
}

variable "event_log_enabled" {
  description = "Enable Apache Iceberg event log"
  type        = string
  default     = "0"
}

variable "iceberg_catalog" {
  description = "Iceberg catalog type"
  type        = string
  default     = "rest"
}

variable "iceberg_catalog_uri" {
  description = "Iceberg catalog URI"
  type        = string
  default     = ""
}

variable "iceberg_warehouse" {
  description = "Iceberg warehouse path (S3/GCS/Azure)"
  type        = string
  default     = ""
}

variable "iceberg_table" {
  description = "Iceberg table name"
  type        = string
  default     = "mandala.events"
}

variable "iceberg_namespace" {
  description = "Iceberg namespace"
  type        = string
  default     = "mandala"
}

variable "zk_enabled" {
  description = "Enable Zero-Knowledge Proofs"
  type        = string
  default     = "0"
}

variable "zk_max_concurrent_proofs" {
  description = "Max concurrent ZK proofs"
  type        = number
  default     = 4
}

variable "zk_circuit_path" {
  description = "ZK circuit path"
  type        = string
  default     = "/opt/mandala/zk/circuits/"
}

variable "zk_proving_key" {
  description = "ZK proving key path"
  type        = string
  default     = ""
}

variable "zk_verification_key" {
  description = "ZK verification key path"
  type        = string
  default     = ""
}

variable "zk_remote_verifier_endpoint" {
  description = "ZK remote verifier endpoint"
  type        = string
  default     = ""
}

variable "event_time_determinism_enabled" {
  description = "Enable event-time determinism"
  type        = string
  default     = "0"
}

variable "geometric_hash_provider" {
  description = "Geometric hash provider (h3, s2, or none)"
  type        = string
  default     = "none"
}

variable "geometric_hash_resolution" {
  description = "Geometric hash resolution"
  type        = number
  default     = 9
}

variable "stator_latch_enabled" {
  description = "Enable Stator's Latch"
  type        = string
  default     = "0"
}

variable "stator_latch_ttl_seconds" {
  description = "Stator's Latch TTL in seconds"
  type        = number
  default     = 1209600
}

variable "stator_latch_tolerance_seconds" {
  description = "Stator's Latch tolerance in seconds"
  type        = number
  default     = 1
}

variable "reorder_buffer_enabled" {
  description = "Enable re-ordering buffer"
  type        = string
  default     = "0"
}

variable "reorder_buffer_max_events_per_entity" {
  description = "Max events per entity in reorder buffer"
  type        = number
  default     = 100
}

variable "reorder_buffer_max_wait_seconds" {
  description = "Max wait time for reorder buffer in seconds"
  type        = number
  default     = 300
}

variable "reorder_buffer_expire_seconds" {
  description = "Expire time for reorder buffer in seconds"
  type        = number
  default     = 3600
}

variable "spatial_coherence_enabled" {
  description = "Enable spatial coherence checks"
  type        = string
  default     = "0"
}

variable "max_velocity_mps" {
  description = "Maximum velocity in meters per second"
  type        = number
  default     = 150.0
}

variable "stream_batch_size" {
  description = "Events per XREADGROUP batch"
  type        = number
  default     = 10
}

variable "stream_block_ms" {
  description = "Block time in milliseconds"
  type        = number
  default     = 5000
}

variable "max_concurrent_events" {
  description = "Max events processed concurrently per worker"
  type        = number
  default     = 50
}

variable "stream_maxlen" {
  description = "Max messages per Redis Stream"
  type        = number
  default     = 100000
}

variable "backpressure_enabled" {
  description = "Enable backpressure"
  type        = string
  default     = "1"
}

variable "backpressure_threshold" {
  description = "Stream length threshold for backpressure"
  type        = number
  default     = 80000
}

variable "backpressure_response_code" {
  description = "HTTP status code when backpressure active"
  type        = number
  default     = 503
}

variable "rate_limit_enabled" {
  description = "Enable rate limiting"
  type        = string
  default     = "1"
}

variable "rate_limit_requests_per_minute" {
  description = "Max requests per minute per IP"
  type        = number
  default     = 1000
}

variable "rate_limit_burst_size" {
  description = "Burst size for token bucket"
  type        = number
  default     = 100
}

variable "log_level" {
  description = "Log level"
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}
