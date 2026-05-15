terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
      configuration_aliases = [aws.primary, aws.replica]
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# -----------------------------------------------------------------------------
# AWS ElastiCache Redis
# -----------------------------------------------------------------------------
resource "aws_elasticache_subnet_group" "mandala" {
  name       = "${var.name}-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name        = "${var.name}-redis"
    Environment = var.environment
  }
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  description = "Security group for Mandala Redis"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = var.vpc_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.name}-redis"
    Environment = var.environment
  }
}

# Redis HA with Sentinel (optional)
resource "aws_elasticache_replication_group" "mandala_ha" {
  count = var.redis_ha_enabled ? 1 : 0
  
  replication_group_id          = "${var.name}-redis-ha"
  replication_group_description = "Mandala Redis cluster with HA"
  node_type                      = var.redis_node_type
  number_cache_clusters          = var.redis_num_nodes
  engine                        = "redis"
  engine_version                = "7.0"
  parameter_group_name           = "default.redis7"
  subnet_group_name             = aws_elasticache_subnet_group.mandala.name
  security_group_ids            = [aws_security_group.redis.id]
  automatic_failover_enabled    = true
  multi_az_enabled              = true
  at_rest_encryption_enabled    = true
  transit_encryption_enabled    = true
  
  tags = {
    Name        = "${var.name}-redis-ha"
    Environment = var.environment
  }
}

resource "aws_elasticache_replication_group" "mandala" {
  count = var.redis_ha_enabled ? 0 : 1
  
  replication_group_id          = "${var.name}-redis"
  replication_group_description = "Mandala Redis cluster"
  node_type                      = var.redis_node_type
  number_cache_clusters          = var.redis_num_nodes
  engine                        = "redis"
  engine_version                = "7.0"
  parameter_group_name           = "default.redis7"
  subnet_group_name             = aws_elasticache_subnet_group.mandala.name
  security_group_ids            = [aws_security_group.redis.id]
  automatic_failover_enabled    = var.redis_auto_failover
  multi_az_enabled              = var.redis_multi_az

  tags = {
    Name        = "${var.name}-redis"
    Environment = var.environment
  }
}

# -----------------------------------------------------------------------------
# AWS Secrets Manager
# -----------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "samsara" {
  name = "${var.name}/samsara-webhook-secret"
}

resource "aws_secretsmanager_secret_version" "samsara" {
  secret_id     = aws_secretsmanager_secret.samsara.id
  secret_string = var.samsara_webhook_secret
}

resource "aws_secretsmanager_secret" "vizion" {
  count = var.vizion_api_key != "" ? 1 : 0
  name  = "${var.name}/vizion-api-key"
}

resource "aws_secretsmanager_secret_version" "vizion" {
  count      = var.vizion_api_key != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.vizion[0].id
  secret_string = var.vizion_api_key
}

resource "aws_secretsmanager_secret" "samsara_api_token" {
  count = var.samsara_api_token != "" ? 1 : 0
  name  = "${var.name}/samsara-api-token"
}

resource "aws_secretsmanager_secret_version" "samsara_api_token" {
  count      = var.samsara_api_token != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.samsara_api_token[0].id
  secret_string = var.samsara_api_token
}

resource "aws_secretsmanager_secret" "descartes" {
  count = var.descartes_webhook_secret != "" ? 1 : 0
  name  = "${var.name}/descartes-webhook-secret"
}

resource "aws_secretsmanager_secret_version" "descartes" {
  count      = var.descartes_webhook_secret != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.descartes[0].id
  secret_string = var.descartes_webhook_secret
}

resource "aws_secretsmanager_secret" "descartes_api_key" {
  count = var.descartes_api_key != "" ? 1 : 0
  name  = "${var.name}/descartes-api-key"
}

resource "aws_secretsmanager_secret_version" "descartes_api_key" {
  count      = var.descartes_api_key != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.descartes_api_key[0].id
  secret_string = var.descartes_api_key
}

resource "aws_secretsmanager_secret" "cargowise" {
  count = var.cargowise_webhook_secret != "" ? 1 : 0
  name  = "${var.name}/cargowise-webhook-secret"
}

resource "aws_secretsmanager_secret_version" "cargowise" {
  count      = var.cargowise_webhook_secret != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.cargowise[0].id
  secret_string = var.cargowise_webhook_secret
}

resource "aws_secretsmanager_secret" "cargowise_credentials" {
  count = var.cargowise_webhook_secret != "" ? 1 : 0
  name  = "${var.name}/cargowise-credentials"
}

resource "aws_secretsmanager_secret_version" "cargowise_credentials" {
  count      = var.cargowise_webhook_secret != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.cargowise_credentials[0].id
  secret_string = jsonencode({
    eadaptor_url = var.cargowise_eadaptor_url
    username     = var.cargowise_username
    password     = var.cargowise_password
    organization_code = var.cargowise_organization_code
  })
}

resource "aws_secretsmanager_secret" "dat" {
  count = var.dat_client_id != "" ? 1 : 0
  name  = "${var.name}/dat-credentials"
}

resource "aws_secretsmanager_secret_version" "dat" {
  count      = var.dat_client_id != "" ? 1 : 0
  secret_id  = aws_secretsmanager_secret.dat[0].id
  secret_string = jsonencode({
    client_id     = var.dat_client_id
    client_secret = var.dat_client_secret
  })
}

# -----------------------------------------------------------------------------
# ECS Task Definition
# -----------------------------------------------------------------------------
resource "aws_ecs_task_definition" "mandala" {
  family                   = "${var.name}-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "mandala-serve"
      image     = "${var.container_image}:${var.container_tag}"
      essential = true
      command   = ["mandala", "serve"]
      portMappings = [
        {
          containerPort = 8000
          protocol      = "tcp"
        }
      ]
      environment = [
        {
          name  = "MANDALA_REDIS_URL"
          value = "redis://${var.redis_ha_enabled ? aws_elasticache_replication_group.mandala_ha[0].primary_endpoint_address : aws_elasticache_replication_group.mandala[0].primary_endpoint_address}:6379/0"
        },
        {
          name  = "MANDALA_SAMSARA_WEBHOOK_SECRET"
          value = "{{resolve:secretsmanager:${aws_secretsmanager_secret.samsara.name}:SecretString}}"
        },
        {
          name  = "MANDALA_SAMSARA_BASE_URL"
          value = var.samsara_base_url
        },
        {
          name  = "MANDALA_SAMSARA_OUTBOUND_ENABLED"
          value = var.samsara_outbound_enabled
        },
        {
          name  = "MANDALA_DESCARTES_BASE_URL"
          value = var.descartes_base_url
        },
        {
          name  = "MANDALA_CARGOWISE_EADAPTOR_URL"
          value = var.cargowise_eadaptor_url
        },
        {
          name  = "MANDALA_LOADBOARD_ENABLED"
          value = var.loadboard_enabled
        },
        {
          name  = "MANDALA_LOADBOARD_POST_DEFAULT_RADIUS_MI"
          value = var.loadboard_post_default_radius_mi
        },
        {
          name  = "MANDALA_LOADBOARD_POST_TTL_HOURS"
          value = var.loadboard_post_ttl_hours
        },
        {
          name  = "MANDALA_OTLP_ENDPOINT"
          value = var.otlp_endpoint
        },
        {
          name  = "MANDALA_EVENT_LOG_ENABLED"
          value = var.event_log_enabled
        },
        {
          name  = "MANDALA_ICEBERG_CATALOG"
          value = var.iceberg_catalog
        },
        {
          name  = "MANDALA_ICEBERG_CATALOG_URI"
          value = var.iceberg_catalog_uri
        },
        {
          name  = "MANDALA_ICEBERG_WAREHOUSE"
          value = var.iceberg_warehouse
        },
        {
          name  = "MANDALA_ICEBERG_TABLE"
          value = var.iceberg_table
        },
        {
          name  = "MANDALA_ICEBERG_NAMESPACE"
          value = var.iceberg_namespace
        },
        {
          name  = "MANDALA_ZK_ENABLED"
          value = var.zk_enabled
        },
        {
          name  = "MANDALA_ZK_MAX_CONCURRENT_PROOFS"
          value = var.zk_max_concurrent_proofs
        },
        {
          name  = "MANDALA_ZK_CIRCUIT_PATH"
          value = var.zk_circuit_path
        },
        {
          name  = "MANDALA_ZK_PROVING_KEY"
          value = var.zk_proving_key
        },
        {
          name  = "MANDALA_ZK_VERIFICATION_KEY"
          value = var.zk_verification_key
        },
        {
          name  = "MANDALA_ZK_REMOTE_VERIFIER_ENDPOINT"
          value = var.zk_remote_verifier_endpoint
        },
        {
          name  = "MANDALA_EVENT_TIME_DETERMINISM_ENABLED"
          value = var.event_time_determinism_enabled
        },
        {
          name  = "MANDALA_GEOMETRIC_HASH_PROVIDER"
          value = var.geometric_hash_provider
        },
        {
          name  = "MANDALA_GEOMETRIC_HASH_RESOLUTION"
          value = var.geometric_hash_resolution
        },
        {
          name  = "MANDALA_STATOR_LATCH_ENABLED"
          value = var.stator_latch_enabled
        },
        {
          name  = "MANDALA_STATOR_LATCH_TTL_SECONDS"
          value = var.stator_latch_ttl_seconds
        },
        {
          name  = "MANDALA_STATOR_LATCH_TOLERANCE_SECONDS"
          value = var.stator_latch_tolerance_seconds
        },
        {
          name  = "MANDALA_REORDER_BUFFER_ENABLED"
          value = var.reorder_buffer_enabled
        },
        {
          name  = "MANDALA_REORDER_BUFFER_MAX_EVENTS_PER_ENTITY"
          value = var.reorder_buffer_max_events_per_entity
        },
        {
          name  = "MANDALA_REORDER_BUFFER_MAX_WAIT_SECONDS"
          value = var.reorder_buffer_max_wait_seconds
        },
        {
          name  = "MANDALA_REORDER_BUFFER_EXPIRE_SECONDS"
          value = var.reorder_buffer_expire_seconds
        },
        {
          name  = "MANDALA_SPATIAL_COHERENCE_ENABLED"
          value = var.spatial_coherence_enabled
        },
        {
          name  = "MANDALA_MAX_VELOCITY_MPS"
          value = var.max_velocity_mps
        },
        {
          name  = "MANDALA_STREAM_BATCH_SIZE"
          value = var.stream_batch_size
        },
        {
          name  = "MANDALA_STREAM_BLOCK_MS"
          value = var.stream_block_ms
        },
        {
          name  = "MANDALA_MAX_CONCURRENT_EVENTS"
          value = var.max_concurrent_events
        },
        {
          name  = "MANDALA_STREAM_MAXLEN"
          value = var.stream_maxlen
        },
        {
          name  = "MANDALA_BACKPRESSURE_ENABLED"
          value = var.backpressure_enabled
        },
        {
          name  = "MANDALA_BACKPRESSURE_THRESHOLD"
          value = var.backpressure_threshold
        },
        {
          name  = "MANDALA_BACKPRESSURE_RESPONSE_CODE"
          value = var.backpressure_response_code
        },
        {
          name  = "MANDALA_RATE_LIMIT_ENABLED"
          value = var.rate_limit_enabled
        },
        {
          name  = "MANDALA_RATE_LIMIT_REQUESTS_PER_MINUTE"
          value = var.rate_limit_requests_per_minute
        },
        {
          name  = "MANDALA_RATE_LIMIT_BURST_SIZE"
          value = var.rate_limit_burst_size
        },
        {
          name  = "MANDALA_LOG_LEVEL"
          value = var.log_level
        }
      ]
      secrets = concat(
        var.vizion_api_key != "" ? [
          {
            name      = "MANDALA_VIZION_API_KEY"
            valueFrom = "${aws_secretsmanager_secret.vizion[0].arn}:SecretString"
          }
        ] : [],
        var.samsara_api_token != "" ? [
          {
            name      = "MANDALA_SAMSARA_API_TOKEN"
            valueFrom = "${aws_secretsmanager_secret.samsara_api_token.arn}:SecretString"
          }
        ] : [],
        var.descartes_webhook_secret != "" ? [
          {
            name      = "MANDALA_DESCARTES_WEBHOOK_SECRET"
            valueFrom = "${aws_secretsmanager_secret.descartes[0].arn}:SecretString"
          },
          {
            name      = "MANDALA_DESCARTES_API_KEY"
            valueFrom = "${aws_secretsmanager_secret.descartes_api_key[0].arn}:SecretString"
          }
        ] : [],
        var.cargowise_webhook_secret != "" ? [
          {
            name      = "MANDALA_CARGOWISE_WEBHOOK_SECRET"
            valueFrom = "${aws_secretsmanager_secret.cargowise[0].arn}:SecretString"
          },
          {
            name      = "MANDALA_CARGOWISE_CREDENTIALS"
            valueFrom = "${aws_secretsmanager_secret.cargowise_credentials[0].arn}:SecretString"
          }
        ] : [],
        var.dat_client_id != "" ? [
          {
            name      = "MANDALA_DAT_CREDENTIALS"
            valueFrom = "${aws_secretsmanager_secret.dat[0].arn}:SecretString"
          }
        ] : []
      )
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mandala.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "serve"
        }
      }
    },
    {
      name      = "mandala-worker"
      image     = "${var.container_image}:${var.container_tag}"
      essential = true
      command   = ["mandala", "worker"]
      environment = [
        {
          name  = "MANDALA_REDIS_URL"
          value = "redis://${var.redis_ha_enabled ? aws_elasticache_replication_group.mandala_ha[0].primary_endpoint_address : aws_elasticache_replication_group.mandala[0].primary_endpoint_address}:6379/0"
        },
        {
          name  = "MANDALA_SAMSARA_BASE_URL"
          value = var.samsara_base_url
        },
        {
          name  = "MANDALA_SAMSARA_OUTBOUND_ENABLED"
          value = var.samsara_outbound_enabled
        },
        {
          name  = "MANDALA_DESCARTES_BASE_URL"
          value = var.descartes_base_url
        },
        {
          name  = "MANDALA_LOADBOARD_ENABLED"
          value = var.loadboard_enabled
        },
        {
          name  = "MANDALA_LOADBOARD_POST_DEFAULT_RADIUS_MI"
          value = var.loadboard_post_default_radius_mi
        },
        {
          name  = "MANDALA_LOADBOARD_POST_TTL_HOURS"
          value = var.loadboard_post_ttl_hours
        },
        {
          name  = "MANDALA_OTLP_ENDPOINT"
          value = var.otlp_endpoint
        },
        {
          name  = "MANDALA_EVENT_LOG_ENABLED"
          value = var.event_log_enabled
        },
        {
          name  = "MANDALA_ICEBERG_CATALOG"
          value = var.iceberg_catalog
        },
        {
          name  = "MANDALA_ICEBERG_CATALOG_URI"
          value = var.iceberg_catalog_uri
        },
        {
          name  = "MANDALA_ICEBERG_WAREHOUSE"
          value = var.iceberg_warehouse
        },
        {
          name  = "MANDALA_ICEBERG_TABLE"
          value = var.iceberg_table
        },
        {
          name  = "MANDALA_ICEBERG_NAMESPACE"
          value = var.iceberg_namespace
        },
        {
          name  = "MANDALA_ZK_ENABLED"
          value = var.zk_enabled
        },
        {
          name  = "MANDALA_ZK_MAX_CONCURRENT_PROOFS"
          value = var.zk_max_concurrent_proofs
        },
        {
          name  = "MANDALA_ZK_CIRCUIT_PATH"
          value = var.zk_circuit_path
        },
        {
          name  = "MANDALA_ZK_PROVING_KEY"
          value = var.zk_proving_key
        },
        {
          name  = "MANDALA_ZK_VERIFICATION_KEY"
          value = var.zk_verification_key
        },
        {
          name  = "MANDALA_ZK_REMOTE_VERIFIER_ENDPOINT"
          value = var.zk_remote_verifier_endpoint
        },
        {
          name  = "MANDALA_EVENT_TIME_DETERMINISM_ENABLED"
          value = var.event_time_determinism_enabled
        },
        {
          name  = "MANDALA_GEOMETRIC_HASH_PROVIDER"
          value = var.geometric_hash_provider
        },
        {
          name  = "MANDALA_GEOMETRIC_HASH_RESOLUTION"
          value = var.geometric_hash_resolution
        },
        {
          name  = "MANDALA_STATOR_LATCH_ENABLED"
          value = var.stator_latch_enabled
        },
        {
          name  = "MANDALA_STATOR_LATCH_TTL_SECONDS"
          value = var.stator_latch_ttl_seconds
        },
        {
          name  = "MANDALA_STATOR_LATCH_TOLERANCE_SECONDS"
          value = var.stator_latch_tolerance_seconds
        },
        {
          name  = "MANDALA_REORDER_BUFFER_ENABLED"
          value = var.reorder_buffer_enabled
        },
        {
          name  = "MANDALA_REORDER_BUFFER_MAX_EVENTS_PER_ENTITY"
          value = var.reorder_buffer_max_events_per_entity
        },
        {
          name  = "MANDALA_REORDER_BUFFER_MAX_WAIT_SECONDS"
          value = var.reorder_buffer_max_wait_seconds
        },
        {
          name  = "MANDALA_REORDER_BUFFER_EXPIRE_SECONDS"
          value = var.reorder_buffer_expire_seconds
        },
        {
          name  = "MANDALA_SPATIAL_COHERENCE_ENABLED"
          value = var.spatial_coherence_enabled
        },
        {
          name  = "MANDALA_MAX_VELOCITY_MPS"
          value = var.max_velocity_mps
        },
        {
          name  = "MANDALA_STREAM_BATCH_SIZE"
          value = var.stream_batch_size
        },
        {
          name  = "MANDALA_STREAM_BLOCK_MS"
          value = var.stream_block_ms
        },
        {
          name  = "MANDALA_MAX_CONCURRENT_EVENTS"
          value = var.max_concurrent_events
        },
        {
          name  = "MANDALA_STREAM_MAXLEN"
          value = var.stream_maxlen
        },
        {
          name  = "MANDALA_BACKPRESSURE_ENABLED"
          value = var.backpressure_enabled
        },
        {
          name  = "MANDALA_BACKPRESSURE_THRESHOLD"
          value = var.backpressure_threshold
        },
        {
          name  = "MANDALA_BACKPRESSURE_RESPONSE_CODE"
          value = var.backpressure_response_code
        },
        {
          name  = "MANDALA_LOG_LEVEL"
          value = var.log_level
        }
      ]
      secrets = concat(
        var.samsara_api_token != "" ? [
          {
            name      = "MANDALA_SAMSARA_API_TOKEN"
            valueFrom = "${aws_secretsmanager_secret.samsara_api_token[0].arn}:SecretString"
          }
        ] : [],
        var.vizion_api_key != "" ? [
          {
            name      = "MANDALA_VIZION_API_KEY"
            valueFrom = "${aws_secretsmanager_secret.vizion[0].arn}:SecretString"
          }
        ] : [],
        var.descartes_webhook_secret != "" ? [
          {
            name      = "MANDALA_DESCARTES_API_KEY"
            valueFrom = "${aws_secretsmanager_secret.descartes_api_key[0].arn}:SecretString"
          }
        ] : [],
        var.cargowise_webhook_secret != "" ? [
          {
            name      = "MANDALA_CARGOWISE_CREDENTIALS"
            valueFrom = "${aws_secretsmanager_secret.cargowise_credentials[0].arn}:SecretString"
          }
        ] : [],
        var.dat_client_id != "" ? [
          {
            name      = "MANDALA_DAT_CREDENTIALS"
            valueFrom = "${aws_secretsmanager_secret.dat[0].arn}:SecretString"
          }
        ] : []
      )
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mandala.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "worker"
        }
      }
    },
    {
      name      = "mandala-views"
      image     = "${var.container_image}:${var.container_tag}"
      essential = false
      command   = ["mandala", "views"]
      environment = [
        {
          name  = "MANDALA_REDIS_URL"
          value = "redis://${var.redis_ha_enabled ? aws_elasticache_replication_group.mandala_ha[0].primary_endpoint_address : aws_elasticache_replication_group.mandala[0].primary_endpoint_address}:6379/0"
        },
        {
          name  = "MANDALA_LOG_LEVEL"
          value = var.log_level
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mandala.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "views"
        }
      }
    }
  ])

  tags = {
    Name        = "${var.name}-task"
    Environment = var.environment
  }
}

# -----------------------------------------------------------------------------
# ECS Service
# -----------------------------------------------------------------------------
resource "aws_ecs_cluster" "mandala" {
  name = var.name
}

resource "aws_ecs_service" "mandala" {
  name            = "${var.name}-service"
  cluster         = aws_ecs_cluster.mandala.id
  task_definition = aws_ecs_task_definition.mandala.arn
  desired_count   = var.service_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  tags = {
    Name        = "${var.name}-service"
    Environment = var.environment
  }
}

# -----------------------------------------------------------------------------
# Application Load Balancer
# -----------------------------------------------------------------------------
resource "aws_lb" "mandala" {
  name               = "${var.name}-alb"
  internal           = var.alb_internal
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets           = var.public_subnet_ids

  enable_deletion_protection = false

  tags = {
    Name        = "${var.name}-alb"
    Environment = var.environment
  }
}

resource "aws_lb_target_group" "mandala" {
  name        = "${var.name}-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/healthz"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = {
    Name        = "${var.name}-tg"
    Environment = var.environment
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.mandala.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.mandala.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.mandala.arn
  }
}

# -----------------------------------------------------------------------------
# Security Groups
# -----------------------------------------------------------------------------
resource "aws_security_group" "ecs" {
  name_prefix = "${var.name}-ecs-"
  description = "Security group for Mandala ECS tasks"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.name}-ecs"
    Environment = var.environment
  }
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  description = "Security group for Mandala ALB"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.alb_allowed_cidrs
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.alb_allowed_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.name}-alb"
    Environment = var.environment
  }
}

# -----------------------------------------------------------------------------
# IAM Roles
# -----------------------------------------------------------------------------
resource "aws_iam_role" "task_execution" {
  name = "${var.name}-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.name}-task-execution-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name = "${var.name}-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.name}-task-role"
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "task" {
  name = "${var.name}-task-policy"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticache:Connect"
        ]
        Resource = var.redis_ha_enabled ? aws_elasticache_replication_group.mandala_ha[0].arn : aws_elasticache_replication_group.mandala[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = concat(
          [aws_secretsmanager_secret.samsara.arn],
          var.vizion_api_key != "" ? [aws_secretsmanager_secret.vizion[0].arn] : [],
          var.samsara_api_token != "" ? [aws_secretsmanager_secret.samsara_api_token[0].arn] : [],
          var.descartes_webhook_secret != "" ? [aws_secretsmanager_secret.descartes[0].arn, aws_secretsmanager_secret.descartes_api_key[0].arn, aws_secretsmanager_secret.cargowise[0].arn, aws_secretsmanager_secret.cargowise_credentials[0].arn] : [],
          var.dat_client_id != "" ? [aws_secretsmanager_secret.dat[0].arn] : []
        )
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.mandala.arn}:*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# CloudWatch Logs
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "mandala" {
  name              = "/ecs/${var.name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.name}-logs"
    Environment = var.environment
  }
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------
output "alb_dns_name" {
  description = "DNS name of the Application Load Balancer"
  value       = aws_lb.mandala.dns_name
}

output "redis_endpoint" {
  description = "Redis cluster endpoint"
  value       = var.redis_ha_enabled ? aws_elasticache_replication_group.mandala_ha[0].primary_endpoint_address : aws_elasticache_replication_group.mandala[0].primary_endpoint_address
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.mandala.name
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.mandala.name
}

output "task_definition_arn" {
  description = "ECS task definition ARN"
  value       = aws_ecs_task_definition.mandala.arn
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.mandala.name
}
