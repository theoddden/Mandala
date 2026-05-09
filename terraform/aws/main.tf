terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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

resource "aws_elasticache_replication_group" "mandala" {
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
          value = "redis://${aws_elasticache_replication_group.mandala.primary_endpoint_address}:6379/0"
        },
        {
          name  = "MANDALA_SAMSARA_WEBHOOK_SECRET"
          value = "{{resolve:secretsmanager:${aws_secretsmanager_secret.samsara.name}:SecretString}}"
        }
      ]
      secrets = var.vizion_api_key != "" ? [
        {
          name      = "MANDALA_VIZION_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.vizion[0].arn}:SecretString"
        }
      ] : []
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
          value = "redis://${aws_elasticache_replication_group.mandala.primary_endpoint_address}:6379/0"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mandala.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "worker"
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
        Resource = aws_elasticache_replication_group.mandala.arn
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = concat(
          [aws_secretsmanager_secret.samsara.arn],
          var.vizion_api_key != "" ? [aws_secretsmanager_secret.vizion[0].arn] : []
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
  value       = aws_elasticache_replication_group.mandala.primary_endpoint_address
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.mandala.name
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.mandala.name
}
