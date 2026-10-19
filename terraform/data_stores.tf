# ============================== Data tier (private database subnets, Multi-AZ) ===============================

# ---- PostgreSQL (write model + Debezium WAL source) ----
resource "aws_db_parameter_group" "postgres" {
  name_prefix = "${var.name}-pg16-"
  family      = "postgres16"
  parameter {
    name         = "rds.logical_replication" # required for Debezium (pgoutput)
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }
  lifecycle { create_before_destroy = true }
}

resource "aws_db_instance" "postgres" {
  identifier                          = "${var.name}-orders"
  engine                              = "postgres"
  engine_version                      = "16.4"
  instance_class                      = "db.r6g.large"
  allocated_storage                   = 100
  max_allocated_storage               = 500
  storage_type                        = "gp3"
  storage_encrypted                   = true
  db_name                             = "orders"
  username                            = "orders_admin"
  manage_master_user_password         = true # stored in Secrets Manager
  multi_az                            = true
  db_subnet_group_name                = module.vpc.database_subnet_group_name
  vpc_security_group_ids              = [aws_security_group.data.id]
  parameter_group_name                = aws_db_parameter_group.postgres.name
  backup_retention_period             = 7
  deletion_protection                 = true
  performance_insights_enabled        = true
  iam_database_authentication_enabled = true
  final_snapshot_identifier           = "${var.name}-orders-final"
}

# ---- Redis (rate limiter, L2 cache, token deny-list) ----
resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.name}-redis"
  subnet_ids = module.vpc.database_subnets
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${var.name}-redis"
  description                = "rate limiter + L2 cache"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = "cache.r7g.large"
  num_cache_clusters         = 3 # one per AZ
  automatic_failover_enabled = true
  multi_az_enabled           = true
  subnet_group_name          = aws_elasticache_subnet_group.redis.name
  security_group_ids         = [aws_security_group.data.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = var.redis_auth_token
}

# ---- Kafka (MSK, 3 brokers across 3 AZs) ----
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "${var.name}-kafka"
  kafka_version          = "3.7.x"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = module.vpc.database_subnets
    security_groups = [aws_security_group.data.id]
    storage_info {
      ebs_storage_info {
        volume_size = 200
      }
    }
  }
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
  # TLS in transit + security-group isolation. For per-client identity switch to SASL/IAM or TLS client
  # auth (requires aws-msk-iam-auth on the classpath / an ACM Private CA).
  client_authentication {
    unauthenticated = true
  }
}

# ---- MongoDB-compatible read store (Amazon DocumentDB) ----
resource "aws_docdb_subnet_group" "docdb" {
  name       = "${var.name}-docdb"
  subnet_ids = module.vpc.database_subnets
}

resource "aws_docdb_cluster" "read_model" {
  cluster_identifier          = "${var.name}-read-model"
  engine                      = "docdb"
  master_username             = "readmodel_admin"
  manage_master_user_password = true
  db_subnet_group_name        = aws_docdb_subnet_group.docdb.name
  vpc_security_group_ids      = [aws_security_group.data.id]
  storage_encrypted           = true
  backup_retention_period     = 7
  deletion_protection         = true
  final_snapshot_identifier   = "${var.name}-read-model-final"
}

resource "aws_docdb_cluster_instance" "read_model" {
  count              = 3
  identifier         = "${var.name}-read-model-${count.index}"
  cluster_identifier = aws_docdb_cluster.read_model.id
  instance_class     = "db.r6g.large"
}
