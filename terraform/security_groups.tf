data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

# Layer 1: external ALB. Reachable only from CloudFront edge IPs.
resource "aws_security_group" "external_alb" {
  name_prefix = "${var.name}-external-alb-"
  description = "External ALB: HTTPS from CloudFront only"
  vpc_id      = module.vpc.vpc_id
  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_security_group_ingress_rule" "alb_https_from_cloudfront" {
  security_group_id = aws_security_group.external_alb.id
  prefix_list_id    = data.aws_ec2_managed_prefix_list.cloudfront.id
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "alb_to_vpc" {
  security_group_id = aws_security_group.external_alb.id
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "-1"
}

# ALB -> gateway pods (8080) and ALB health checks (9090)
resource "aws_vpc_security_group_ingress_rule" "nodes_from_alb" {
  for_each                     = toset(["8080", "9090"])
  security_group_id            = module.eks.node_security_group_id
  referenced_security_group_id = aws_security_group.external_alb.id
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.value)
  to_port                      = tonumber(each.value)
}

# Data tier: only EKS worker nodes may connect.
resource "aws_security_group" "data" {
  name_prefix = "${var.name}-data-"
  description = "PostgreSQL, Redis, Kafka, MongoDB"
  vpc_id      = module.vpc.vpc_id
  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_security_group_ingress_rule" "data_from_nodes" {
  for_each = {
    postgres = 5432
    redis    = 6379
    kafka    = 9094
    mongodb  = 27017
  }
  security_group_id            = aws_security_group.data.id
  referenced_security_group_id = module.eks.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  description                  = each.key
}
