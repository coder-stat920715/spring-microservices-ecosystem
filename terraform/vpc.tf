data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3) # AZ-a, AZ-b, AZ-c
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${var.name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  public_subnets   = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 8, i)]      # IGW, NAT, external ALB
  private_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 8, i + 10)] # EKS workers, no inbound internet
  database_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 8, i + 20)] # PostgreSQL, Redis, Kafka, MongoDB

  enable_nat_gateway     = true
  single_nat_gateway     = false
  one_nat_gateway_per_az = true # one NAT per AZ: an AZ outage does not break egress elsewhere
  enable_dns_hostnames   = true
  enable_dns_support     = true

  create_database_subnet_group       = true
  create_database_subnet_route_table = true # DB route table has no route to the internet or NAT

  enable_flow_log                                 = true
  create_flow_log_cloudwatch_log_group            = true
  create_flow_log_cloudwatch_iam_role             = true
  flow_log_max_aggregation_interval               = 60
  flow_log_cloudwatch_log_group_retention_in_days = 90

  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
}
