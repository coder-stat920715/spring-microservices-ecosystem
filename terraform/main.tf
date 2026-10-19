terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
  # backend "s3" { ... }   # configure remote state + DynamoDB locking for real use
}

provider "aws" {
  region = var.aws_region
}

# CloudFront-scoped WAF, ACM certs and Shield protections for CloudFront live in us-east-1.
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

# ============================== EKS (workers in private app subnets, 3 AZs) ===============================
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = "${var.name}-eks"
  cluster_version = "1.30"
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnets

  cluster_endpoint_public_access           = var.eks_public_endpoint
  cluster_endpoint_private_access          = true
  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true
  cluster_enabled_log_types                = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true }
    eks-pod-identity-agent = { most_recent = true }
  }

  eks_managed_node_groups = {
    apps = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = ["m6i.xlarge"]
      min_size       = 3 # at least one node per AZ
      max_size       = 12
      desired_size   = 3
      subnet_ids     = module.vpc.private_subnets
      labels         = { workload = "microservices" }
    }
  }
}

module "lb_controller_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name                              = "${var.name}-aws-lb-controller"
  attach_load_balancer_controller_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }
}

module "adot_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name = "${var.name}-adot-collector"
  role_policy_arns = {
    xray       = "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
    cloudwatch = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  }
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["observability:adot-collector"]
    }
  }
}

# ============================== AWS WAF (managed rule groups + rate limiting) ===============================
locals {
  managed_rule_groups = [
    { name = "AWSManagedRulesCommonRuleSet", priority = 10 },          # includes XSS rules
    { name = "AWSManagedRulesSQLiRuleSet", priority = 20 },
    { name = "AWSManagedRulesKnownBadInputsRuleSet", priority = 30 },
    { name = "AWSManagedRulesAmazonIpReputationList", priority = 40 }, # IP reputation
    { name = "AWSManagedRulesAnonymousIpList", priority = 50 },
  ]
}

resource "aws_wafv2_web_acl" "cloudfront" {
  provider = aws.us_east_1
  name  = "${var.name}-cloudfront"
  scope = "CLOUDFRONT"

  default_action {
    allow {}
  }

  rule {
    name     = "ip-rate-limit"
    priority = 1
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = var.waf_rate_limit
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "cloudfront-ip-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  dynamic "rule" {
    for_each = local.managed_rule_groups
    content {
      name     = rule.value.name
      priority = rule.value.priority
      override_action {
        none {}
      }
      statement {
        managed_rule_group_statement {
          name        = rule.value.name
          vendor_name = "AWS"
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = "cloudfront-${rule.value.name}"
        sampled_requests_enabled   = true
      }
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "cloudfront"
    sampled_requests_enabled   = true
  }
}

resource "aws_wafv2_web_acl" "regional" {
  name  = "${var.name}-regional"
  scope = "REGIONAL"

  default_action {
    allow {}
  }

  rule {
    name     = "ip-rate-limit"
    priority = 1
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = var.waf_rate_limit
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "regional-ip-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  dynamic "rule" {
    for_each = local.managed_rule_groups
    content {
      name     = rule.value.name
      priority = rule.value.priority
      override_action {
        none {}
      }
      statement {
        managed_rule_group_statement {
          name        = rule.value.name
          vendor_name = "AWS"
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = "regional-${rule.value.name}"
        sampled_requests_enabled   = true
      }
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "regional"
    sampled_requests_enabled   = true
  }
}

# ============================== CloudFront (edge CDN + TLS termination) ===============================
data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}
data "aws_cloudfront_cache_policy" "optimized" {
  name = "Managed-CachingOptimized"
}

resource "aws_cloudfront_origin_request_policy" "api" {
  name = "${var.name}-api-origin"
  cookies_config {
    cookie_behavior = "none"
  }
  headers_config {
    header_behavior = "allViewerAndWhitelistCloudFront"
    headers {
      items = ["CloudFront-Viewer-Country"]
    }
  }
  query_strings_config {
    query_string_behavior = "all"
  }
}

resource "aws_cloudfront_distribution" "main" {
  enabled         = true
  is_ipv6_enabled = true
  http_version    = "http2and3"
  aliases         = [var.app_domain]
  web_acl_id      = aws_wafv2_web_acl.cloudfront.arn
  price_class     = "PriceClass_All"

  origin {
    domain_name = var.origin_domain
    origin_id   = "external-alb"
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
    custom_header {
      name  = "X-Origin-Verify"
      value = var.origin_verify_secret
    }
  }

  default_cache_behavior {
    target_origin_id         = "external-alb"
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = data.aws_cloudfront_cache_policy.disabled.id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.api.id
    compress                 = true
  }

  ordered_cache_behavior {
    path_pattern           = "/static/*"
    target_origin_id       = "external-alb"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.optimized.id
    compress               = true
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = var.cloudfront_certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

# ============================== Shield Advanced (needs an active subscription) ===============================
resource "aws_shield_protection" "cloudfront" {
  provider     = aws.us_east_1
  count        = var.enable_shield_advanced ? 1 : 0
  name         = "${var.name}-cloudfront"
  resource_arn = aws_cloudfront_distribution.main.arn
}

# The external ALB itself is also protected through the Ingress annotation
# alb.ingress.kubernetes.io/shield-advanced-protection: "true".

# ============================== Route 53 ===============================
data "aws_route53_zone" "this" {
  name         = var.zone_name
  private_zone = false
}

resource "aws_route53_record" "app" {
  for_each = toset(["A", "AAAA"])
  zone_id  = data.aws_route53_zone.this.zone_id
  name     = var.app_domain
  type     = each.value
  alias {
    name                   = aws_cloudfront_distribution.main.domain_name
    zone_id                = aws_cloudfront_distribution.main.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_health_check" "origin" {
  fqdn              = var.origin_domain
  port              = 443
  type              = "HTTPS"
  resource_path     = "/health"
  failure_threshold = 3
  request_interval  = 10
  measure_latency   = true
  tags              = { Name = "${var.name}-origin" }
}

# Latency-based routing with health-check failover between regional ALBs (set primary_alb_dns after the Ingress exists).
resource "aws_route53_record" "origin_primary" {
  count           = var.primary_alb_dns == "" ? 0 : 1
  zone_id         = data.aws_route53_zone.this.zone_id
  name            = var.origin_domain
  type            = "CNAME"
  ttl             = 30
  records         = [var.primary_alb_dns]
  set_identifier  = "primary-${var.aws_region}"
  health_check_id = aws_route53_health_check.origin.id
  latency_routing_policy {
    region = var.aws_region
  }
}

resource "aws_route53_record" "origin_secondary" {
  count          = var.secondary_alb_dns == "" ? 0 : 1
  zone_id        = data.aws_route53_zone.this.zone_id
  name           = var.origin_domain
  type           = "CNAME"
  ttl            = 30
  records        = [var.secondary_alb_dns]
  set_identifier = "secondary-${var.secondary_region}"
  latency_routing_policy {
    region = var.secondary_region
  }
}

# Geo-routing example (data residency): European viewers -> secondary region, everyone else -> primary.
resource "aws_route53_record" "geo_eu" {
  count          = var.secondary_alb_dns == "" ? 0 : 1
  zone_id        = data.aws_route53_zone.this.zone_id
  name           = "geo.${var.zone_name}"
  type           = "CNAME"
  ttl            = 60
  records        = [var.secondary_alb_dns]
  set_identifier = "geo-eu"
  geolocation_routing_policy {
    continent = "EU"
  }
}

resource "aws_route53_record" "geo_default" {
  count          = var.primary_alb_dns == "" ? 0 : 1
  zone_id        = data.aws_route53_zone.this.zone_id
  name           = "geo.${var.zone_name}"
  type           = "CNAME"
  ttl            = 60
  records        = [var.primary_alb_dns]
  set_identifier = "geo-default"
  geolocation_routing_policy {
    country = "*"
  }
}
