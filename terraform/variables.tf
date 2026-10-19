variable "name" {
  type    = string
  default = "ecosystem"
}
variable "aws_region" {
  type    = string
  default = "ap-south-1"
}
variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}
variable "zone_name" {
  description = "Public Route 53 hosted zone, e.g. example.com"
  type        = string
}
variable "app_domain" {
  description = "Public hostname served by CloudFront, e.g. api.example.com"
  type        = string
}
variable "origin_domain" {
  description = "Hostname of the external ALB origin (must be covered by origin_certificate_arn)"
  type        = string
}
variable "cloudfront_certificate_arn" {
  description = "ACM certificate for app_domain, issued in us-east-1"
  type        = string
}
variable "origin_certificate_arn" {
  description = "ACM certificate for origin_domain in the workload region (used by the ALB Ingress)"
  type        = string
}
variable "origin_verify_secret" {
  description = "Shared secret CloudFront adds as X-Origin-Verify; the gateway/ALB rejects requests without it"
  type        = string
  sensitive   = true
}
variable "primary_alb_dns" {
  description = "DNS name of the external ALB created by the AWS LB Controller (set after first apply of k8s/external-alb-ingress.yaml)"
  type        = string
  default     = ""
}
variable "secondary_alb_dns" {
  description = "Optional DR/second-region ALB for latency + failover routing"
  type        = string
  default     = ""
}
variable "secondary_region" {
  type    = string
  default = "eu-west-1"
}
variable "waf_rate_limit" {
  description = "Requests per 5 minutes per IP before WAF blocks"
  type        = number
  default     = 2000
}
variable "enable_shield_advanced" {
  description = "Requires an active Shield Advanced subscription on the account"
  type        = bool
  default     = false
}
variable "eks_public_endpoint" {
  type    = bool
  default = false
}
variable "redis_auth_token" {
  type      = string
  sensitive = true
}
