variable "account_name" {
  description = "AWS account name."
  type        = string
}

variable "region" {
  description = "Region to deploy Chroma."
  type        = string
  default     = "us-east-1"
}
