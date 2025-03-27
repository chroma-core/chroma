variable "region" {
  description = "Region to deploy Chroma."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Unique name for the Chroma deployment."
  type        = string
  default     = "chroma"
}

variable "vpc_cidr" {
  description = "VPC CIDR block. Must be /17 or larger."
  type        = string
  default     = "10.0.0.0/17"
}
