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

variable "namespace" {
  description = "Kubernetes namespace for the Chroma deployment."
  type        = string
  default     = "chroma"
}

variable "bucket_name" {
  description = "Name of the S3 bucket to use for Chroma storage."
  type        = string
}
