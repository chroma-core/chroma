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
