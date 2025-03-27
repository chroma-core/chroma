output "vpc_id" {
    description = "VPC ID"
    value       = module.vpc.vpc_id
}

output "vpc_cidr" {
    description = "VPC CIDR block"
    value       = module.vpc.vpc_cidr_block
}

output "vpc_azs" {
    description = "VPC availability zones"
    value       = module.vpc.azs
}

output "vpc_private_subnets" {
    description = "VPC private Subnets"
    value       = module.vpc.private_subnets
}

output "vpc_default_security_group_id" {
    description = "VPC default security group ID"
    value       = module.vpc.default_security_group_id
}
