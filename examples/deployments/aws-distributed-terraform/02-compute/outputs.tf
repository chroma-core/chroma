output "eks_cluster_endpoint" {
    description = "EKS cluster endpoint"
    value       = module.eks.cluster_endpoint
}

output "eks_cluster_certificate_authority_data" {
    description = "EKS cluster certificate authority data"
    value       = module.eks.cluster_certificate_authority_data
}
