output "sysdb_endpoint" {
    description = "sysdb endpoint"
    value       = aws_rds_cluster.sysdb.endpoint
}

output "sysdb_database" {
    description = "sysdb database"
    value       = aws_rds_cluster.sysdb.database_name
}

output "sysdb_port" {
    description = "sysdb port"
    value       = aws_rds_cluster.sysdb.port
}

output "sysdb_username" {
    description = "sysdb username"
    value       = aws_rds_cluster.sysdb.master_username
}

output "sysdb_password" {
    description = "sysdb password"
    sensitive   = true
    value       = random_password.sysdb_password.result
}

output "sysdb_url" {
    description = "sysdb url"
    sensitive   = true
    value       = format("postgresql://%s:%s@%s:%d/%s?sslmode=require",
        aws_rds_cluster.sysdb.master_username,
        random_password.sysdb_password.result,
        aws_rds_cluster.sysdb.endpoint,
        aws_rds_cluster.sysdb.port,
        aws_rds_cluster.sysdb.database_name
    )
}

output "logdb_endpoint" {
    description = "logdb endpoint"
    value       = aws_rds_cluster.logdb.endpoint
}

output "logdb_database" {
    description = "logdb database"
    value       = aws_rds_cluster.logdb.database_name
}

output "logdb_port" {
    description = "logdb port"
    value       = aws_rds_cluster.logdb.port
}

output "logdb_username" {
    description = "logdb username"
    value       = aws_rds_cluster.logdb.master_username
}

output "logdb_password" {
    description = "logdb password"
    sensitive   = true
    value       = random_password.logdb_password.result
}

output "logdb_url" {
    description = "logdb url"
    sensitive   = true
    value       = format("postgresql://%s:%s@%s:%d/%s?sslmode=require",
        aws_rds_cluster.logdb.master_username,
        random_password.logdb_password.result,
        aws_rds_cluster.logdb.endpoint,
        aws_rds_cluster.logdb.port,
        aws_rds_cluster.logdb.database_name
    )
}