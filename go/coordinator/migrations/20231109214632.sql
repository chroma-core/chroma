-- Create "collection_metadata" table
CREATE TABLE `collection_metadata` (
  `collection_id` varchar(191) NOT NULL,
  `key` varchar(191) NOT NULL,
  `str_value` longtext NULL,
  `int_value` bigint NULL,
  `float_value` double NULL,
  `ts` bigint NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`collection_id`, `key`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- Create "collections" table
CREATE TABLE `collections` (
  `id` varchar(191) NOT NULL,
  `name` varchar(191) NULL,
  `topic` longtext NULL,
  `dimension` int NULL,
  `database_id` longtext NULL,
  `ts` bigint NULL DEFAULT 0,
  `is_deleted` bool NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name` (`name`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- Create "databases" table
CREATE TABLE `databases` (
  `id` varchar(191) NOT NULL,
  `name` varchar(128) NULL,
  `tenant_id` varchar(128) NULL,
  `ts` bigint NULL DEFAULT 0,
  `is_deleted` bool NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id` (`id`),
  UNIQUE INDEX `idx_tenantid_name` (`name`, `tenant_id`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- Create "segment_metadata" table
CREATE TABLE `segment_metadata` (
  `segment_id` varchar(191) NOT NULL,
  `key` varchar(191) NOT NULL,
  `str_value` longtext NULL,
  `int_value` bigint NULL,
  `float_value` double NULL,
  `ts` bigint NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`segment_id`, `key`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- Create "segments" table
CREATE TABLE `segments` (
  `id` varchar(191) NOT NULL,
  `type` longtext NOT NULL,
  `scope` longtext NULL,
  `topic` longtext NULL,
  `ts` bigint NULL DEFAULT 0,
  `is_deleted` bool NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `collection_id` longtext NULL,
  PRIMARY KEY (`id`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- Create "tenants" table
CREATE TABLE `tenants` (
  `id` varchar(191) NOT NULL,
  `ts` bigint NULL DEFAULT 0,
  `is_deleted` bool NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id` (`id`)
) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
