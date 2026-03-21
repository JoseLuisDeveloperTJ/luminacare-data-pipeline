resource "snowflake_database" "luminacare_db" {
  name    = "LUMINACARE_DB"
  comment = "builded by Jose Luis Arteaga Rivera"
}

resource "snowflake_schema" "staging_schema" {
  database = snowflake_database.luminacare_db.name
  name     = "STAGING"
  comment = "Charging station for files Parquet S3"
}

resource "snowflake_warehouse" "compute_wh" {
  name           = "LUMINACARE_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_file_format" "parquet_format" {
  name        = "PARQUET_FORMAT"
  database    = snowflake_database.luminacare_db.name
  schema      = snowflake_schema.staging_schema.name
  format_type = "PARQUET"
  compression = "SNAPPY"
}

resource "snowflake_table" "tickets_staging" {
  database = snowflake_database.luminacare_db.name
  schema   = snowflake_schema.staging_schema.name
  name     = "RAW_TICKETS"

  column {
    name = "DATA"
    type = "VARIANT"
  }
}


resource "snowflake_storage_integration" "s3_int" {
  name    = "S3_LUMINACARE_INTEGRATION"
  comment = "Integracion with S3 handled by Terraform"
  type    = "EXTERNAL_STAGE"

  enabled = true

  storage_allowed_locations = ["s3://luminacare-datalake-luis/"] 
  storage_provider         = "S3"
  storage_aws_role_arn     = "arn:aws:iam::563415215444:role/snowflake_s3_readonly_role"
}