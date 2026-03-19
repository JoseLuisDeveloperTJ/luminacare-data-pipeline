terraform{
    required_providers {
        snowflake = {
            source = "Snowflake-Labs/snowflake"
            version = "~> 0.87.0"
        }
    }
}

#Keys:
provider "snowflake" {
  role     = "ACCOUNTADMIN"  
}

# Create database:
resource "snowflake_database" "luminacare_db" {
  name    = "LUMINACARE_DB"
  comment = "Construida por Luis el Cloud Data Engineer"
}

resource "snowflake_schema" "staging_schema" {
  database = snowflake_database.luminacare_db.name
  name     = "STAGING"
  comment  = "Land station for files parquiet from S3"
}


resource "snowflake_warehouse" "compute_wh" {
  name           = "LUMINACARE_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60     
  auto_resume    = true   
}