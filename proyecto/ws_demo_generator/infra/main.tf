terraform {
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.0" 
    }

    minio = {
      source  = "aminueza/minio"
      version = ">= 3.0.0"
    }

    kafka = {
      source = "Mongey/kafka"
    }
  }
}

# https://registry.terraform.io/providers/cyrilgdn/postgresql/latest/docs
provider "postgresql" {
  host     = "postgresql" # Or your local PostgreSQL host
  port     = 5432        # Or your PostgreSQL port
  username = "postgres"  # Your PostgreSQL username
  password = "password-root"          # Your PostgreSQL password
  sslmode  = "disable"
}

resource "postgresql_database" "local_db" {
  name      = "local_db" # The name of your database
}
resource "postgresql_role" "local_user" {
  name     = "local_user" # The name of the user
  password = "local_password" # The password for the user
  login    = true            # Allow the user to log in
}

resource "postgresql_schema" "ingestion_schema" {
  name  = "ingestion_schema"
  owner = postgresql_role.local_user.name
  database = postgresql_database.local_db.name
}

resource "postgresql_grant" "local_grant" {
  database    = postgresql_database.local_db.name
  role        = postgresql_role.local_user.name
  privileges  = ["ALL"]
  schema      = postgresql_schema.ingestion_schema.name
  object_type = "table"
}

resource "postgresql_grant" "public_grant" {
  database    = postgresql_database.local_db.name
  role        = postgresql_role.local_user.name
  privileges  = ["ALL"]
  schema      = "public"
  object_type = "table"
}


resource "local_file" "foo" {
  content  = "foo!"
  filename = "${path.module}/foo.bar2"
}


# https://github.com/aminueza/terraform-provider-minio
provider "minio" {
  minio_server   = "minio:9000"
  minio_user     = "minio-root-user"
  minio_password = "minio-root-password"
}

resource "minio_s3_bucket" "local_bucket" {
  bucket = "local-bucket"
  acl    = "public"
}

resource "minio_s3_object" "txt_file" {
  depends_on  = [minio_s3_bucket.local_bucket]
  bucket_name = minio_s3_bucket.local_bucket.bucket
  object_name = "text.txt"
  content     = "Hello, World!"
}


provider "kafka" {
  bootstrap_servers = ["kafka:19092"]
  tls_enabled = false
}

resource "kafka_topic" "logs" {
  name               = "systemd_logs"
  replication_factor = 1
  partitions         = 1

  config = {
    "segment.ms"     = "20000"
    "cleanup.policy" = "compact"
  }
}

#resource "kafka_topic" "message_logs" {
#  name               = "message_logs"
#  replication_factor = 1
#  partitions         = 1

#  config = {
#    "segment.ms"     = "20000"
#    "cleanup.policy" = "compact"
#  }
#}