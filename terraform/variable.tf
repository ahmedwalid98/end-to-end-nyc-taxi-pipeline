variable "s3_bucket_name" {
  description = "Name of S3 buckut"
  default     = "nyc-taxi-walid-lab-2026"
}

variable "region" {
  description = "Region Value"
  default     = "eu-north-1"
}

variable "glue_catalog_database" {
  description = "Name of Glue catalog database"
  default     = "nyc_taxi_database"
}

variable "profile" {
  description = "Name of the profile with the creds"
  default     = "default"
}
