terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}


resource "aws_s3_bucket" "nyc_taxi" {
  bucket        = var.s3_bucket_name
  force_destroy = false

}

resource "aws_glue_catalog_database" "dataset" {
  name = var.glue_catalog_database
}



