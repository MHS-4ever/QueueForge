terraform {
  required_version = ">= 1.6, < 2.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "shipsafe-credits"
  default_tags {
    tags = {
      Project     = "QueueForge"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}
