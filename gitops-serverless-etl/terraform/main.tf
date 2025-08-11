provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_object" "csv_upload" {
  bucket = "awsbucket-lurdhu-123456"
  key    = "input/Students Social Media Addiction.csv"
  source = "C:/Users/ravi/OneDrive/Desktop/gitops-serverless-etl/my_data.csv/Students Social Media Addiction.csv"
  etag   = filemd5("C:/Users/ravi/OneDrive/Desktop/gitops-serverless-etl/my_data.csv/Students Social Media Addiction.csv")
}

resource "aws_s3_object" "glue_script" {
  bucket = "awsbucket-lurdhu-123456"
  key    = "scripts/etl_script.py"
  source = "C:/Users/ravi/OneDrive/Desktop/gitops-serverless-etl/etl/etl_script.py"
  etag   = filemd5("C:/Users/ravi/OneDrive/Desktop/gitops-serverless-etl/etl/etl_script.py")
}

resource "aws_glue_job" "etl_job" {
  name     = "etl-csv-job"
  role_arn = "arn:aws:iam::077058346592:role/terraformtask"

  command {
    name            = "glueetl"
    script_location = "s3://awsbucket-lurdhu-123456/scripts/etl_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"   = "python"
    "--SOURCE_S3_PATH" = "s3://awsbucket-lurdhu-123456/input/Students Social Media Addiction.csv"
    "--OUTPUT_S3_PATH" = "s3://awsbucket-lurdhu-123456/output/"
  }

  glue_version = "4.0"
  max_retries  = 0
}
