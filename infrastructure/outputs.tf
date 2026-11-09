output "bucket_name" {
  value = aws_s3_bucket.incoming.id
}

output "incoming_prefix" {
  value = "incoming/"
}

output "processing_queue_url" {
  value = aws_sqs_queue.processing.url
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dead_letter.url
}

output "lambda_function_name" {
  value = aws_lambda_function.processor.function_name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.processor.name
}

output "jobs_table_name" {
  value = aws_dynamodb_table.jobs.name
}

output "dlq_alarm_name" {
  value = aws_cloudwatch_metric_alarm.dlq_visible.alarm_name
}
