resource "aws_cloudwatch_log_metric_filter" "file-type-error" {
  name           = "text-file-error"
  pattern        = "InvalidFileTypeError"
  log_group_name = "/aws/lambda/${var.dummy_lambda_name}"

  metric_transformation {
    name      = "EventCount"
    namespace = "invalid-file-type"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_errors" {
  alarm_name          = "file-type-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "60"
  metric_name         = "EventCount"
  namespace           = "invalid-file-type"
  threshold           = "1"
  statistic           = "SampleCount"
  alarm_actions       = ["arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dummy-reader-topic"]
}

resource "aws_sns_topic" "dummy-reader-topic" {
  name = "dummy-reader-topic"
}

resource "aws_sns_topic_subscription" "dummy-reader-topic-subscription" {
  topic_arn = aws_sns_topic.dummy-reader-topic.arn
  protocol  = "email"
  endpoint  = "terrifictotedd@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "ingestion_lambda_success" {
  name           = "ingestion_lambda_success"
  pattern        = "Finished_ingestion_lambda"
  log_group_name = "/aws/lambda/${var.ingestion_lambda_name}"
  depends_on     = [aws_cloudwatch_log_group.ingestion_cloudwatch_log_group]

  metric_transformation {
    name      = "EventCount"
    namespace = "ingestion_lambda_success"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_complete" {
  alarm_name          = "ingestion_lambda_complete"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  period              = "30"
  metric_name         = "EventCount"
  namespace           = "ingestion_lambda_success"
  threshold           = "1"
  statistic           = "SampleCount"
  alarm_actions       = ["arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:trigger_processing_lambda_topic"]
  # alarm actions may need to send an email, or some other way of alerting us to the alarm
}
resource "aws_sns_topic" "trigger_processing_lambda_topic" {
  name = "trigger_processing_lambda_topic"
}

resource "aws_sns_topic_subscription" "trigger_processing_lambda_subscription" {
  topic_arn = aws_sns_topic.trigger_processing_lambda_topic.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.processing_lambda.arn
}

resource "aws_lambda_permission" "allow_sns_to_trigger_processing_lambda" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.processing_lambda.function_name
  principal      = "sns.amazonaws.com"
  source_arn     = aws_sns_topic.trigger_processing_lambda_topic.arn
  source_account = data.aws_caller_identity.current.account_id
}

