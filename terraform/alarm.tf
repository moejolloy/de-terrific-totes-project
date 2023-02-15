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
  alarm_actions       = ["arn:aws:sns:us-east-1:206923297952:test-error-alerts"]
# alarm actions may need to send an email, or some other way of alerting us to the alarm

}