
resource "aws_secretsmanager_secret" "database_credentials" {
    name = "database_credentials"
}

resource "aws_secretsmanager_secret_version" "db_secret_string" {
    secret_id = aws_secretsmanager_secret.database_credentials.id 
    secret_string = var.database_info
}