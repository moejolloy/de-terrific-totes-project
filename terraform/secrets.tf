
resource "aws_secretsmanager_secret" "database_credentials" {
    name = "database_credentials"
}

resource "aws_secretsmanager_secret_version" "db_secret_string" {
    secret_id = aws_secretsmanager_secret.database_credentials.id 
    secret_string = var.database_info
}


resource "aws_secretsmanager_secret" "warehouse_credentials" {
    name = "warehouse_credentials"
}

resource "aws_secretsmanager_secret_version" "wh_secret_string" {
    secret_id = aws_secretsmanager_secret.warehouse_credentials.id 
    secret_string = var.warehouse_info
}

