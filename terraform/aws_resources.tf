resource "aws_iam_role" "snowflake_role" {
  name = "snowflake_s3_readonly_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com" # Esto es temporal
        }
      }
    ]
  })
}


resource "aws_iam_role_policy" "snowflake_s3_policy" {
  name = "snowflake_s3_policy"
  role = aws_iam_role.snowflake_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::luminacare-datalake-luis",
          "arn:aws:s3:::luminacare-datalake-luis/*"
        ]
      }
    ]
  })
}