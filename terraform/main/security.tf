resource "aws_security_group" "rds_sg" {
  name        = "rds_security_group"
  description = "Allow inbound PostgreSQL access from allowed IP(s)"
  vpc_id      = aws_vpc.main_vpc.id

  
  ingress {
    description = "Allow PostgreSQL (port 5432) access"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
  }

  
  egress {
    description = "Allow outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  
  tags = {
    Name = "RDS_SG"
  }
}
