resource "aws_db_subnet_group" "rds_subnet_group" {
  name       = "rds_subnet_group"
  subnet_ids = [aws_subnet.public_subnet.id, aws_subnet.public_subnet_2.id]
  tags = {
    Name = "RDS Subnet Group"
  }
}



resource "aws_db_instance" "rds_instance" {
  allocated_storage      = 20
  storage_type           = "gp2"
  engine                 = "postgres"
  engine_version         = "12.17"
  instance_class         = "db.t3.micro"
  identifier             = "household-consumption-db"
  username               = "dbadmin" 
  password               = var.rds_password
  db_name                = var.rds_db_name
  port                   = 5432
  publicly_accessible    = true
  skip_final_snapshot    = true
  multi_az               = false
  deletion_protection    = false

  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.rds_subnet_group.name

  
  
  tags = {
    Name = "HouseholdConsumptionDB"
  }
}
