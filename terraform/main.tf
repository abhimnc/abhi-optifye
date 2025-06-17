provider "aws" {
  region = "eu-north-1" # or your actual region like "eu-north-1"
}

resource "aws_key_pair" "deployer" {
  key_name   = "kafka-key"
  public_key = file("~/.ssh/id_rsa.pub")
}

resource "aws_security_group" "kafka_sg" {
  name_prefix = "kafka-sg"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "kafka" {
  ami           = "ami-042b4708b1d05f512" # Ubuntu 20.04 in us-east-1
  instance_type = "t3.micro"
  key_name      = aws_key_pair.deployer.key_name
  security_groups = [aws_security_group.kafka_sg.name]

  tags = {
    Name = "KafkaInstance"
  }
}

