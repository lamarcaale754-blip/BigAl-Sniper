terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ──────────────────────────────────────────
#  Latest Ubuntu 22.04 AMI (x86_64)
# ──────────────────────────────────────────

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ──────────────────────────────────────────
#  VPC + Networking
# ──────────────────────────────────────────

resource "aws_vpc" "bigal" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.instance_name}-vpc" }
}

resource "aws_internet_gateway" "bigal" {
  vpc_id = aws_vpc.bigal.id
  tags   = { Name = "${var.instance_name}-igw" }
}

resource "aws_subnet" "bigal" {
  vpc_id                  = aws_vpc.bigal.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = { Name = "${var.instance_name}-subnet" }
}

resource "aws_route_table" "bigal" {
  vpc_id = aws_vpc.bigal.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bigal.id
  }

  tags = { Name = "${var.instance_name}-rt" }
}

resource "aws_route_table_association" "bigal" {
  subnet_id      = aws_subnet.bigal.id
  route_table_id = aws_route_table.bigal.id
}

# ──────────────────────────────────────────
#  Security Group
# ──────────────────────────────────────────

resource "aws_security_group" "bigal" {
  name_prefix = "${var.instance_name}-"
  description = "BigAl Sniper - SSH only"
  vpc_id      = aws_vpc.bigal.id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
    description = "SSH"
  }

  # All outbound (RPC, Jito, PumpPortal WS, Helius gRPC)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound"
  }

  tags = { Name = "${var.instance_name}-sg" }
}

# ──────────────────────────────────────────
#  Secrets in SSM Parameter Store
# ──────────────────────────────────────────

resource "aws_ssm_parameter" "helius_rpc_url" {
  name  = "/${var.instance_name}/HELIUS_RPC_URL"
  type  = "SecureString"
  value = var.helius_rpc_url
}

resource "aws_ssm_parameter" "helius_api_key" {
  name  = "/${var.instance_name}/HELIUS_API_KEY"
  type  = "SecureString"
  value = var.helius_api_key
}

resource "aws_ssm_parameter" "wallet_key" {
  name  = "/${var.instance_name}/BIGAL_PRIVATE_KEY"
  type  = "SecureString"
  value = var.wallet_private_key
}

# ──────────────────────────────────────────
#  IAM Role for SSM access
# ──────────────────────────────────────────

resource "aws_iam_role" "bigal" {
  name = "${var.instance_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "ssm_read" {
  name = "ssm-read-secrets"
  role = aws_iam_role.bigal.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter", "ssm:GetParameters"]
      Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/${var.instance_name}/*"
    }]
  })
}

resource "aws_iam_instance_profile" "bigal" {
  name = "${var.instance_name}-profile"
  role = aws_iam_role.bigal.name
}

# ──────────────────────────────────────────
#  EC2 Instance
# ──────────────────────────────────────────

resource "aws_instance" "bigal" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  subnet_id              = aws_subnet.bigal.id
  vpc_security_group_ids = [aws_security_group.bigal.id]
  iam_instance_profile   = aws_iam_instance_profile.bigal.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    iops        = 3000
    throughput   = 125
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    instance_name    = var.instance_name
    aws_region       = var.aws_region
    jito_block_engine = var.jito_block_engine
  })

  tags = {
    Name = var.instance_name
  }
}
