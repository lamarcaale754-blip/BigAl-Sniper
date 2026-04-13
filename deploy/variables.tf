variable "aws_region" {
  description = "AWS region — us-east-1 for lowest latency to Solana validators + Jito NY"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type — c6g.xlarge (4 vCPU ARM) for CPU pinning, or c6i.xlarge (Intel)"
  type        = string
  default     = "c6i.xlarge"
}

variable "key_name" {
  description = "Name of existing EC2 key pair for SSH access"
  type        = string
}

variable "helius_rpc_url" {
  description = "Helius mainnet RPC URL (with API key)"
  type        = string
  sensitive   = true
}

variable "helius_api_key" {
  description = "Helius API key (for gRPC sidecar / LaserStream)"
  type        = string
  sensitive   = true
}

variable "wallet_private_key" {
  description = "Solana wallet private key (base58)"
  type        = string
  sensitive   = true
}

variable "jito_block_engine" {
  description = "Jito block engine URL"
  type        = string
  default     = "https://ny.mainnet.block-engine.jito.wtf"
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH (your IP, e.g. 1.2.3.4/32)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "instance_name" {
  description = "Name tag for the EC2 instance"
  type        = string
  default     = "bigal-sniper"
}
