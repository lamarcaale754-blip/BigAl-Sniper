output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.bigal.id
}

output "public_ip" {
  description = "Public IP of the BigAl instance"
  value       = aws_instance.bigal.public_ip
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.bigal.public_ip}"
}

output "deploy_command" {
  description = "Command to upload code to the instance"
  value       = "scp -i ~/.ssh/${var.key_name}.pem -r . ubuntu@${aws_instance.bigal.public_ip}:/opt/bigal/"
}

output "region" {
  description = "AWS region"
  value       = var.aws_region
}
