# -------------------------------------------------------------------------------------
#
# Exposes the DR Lambda scripts' file paths so asgardeo-deployment-iac's
# deployments/dr/lambda/*.tf can resolve module.sre-task-automation.<output>
# (used via dirname(...) to get each Lambda's source directory).
# -------------------------------------------------------------------------------------

output "dr_scale_lambda_script" {
  value = "${path.module}/lambda-functions/env_start_stop.py"
}

output "cloudfront_cutover_lambda_script" {
  value = "${path.module}/lambda-functions/cloudfront_cutover.py"
}

output "aurora_dr_failover_lambda_script" {
  value = "${path.module}/lambda-functions/aurora_dr_failover.py"
}

# Bastion EC2 start/stop. Pairs with dr_scale_lambda_script rather than replacing it:
# this script stops/starts the bastion instance(s) and invokes the dr-scale Lambda for
# the EKS half, which remains env_start_stop.py's sole responsibility.
output "ec2_start_stop_lambda_script" {
  value = "${path.module}/lambda-functions/ec2_start_stop.py"
}
