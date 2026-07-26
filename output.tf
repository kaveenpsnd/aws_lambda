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
