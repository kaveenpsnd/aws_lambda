"""Warm-DR EKS node group start/stop for the dr-scale Lambda.

Scales the three DR cluster node groups (system, user, utils) to a floor of
2 (start) or 0 (stop). Cluster Autoscaler handles everything beyond that
floor under real load -- this script only sets the floor, never min/max.

Manual invoke only, no automatic trigger.

Event payload:
  {"action": "start"}
  {"action": "stop"}

Env vars:
  ENVIRONMENT, EKS_CLUSTER_NAME, SNS_TOPIC_ARN -- always set by this deployment.
  RDS_CLUSTER_IDS, MANAGED_RULE_NAMES -- optional, not used by this deployment
    (this script is shared/reused elsewhere with a broader scope; DR's
    invocation intentionally omits them -- must not crash or require them).

TODO: this assumes Cluster Autoscaler is actually running its workload in the
DR cluster (not just that its IAM role exists) -- confirm that's really
deployed before relying on it to scale past the floor this script sets.
"""

import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

NODEGROUPS = ["system", "user", "utils"]
DESIRED_SIZE_BY_ACTION = {"start": 2, "stop": 0}
RETRYABLE_ERROR_CODES = {"ResourceInUseException", "ThrottlingException"}
MAX_ATTEMPTS = 4
BASE_BACKOFF_SECONDS = 5
# Fixed settings loaded once at startup: the three node groups this script is
# allowed to touch, the desired size for each action, and how to handle AWS
# telling us it's temporarily busy.


def _log(level, message, **fields):
    logger.log(level, json.dumps({"message": message, **fields}, default=str))


# Writes one structured (JSON) log line, so log entries are easy to search and
# filter in CloudWatch instead of being plain, hard-to-parse sentences.


def _call_with_retry(fn, description):
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            last_error = exc
            if code in RETRYABLE_ERROR_CODES and attempt < MAX_ATTEMPTS:
                backoff = BASE_BACKOFF_SECONDS * attempt
                _log(
                    logging.WARNING,
                    "Retryable error, backing off",
                    action=description,
                    error_code=code,
                    attempt=attempt,
                    backoff_seconds=backoff,
                )
                time.sleep(backoff)
                continue
            raise
    raise last_error


# A generic "try this AWS action, and if AWS says it's just busy or the node
# group is mid-update, wait a little and try again" helper -- up to 4 attempts.
# Any other kind of error is not retried, it fails right away.


def _warn_if_set(env_var_name):
    if os.environ.get(env_var_name):
        _log(
            logging.WARNING,
            f"{env_var_name} is set but not acted on by this deployment (EKS-only scope)",
        )


# This script is shared with other, broader-scope deployments that may also
# manage RDS/WAF settings via these two env vars. For DR, they're intentionally
# unused -- this just logs a heads-up if someone sets them here by mistake,
# rather than silently ignoring or crashing on them.


def _scale_nodegroup(eks, cluster_name, nodegroup_name, desired_size):
    previous_desired_size = None
    try:
        desc = eks.describe_nodegroup(clusterName=cluster_name, nodegroupName=nodegroup_name)
        previous_desired_size = desc["nodegroup"]["scalingConfig"].get("desiredSize")
    except ClientError as exc:
        _log(
            logging.WARNING,
            "Could not describe nodegroup before scaling",
            nodegroup=nodegroup_name,
            error=str(exc),
        )

    try:
        _call_with_retry(
            lambda: eks.update_nodegroup_config(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name,
                scalingConfig={"desiredSize": desired_size},
            ),
            description=f"update_nodegroup_config:{nodegroup_name}",
        )
        return {
            "nodegroup": nodegroup_name,
            "success": True,
            "previous_desired_size": previous_desired_size,
            "desired_size": desired_size,
        }
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        _log(
            logging.ERROR,
            "Failed to update nodegroup",
            nodegroup=nodegroup_name,
            error_code=code,
            error=str(exc),
        )
        return {
            "nodegroup": nodegroup_name,
            "success": False,
            "previous_desired_size": previous_desired_size,
            "error": str(exc),
            "error_code": code,
        }


# The actual work for ONE node group: check its current size first (just so the
# report can show "before -> after"), then set the new desired size -- never
# touching min/max. Any AWS error here is caught and turned into a "failed"
# result rather than raised, so the caller can keep going and try the other
# node groups even if this one has a problem.


def _publish_summary(sns, topic_arn, action, cluster_name, results):
    succeeded = [r for r in results if r["success"]]
    lines = [
        f"DR EKS scale ({action}) on {cluster_name}: {len(succeeded)}/{len(results)} node groups succeeded"
    ]
    for r in results:
        status_word = "OK" if r["success"] else "FAILED"
        detail = (
            f"{r.get('previous_desired_size')} -> {r.get('desired_size')}"
            if r["success"]
            else r.get("error", "")
        )
        lines.append(f"  [{status_word}] {r['nodegroup']}: {detail}")
    message = "\n".join(lines)
    subject = f"DR EKS scale ({action}): {len(succeeded)}/{len(results)} succeeded"[:100]
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as exc:
        _log(logging.ERROR, "Failed to publish SNS summary", error=str(exc))


# Sends one human-readable notification listing every node group and whether it
# succeeded or failed, so someone doesn't have to dig through raw logs to find
# out what just happened. A failure to send this notification is only logged --
# it doesn't undo or affect the scaling work that already happened.


def handler(event, context):
    event = event or {}
    action = event.get("action")
    if action not in DESIRED_SIZE_BY_ACTION:
        raise ValueError(f"Unsupported action '{action}', expected 'start' or 'stop'")

    cluster_name = os.environ["EKS_CLUSTER_NAME"]
    environment = os.environ.get("ENVIRONMENT", "unknown")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")

    _warn_if_set("RDS_CLUSTER_IDS")
    _warn_if_set("MANAGED_RULE_NAMES")

    _log(
        logging.INFO,
        "DR EKS scale Lambda invoked",
        action=action,
        cluster_name=cluster_name,
        environment=environment,
    )

    # Setup: validate the requested action, read configuration, and flag (without
    # failing) any unused env vars this DR deployment doesn't act on.

    eks = boto3.client("eks")
    desired_size = DESIRED_SIZE_BY_ACTION[action]
    results = [_scale_nodegroup(eks, cluster_name, ng, desired_size) for ng in NODEGROUPS]

    for r in results:
        _log(
            logging.INFO if r["success"] else logging.ERROR,
            "Nodegroup scale attempt complete",
            **r,
        )

    # The main event: scale all three node groups (system, user, utils), one at a
    # time. One group failing doesn't stop the others -- every result, success or
    # failure, is collected here.

    if sns_topic_arn:
        _publish_summary(boto3.client("sns"), sns_topic_arn, action, cluster_name, results)
    else:
        _log(logging.WARNING, "SNS_TOPIC_ARN not set, skipping summary publish")

    return {
        "action": action,
        "cluster_name": cluster_name,
        "succeeded": sum(1 for r in results if r["success"]),
        "total": len(results),
        "results": results,
    }

    # Wrap-up: notify (if configured) and hand back a full structured summary --
    # how many node groups succeeded out of how many, plus the detailed
    # per-node-group results -- so the outcome is visible both as a notification
    # and as raw data for a manual console test.
