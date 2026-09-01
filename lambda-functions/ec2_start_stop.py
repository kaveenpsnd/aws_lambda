"""Warm-DR bastion EC2 start/stop, with a pass-through trigger for the dr-scale Lambda.

Handles the half of a warm-DR bring-up/tear-down that env_start_stop.py deliberately
does not: the standalone bastion EC2 instance(s). Bastions are stopped and started
(never terminated), so the instance -- and its instance ID, private IP, and any local
state -- survives across DR cycles.

The EKS half is NOT reimplemented here. This function invokes env_start_stop.py
("dr-scale") with the same action, and that script remains the single owner of node
group scaling. Note the two halves use fundamentally different mechanisms, and that is
intentional: a bastion is a plain instance that can genuinely be stopped, while an EKS
managed node group's instances are owned by an Auto Scaling Group and are terminated
(not stopped) when it scales to zero. There is no "stopped EKS node" state to target.

The two steps do not gate each other -- nothing about the bastion depends on the node
groups or vice versa, so one failing never skips the other. Both outcomes are collected
and reported together.

Manual invoke only, no automatic trigger.

Event payload:
  {"action": "start"}
  {"action": "stop"}

Env vars:
  BASTION_TAG_KEY, BASTION_TAG_VALUE -- optional, default AutoStartStop / true.
    Which instances this function is allowed to touch. Tag-based rather than a
    hardcoded instance ID so a bastion replacement (an AMI bump forces one -- see
    vm-image-templates' rollout notes) doesn't require re-deploying this Lambda.
  DR_SCALE_FUNCTION_NAME -- optional. Name or ARN of the dr-scale Lambda. If unset,
    the EKS step is skipped (with a warning, not an error) and only the bastion is
    acted on -- useful for testing this function in isolation.
  SNS_TOPIC_ARN -- optional. Summary is skipped (with a log warning) if unset.
  ENVIRONMENT, LOG_LEVEL -- optional, informational / default INFO.
"""

import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DEFAULT_BASTION_TAG_KEY = "AutoStartStop"
DEFAULT_BASTION_TAG_VALUE = "true"
# Only instances in these states are worth acting on for a given action. An instance
# already in (or heading toward) the target state is reported as a no-op rather than
# being called against -- EC2 would reject it with IncorrectInstanceState anyway.
ACTIONABLE_STATES_BY_ACTION = {"start": ["stopped"], "stop": ["running"]}
SETTLED_STATES_BY_ACTION = {"start": ["running", "pending"], "stop": ["stopped", "stopping"]}
SUPPORTED_ACTIONS = set(ACTIONABLE_STATES_BY_ACTION)
RETRYABLE_ERROR_CODES = {
    "IncorrectInstanceState",
    "ThrottlingException",
    "RequestLimitExceeded",
    "TooManyRequestsException",
    "ServiceException",
}
MAX_ATTEMPTS = 4
BASE_BACKOFF_SECONDS = 5
# Fixed settings loaded once at startup: which instances this function may touch, which
# instance states each action applies to, and how to handle AWS saying it's busy.


def _log(level, message, **fields):
    logger.log(level, json.dumps({"message": message, **fields}, default=str))


# Writes one structured (JSON) log line, so log entries are easy to search and filter
# in CloudWatch instead of being plain, hard-to-parse sentences. Same helper the other
# Lambdas in this repo carry -- each script is packaged as a single standalone file, so
# these small helpers are duplicated rather than shared via an import.


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


# A generic "try this AWS action, and if AWS says it's just busy or the instance is
# mid-transition, wait a little and try again" helper -- up to 4 attempts. Any other
# kind of error is not retried, it fails right away.


def _find_bastion_instances(ec2, tag_key, tag_value):
    instances = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(
        Filters=[
            {"Name": f"tag:{tag_key}", "Values": [tag_value]},
            # Terminated/shutting-down instances are excluded outright -- they are not
            # something this function can or should ever act on.
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped"],
            },
        ]
    ):
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                instances.append(
                    {
                        "instance_id": instance["InstanceId"],
                        "state": instance.get("State", {}).get("Name"),
                    }
                )
    return instances


# Finds every instance this function is allowed to touch, by tag. Returns each one's ID
# and current state so the caller can decide which actually need acting on. Deliberately
# tag-scoped: an untagged instance is invisible to this function no matter what.


def _act_on_bastions(ec2, action, tag_key, tag_value):
    try:
        instances = _call_with_retry(
            lambda: _find_bastion_instances(ec2, tag_key, tag_value),
            description="describe_instances",
        )
    except ClientError as exc:
        _log(logging.ERROR, "Could not look up bastion instances", error=str(exc))
        return {
            "step": "bastion",
            "success": False,
            "error": str(exc),
            "error_code": exc.response.get("Error", {}).get("Code", ""),
        }

    if not instances:
        _log(
            logging.WARNING,
            "No instances matched the bastion tag, nothing to do",
            tag_key=tag_key,
            tag_value=tag_value,
        )
        return {"step": "bastion", "success": True, "matched": 0, "acted_on": [], "skipped": []}

    actionable_states = ACTIONABLE_STATES_BY_ACTION[action]
    settled_states = SETTLED_STATES_BY_ACTION[action]
    to_act_on = [i["instance_id"] for i in instances if i["state"] in actionable_states]
    already_settled = [i for i in instances if i["state"] in settled_states]

    if not to_act_on:
        _log(
            logging.INFO,
            "All matched instances already in the target state, no-op",
            action=action,
            instances=already_settled,
        )
        return {
            "step": "bastion",
            "success": True,
            "matched": len(instances),
            "acted_on": [],
            "skipped": already_settled,
        }

    api_call = ec2.start_instances if action == "start" else ec2.stop_instances
    try:
        _call_with_retry(
            lambda: api_call(InstanceIds=to_act_on),
            description=f"{action}_instances",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        _log(
            logging.ERROR,
            "Failed to change bastion instance state",
            action=action,
            instance_ids=to_act_on,
            error_code=code,
            error=str(exc),
        )
        return {
            "step": "bastion",
            "success": False,
            "matched": len(instances),
            "instance_ids": to_act_on,
            "error": str(exc),
            "error_code": code,
        }

    return {
        "step": "bastion",
        "success": True,
        "matched": len(instances),
        "acted_on": to_act_on,
        "skipped": already_settled,
    }


# The bastion half, start to finish: find the tagged instances, work out which ones the
# requested action actually applies to, and issue one start/stop call for them. Instances
# already in (or already heading toward) the target state are reported as skipped rather
# than being called against.
#
# This returns as soon as AWS ACCEPTS the request -- it does not wait for the instances to
# finish reaching running/stopped. A start typically takes tens of seconds to become
# SSM-reachable after this returns, so the summary below reports what was requested, not
# what has completed.


def _invoke_dr_scale(lambda_client, function_name, action):
    try:
        response = _call_with_retry(
            lambda: lambda_client.invoke(
                FunctionName=function_name,
                # RequestResponse, not Event: this is a runbook tool, and a silent
                # failure in the EKS half would be worse than waiting the second or two
                # env_start_stop.py takes to return. It only issues API calls and never
                # waits for nodes to actually register, so it returns quickly.
                InvocationType="RequestResponse",
                Payload=json.dumps({"action": action}).encode("utf-8"),
            ),
            description=f"invoke:{function_name}",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        _log(
            logging.ERROR,
            "Failed to invoke dr-scale Lambda",
            function_name=function_name,
            error_code=code,
            error=str(exc),
        )
        return {"step": "eks", "success": False, "error": str(exc), "error_code": code}

    raw_payload = response["Payload"].read().decode("utf-8")
    try:
        payload = json.loads(raw_payload) if raw_payload else None
    except json.JSONDecodeError:
        payload = raw_payload

    # A Lambda that raised inside its own handler still comes back as StatusCode 200 --
    # FunctionError is the only thing that distinguishes it from a success.
    function_error = response.get("FunctionError")
    if function_error:
        _log(
            logging.ERROR,
            "dr-scale Lambda returned an error",
            function_name=function_name,
            function_error=function_error,
            payload=payload,
        )
        return {
            "step": "eks",
            "success": False,
            "function_error": function_error,
            "payload": payload,
        }

    return {"step": "eks", "success": True, "payload": payload}


# The EKS half: hand the same action straight to env_start_stop.py and report what it
# said. This function never calls the EKS API itself -- node group scaling has exactly
# one owner, and it isn't this script.


def _publish_summary(sns, topic_arn, action, results):
    succeeded = [r for r in results if r["success"]]
    lines = [f"DR EC2 start/stop ({action}): {len(succeeded)}/{len(results)} steps succeeded"]
    for r in results:
        status_word = "OK" if r["success"] else "FAILED"
        if not r["success"]:
            detail = r.get("error") or r.get("function_error", "")
        elif r["step"] == "bastion":
            acted, skipped = r.get("acted_on", []), r.get("skipped", [])
            detail = f"{len(acted)} instance(s) requested to {action}, {len(skipped)} already settled"
            if acted:
                detail += f" ({', '.join(acted)})"
        else:
            payload = r.get("payload") or {}
            nodegroups = payload.get("succeeded") if isinstance(payload, dict) else None
            total = payload.get("total") if isinstance(payload, dict) else None
            detail = (
                f"dr-scale reported {nodegroups}/{total} node groups succeeded"
                if nodegroups is not None
                else "dr-scale invoked"
            )
        lines.append(f"  [{status_word}] {r['step']}: {detail}")
    lines.append("")
    lines.append("Note: bastion state changes are requested, not confirmed -- instances")
    lines.append("may take a further ~1 minute to become reachable after a start.")
    message = "\n".join(lines)
    subject = f"DR EC2 start/stop ({action}): {len(succeeded)}/{len(results)} succeeded"[:100]
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as exc:
        _log(logging.ERROR, "Failed to publish SNS summary", error=str(exc))


# Sends one human-readable notification covering both steps, so someone doesn't have to
# dig through raw logs to find out what just happened. A failure to send this is only
# logged -- it doesn't undo or affect the work that already happened.


def handler(event, context):
    event = event or {}
    action = event.get("action")
    if action not in SUPPORTED_ACTIONS:
        raise ValueError(f"Unsupported action '{action}', expected 'start' or 'stop'")

    environment = os.environ.get("ENVIRONMENT", "unknown")
    tag_key = os.environ.get("BASTION_TAG_KEY", DEFAULT_BASTION_TAG_KEY)
    tag_value = os.environ.get("BASTION_TAG_VALUE", DEFAULT_BASTION_TAG_VALUE)
    dr_scale_function_name = os.environ.get("DR_SCALE_FUNCTION_NAME")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")

    _log(
        logging.INFO,
        "DR EC2 start/stop Lambda invoked",
        action=action,
        environment=environment,
        bastion_tag=f"{tag_key}={tag_value}",
        dr_scale_function_name=dr_scale_function_name,
    )

    # Setup: validate the requested action and read configuration. Nothing below this
    # point can change which instances are in scope -- that's fixed by the tag above.

    results = [_act_on_bastions(boto3.client("ec2"), action, tag_key, tag_value)]

    if dr_scale_function_name:
        results.append(
            _invoke_dr_scale(boto3.client("lambda"), dr_scale_function_name, action)
        )
    else:
        _log(
            logging.WARNING,
            "DR_SCALE_FUNCTION_NAME not set, skipping the EKS node group step",
        )

    for result in results:
        _log(
            logging.INFO if result["success"] else logging.ERROR,
            "Step complete",
            **result,
        )

    # The main event: the bastion step, then the dr-scale hand-off. Neither waits on the
    # other's underlying work to finish, and one failing doesn't stop the other -- every
    # result, success or failure, is collected here.

    if sns_topic_arn:
        _publish_summary(boto3.client("sns"), sns_topic_arn, action, results)
    else:
        _log(logging.WARNING, "SNS_TOPIC_ARN not set, skipping summary publish")

    return {
        "action": action,
        "succeeded": sum(1 for r in results if r["success"]),
        "total": len(results),
        "results": results,
    }

    # Wrap-up: notify (if configured) and hand back a full structured summary -- how many
    # steps succeeded out of how many, plus the detailed per-step results -- so the
    # outcome is visible both as a notification and as raw data for a manual console test.
