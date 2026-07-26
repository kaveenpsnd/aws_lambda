"""CloudFront VPC origin cutover between DR and stage.

Bidirectional -- the same Lambda handles both failover (stage -> dr) and
failback (dr -> stage). Manual invoke only, no automatic trigger.

Event payload:
  {"target": "dr"}                                 -- point at the DR VPC origin
  {"target": "stage"}                              -- point at the stage VPC origin
  {"vpc_origin_id": "...", "domain_name": "..."}    -- explicit override, wins over target

Env vars (all set by this deployment):
  DISTRIBUTION_ID, ORIGIN_ID, SNS_TOPIC_ARN, DR_VPC_ORIGIN_ID,
  STAGE_VPC_ORIGIN_ID, DR_ORIGIN_DOMAIN_NAME, STAGE_ORIGIN_DOMAIN_NAME,
  INVALIDATE_ON_CUTOVER ("true"/"false").
"""

import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DEFAULT_ORIGIN_ID = "NLB-Origin"
POLL_INTERVAL_SECONDS = 20
POLL_SAFETY_MARGIN_MS = 30_000
# Fixed settings loaded once at startup: which origin to edit if not overridden,
# how often to check whether CloudFront finished rolling out the change, and how
# much of a safety buffer to leave before the Lambda itself times out.


def _log(level, message, **fields):
    logger.log(level, json.dumps({"message": message, **fields}, default=str))


# Writes one structured (JSON) log line, so log entries are easy to search and
# filter in CloudWatch instead of being plain, hard-to-parse sentences.


def _remaining_ms(context):
    if context is None:
        return None
    try:
        return context.get_remaining_time_in_millis()
    except Exception:
        return None


# Checks how much running time this Lambda invocation has left before AWS kills
# it for exceeding its timeout, so the polling loop later knows when to stop
# waiting and report "not confirmed yet" instead of just getting cut off.


def _resolve_target(event):
    override_vpc_origin_id = event.get("vpc_origin_id")
    if override_vpc_origin_id:
        return "override", override_vpc_origin_id, event.get("domain_name")

    target = event.get("target")
    if target == "dr":
        return target, os.environ["DR_VPC_ORIGIN_ID"], os.environ["DR_ORIGIN_DOMAIN_NAME"]
    if target == "stage":
        return target, os.environ["STAGE_VPC_ORIGIN_ID"], os.environ["STAGE_ORIGIN_DOMAIN_NAME"]
    raise ValueError(
        f"Unsupported target '{target}', expected 'dr', 'stage', or an explicit vpc_origin_id override"
    )


# Figures out where traffic should be pointed. An explicit override in the event
# always wins; otherwise "dr" or "stage" is translated into the matching VPC
# origin ID + domain name from configuration. Anything else is rejected outright.


def _find_origin(config, origin_id):
    for origin in config["Origins"]["Items"]:
        if origin["Id"] == origin_id:
            return origin
    raise ValueError(f"Origin id '{origin_id}' not found in distribution config")


# Searches CloudFront's current settings for the one origin entry we're allowed
# to edit (matched by its ID), and stops with a clear error if it's not there.


def _apply_origin_target(config, origin_id, target_vpc_origin_id, target_domain_name):
    origin = _find_origin(config, origin_id)
    vpc_origin_config = origin.get("VpcOriginConfig")
    if not vpc_origin_config:
        raise ValueError(f"Origin '{origin_id}' has no VpcOriginConfig")

    previous_vpc_origin_id = vpc_origin_config.get("VpcOriginId")
    if previous_vpc_origin_id == target_vpc_origin_id:
        return False, previous_vpc_origin_id

    vpc_origin_config["VpcOriginId"] = target_vpc_origin_id
    if target_domain_name and origin.get("DomainName") != target_domain_name:
        origin["DomainName"] = target_domain_name
    return True, previous_vpc_origin_id


# Edits CloudFront's settings in memory (nothing sent to AWS yet): if the origin
# already points where we want, report "no change needed." Otherwise switch it to
# the new target, and update the domain name too, but only if that's actually
# different. This is the single source of truth for "did anything actually change."


def _update_with_etag_retry(cf, distribution_id, origin_id, target_vpc_origin_id, target_domain_name, config, etag):
    try:
        cf.update_distribution(Id=distribution_id, DistributionConfig=config, IfMatch=etag)
        return
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code != "PreconditionFailed":
            raise
        _log(logging.WARNING, "ETag conflict updating distribution, re-fetching config and retrying once")

    refreshed = cf.get_distribution_config(Id=distribution_id)
    fresh_config = refreshed["DistributionConfig"]
    fresh_etag = refreshed["ETag"]
    _apply_origin_target(fresh_config, origin_id, target_vpc_origin_id, target_domain_name)
    cf.update_distribution(Id=distribution_id, DistributionConfig=fresh_config, IfMatch=fresh_etag)


# Actually sends the edited settings to AWS. CloudFront requires proof (an ETag)
# that nobody else changed the distribution since we last read it; if someone did
# (a conflict), this re-reads the latest settings, re-applies our same edit on top
# of that newer copy, and tries once more -- it doesn't retry forever.


def _wait_for_deployed(cf, distribution_id, context):
    start = time.monotonic()
    while True:
        resp = cf.get_distribution(Id=distribution_id)
        status = resp["Distribution"]["Status"]
        if status == "Deployed":
            return True, time.monotonic() - start

        remaining = _remaining_ms(context)
        if remaining is not None and remaining < POLL_SAFETY_MARGIN_MS:
            _log(
                logging.WARNING,
                "Stopped polling for Deployed status before Lambda timeout",
                last_status=status,
            )
            return False, time.monotonic() - start

        time.sleep(POLL_INTERVAL_SECONDS)


# CloudFront changes aren't instant -- they roll out gradually. This checks back
# every 20 seconds until AWS reports the rollout is fully "Deployed," but bails
# out gracefully (not a crash) if the Lambda is about to run out of time.


def _create_invalidation(cf, distribution_id, context):
    caller_reference = getattr(context, "aws_request_id", None) or str(time.monotonic())
    resp = cf.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/*"]},
            "CallerReference": caller_reference,
        },
    )
    return resp["Invalidation"]["Id"]


# Tells CloudFront to clear its cache for everything ("/*"), so visitors stop
# getting old cached responses from before the cutover. Uses the Lambda's own
# unique request ID as the required "don't repeat this by accident" token.


def _publish_summary(
    sns,
    topic_arn,
    target_label,
    previous_vpc_origin_id,
    new_vpc_origin_id,
    no_op,
    deployed=None,
    deploy_duration_seconds=None,
    invalidation_result=None,
):
    if no_op:
        message = f"CloudFront cutover ({target_label}): already pointing at {new_vpc_origin_id}, no-op."
        subject = f"CloudFront cutover ({target_label}): no-op"
    else:
        lines = [
            f"CloudFront cutover to '{target_label}':",
            f"  Origin VpcOriginId: {previous_vpc_origin_id} -> {new_vpc_origin_id}",
            f"  Deploy status: {'Deployed' if deployed else 'NOT confirmed deployed (polling timed out)'}",
            f"  Deploy duration: {deploy_duration_seconds:.0f}s",
        ]
        if invalidation_result is not None:
            lines.append(f"  Invalidation: {invalidation_result}")
        message = "\n".join(lines)
        subject = f"CloudFront cutover ({target_label}): {'OK' if deployed else 'INCOMPLETE'}"
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except ClientError as exc:
        _log(logging.ERROR, "Failed to publish SNS summary", error=str(exc))


# Sends one human-readable notification: either "nothing changed" for a no-op, or
# the full picture for a real cutover -- old origin, new origin, whether it
# actually finished deploying, how long it took, and what happened with the cache
# invalidation. A failure to send this notification is only logged, never fatal.


def handler(event, context):
    event = event or {}
    distribution_id = os.environ["DISTRIBUTION_ID"]
    origin_id = os.environ.get("ORIGIN_ID", DEFAULT_ORIGIN_ID)
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    invalidate_on_cutover = os.environ.get("INVALIDATE_ON_CUTOVER", "true").strip().lower() == "true"

    target_label, target_vpc_origin_id, target_domain_name = _resolve_target(event)

    _log(
        logging.INFO,
        "CloudFront cutover Lambda invoked",
        target=target_label,
        distribution_id=distribution_id,
        origin_id=origin_id,
    )

    # Setup: read configuration, figure out where we're cutting over to, and log
    # the request before touching anything in AWS.

    cf = boto3.client("cloudfront")
    config_resp = cf.get_distribution_config(Id=distribution_id)
    config = config_resp["DistributionConfig"]
    etag = config_resp["ETag"]

    changed, previous_vpc_origin_id = _apply_origin_target(config, origin_id, target_vpc_origin_id, target_domain_name)

    # Fetch CloudFront's current live settings, then work out (in memory only)
    # whether our target actually requires a change.

    if not changed:
        _log(logging.INFO, "Already pointing at target VPC origin, no-op", vpc_origin_id=target_vpc_origin_id)
        if sns_topic_arn:
            _publish_summary(
                boto3.client("sns"), sns_topic_arn, target_label, previous_vpc_origin_id, target_vpc_origin_id, True
            )
        return {"target": target_label, "no_op": True, "vpc_origin_id": target_vpc_origin_id}

    # No-op path: we're already pointing where we want, so stop here -- no write
    # to CloudFront, no waiting, no cache invalidation. Just a short notice and return.

    _update_with_etag_retry(cf, distribution_id, origin_id, target_vpc_origin_id, target_domain_name, config, etag)
    _log(logging.INFO, "UpdateDistribution submitted, polling for Deployed status", vpc_origin_id=target_vpc_origin_id)

    deployed, deploy_duration_seconds = _wait_for_deployed(cf, distribution_id, context)

    # The real cutover: push the origin change to CloudFront, then wait and
    # confirm the rollout actually finished (or note that it didn't, within the
    # time available).

    invalidation_result = None
    if invalidate_on_cutover:
        try:
            invalidation_id = _create_invalidation(cf, distribution_id, context)
            invalidation_result = {"success": True, "invalidation_id": invalidation_id}
            _log(logging.INFO, "Invalidation created", invalidation_id=invalidation_id)
        except ClientError as exc:
            invalidation_result = {"success": False, "error": str(exc)}
            _log(
                logging.ERROR,
                "Invalidation failed; origin swap itself was NOT rolled back",
                error=str(exc),
            )
    else:
        _log(logging.INFO, "INVALIDATE_ON_CUTOVER is false, skipping invalidation")

    # Optional last step: clear the cache if configured to. If this specific step
    # fails, it's reported separately -- the already-successful origin swap above
    # is deliberately never undone because of a cache-clearing problem.

    if sns_topic_arn:
        _publish_summary(
            boto3.client("sns"),
            sns_topic_arn,
            target_label,
            previous_vpc_origin_id,
            target_vpc_origin_id,
            False,
            deployed=deployed,
            deploy_duration_seconds=deploy_duration_seconds,
            invalidation_result=invalidation_result,
        )
    else:
        _log(logging.WARNING, "SNS_TOPIC_ARN not set, skipping summary publish")

    return {
        "target": target_label,
        "no_op": False,
        "previous_vpc_origin_id": previous_vpc_origin_id,
        "new_vpc_origin_id": target_vpc_origin_id,
        "deployed": deployed,
        "deploy_duration_seconds": deploy_duration_seconds,
        "invalidation": invalidation_result,
    }

    # Wrap-up: notify (if configured), then hand back a full structured result --
    # what changed, whether it finished deploying, how long it took, and what
    # happened with the invalidation -- for both a human and a console test to read.
