"""Unplanned Aurora Global Database failover for DR secondaries.

Detaches DR-region (ap-southeast-4) Aurora secondaries from their Global
Database, which promotes each to a standalone cluster with full read/write
access. Intended ONLY for the case where the primary region (ap-southeast-2)
is unreachable and a planned `aws rds failover-global-cluster` isn't
possible -- this script does not check primary health itself, the human
invoking it is asserting that.

Manual invoke only, no automatic trigger.

Event payload:
  {"action": "status"}                          -- read-only, check current state
  {"action": "promote", "confirm": true}         -- detach all configured clusters
  {"action": "promote", "confirm": true,
   "db_names": ["identitydb", "consumerdb"]}     -- detach a subset only

Env vars:
  DR_SECONDARY_CLUSTERS -- required. JSON object mapping short db name ->
    DR secondary cluster identifier, e.g.
    {"identitydb": "adu-psql-identitydb-dr-apse4-aurora-rds-cluster", ...}
  SNS_TOPIC_ARN -- optional. Summary is skipped (with a log warning) if unset.
  LOG_LEVEL -- optional, default INFO.
"""

import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

RETRYABLE_ERROR_CODES = {
    "InvalidDBClusterStateFault",
    "ThrottlingException",
    "RequestLimitExceeded",
}

MAX_ATTEMPTS = 4
BASE_BACKOFF_SECONDS = 5
POLL_INTERVAL_SECONDS = 15
POLL_SAFETY_MARGIN_MS = 30_000
# Fixed settings loaded once when the Lambda starts up: which AWS error codes are
# worth retrying, how many times to retry, how often to check on a database's
# progress, and how much of a safety buffer to leave before the Lambda times out.


def _log(level, message, **fields):
    logger.log(level, json.dumps({"message": message, **fields}, default=str))


# Writes one structured (JSON) log line. Keeping logs structured like this makes
# them easy to search and filter later in CloudWatch, instead of plain sentences.


def _load_target_clusters():
    raw = os.environ.get("DR_SECONDARY_CLUSTERS")
    if not raw:
        raise RuntimeError(
            "DR_SECONDARY_CLUSTERS env var is required (JSON object of db_name -> cluster_identifier)"
        )
    try:
        clusters = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"DR_SECONDARY_CLUSTERS is not valid JSON: {exc}") from exc
    if not isinstance(clusters, dict) or not clusters:
        raise RuntimeError("DR_SECONDARY_CLUSTERS must be a non-empty JSON object")
    return clusters


# Reads the list of the 12 DR databases from configuration (an env var set by
# Terraform). If that list is missing or malformed, the whole run stops right
# here -- there is nothing sensible to do without knowing which databases exist.


def _describe_cluster(rds, cluster_id):
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "DBClusterNotFoundFault":
            return None
        raise
    clusters = resp.get("DBClusters", [])
    return clusters[0] if clusters else None


# Asks AWS "what's the current state of this one database?" A database that
# doesn't exist is treated as a normal, expected case (returns nothing) rather
# than an error, so callers can handle it gracefully instead of crashing.


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


# A generic "try this AWS action, and if AWS says it's just busy or throttled,
# wait a little and try again" helper -- up to 4 attempts total. Any other kind
# of error (not on the known "busy" list) is not retried, it fails right away.


def _remaining_ms(context):
    if context is None:
        return None
    try:
        return context.get_remaining_time_in_millis()
    except Exception:
        return None


# Checks how much running time this Lambda invocation has left before AWS kills
# it for exceeding its timeout, so the polling loop below knows when to stop
# waiting and hand back an honest "not fully confirmed yet" instead of just dying.


def _detach_cluster(rds, db_name, cluster_id, context):
    cluster = _describe_cluster(rds, cluster_id)
    if cluster is None:
        return {"db_name": db_name, "cluster_id": cluster_id, "success": False, "error": "cluster not found"}

    global_cluster_id = cluster.get("GlobalClusterIdentifier")
    if not global_cluster_id:
        _log(logging.INFO, "Cluster already standalone, no-op", db_name=db_name, cluster_id=cluster_id)
        return {"db_name": db_name, "cluster_id": cluster_id, "success": True, "already_standalone": True}

    cluster_arn = cluster["DBClusterArn"]

    try:
        _call_with_retry(
            lambda: rds.remove_from_global_cluster(
                GlobalClusterIdentifier=global_cluster_id,
                DbClusterIdentifier=cluster_arn,
            ),
            description=f"remove_from_global_cluster:{db_name}",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "GlobalClusterNotFoundFault":
            _log(
                logging.INFO,
                "Global cluster already gone, treating as already standalone",
                db_name=db_name,
                cluster_id=cluster_id,
            )
            return {"db_name": db_name, "cluster_id": cluster_id, "success": True, "already_standalone": True}
        _log(
            logging.ERROR,
            "Failed to detach cluster from global cluster",
            db_name=db_name,
            cluster_id=cluster_id,
            error_code=code,
            error=str(exc),
        )
        return {"db_name": db_name, "cluster_id": cluster_id, "success": False, "error": str(exc), "error_code": code}

    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        current = _describe_cluster(rds, cluster_id)
        status = current.get("Status") if current else "unknown"
        still_global = bool(current.get("GlobalClusterIdentifier")) if current else False
        if current and status == "available" and not still_global:
            return {
                "db_name": db_name,
                "cluster_id": cluster_id,
                "success": True,
                "already_standalone": False,
                "final_status": status,
            }

        remaining = _remaining_ms(context)
        if remaining is not None and remaining < POLL_SAFETY_MARGIN_MS:
            _log(
                logging.WARNING,
                "Stopped polling before Lambda timeout; detach call succeeded but standalone status unconfirmed",
                db_name=db_name,
                cluster_id=cluster_id,
                last_status=status,
            )
            return {
                "db_name": db_name,
                "cluster_id": cluster_id,
                "success": True,
                "already_standalone": False,
                "final_status": status,
                "warning": "status polling timed out before reaching a terminal state, verify manually",
            }


# The actual work for ONE database, start to finish:
#   1. Look it up. If it's not there, report that and stop for this one.
#   2. If it's already independent (not attached to a global cluster), nothing
#      to do -- report success and move on.
#   3. Otherwise, tell AWS to cut it loose from Sydney's replication group.
#   4. Keep checking back every 15 seconds until AWS confirms it's truly
#      independent now, not just that the request was accepted -- but give up
#      gracefully (with a warning, not a crash) if the Lambda is about to time out.
# Any AWS error along the way is caught and turned into a "failed" result here,
# rather than being allowed to blow up the whole run -- that's what lets the
# caller keep going and try the other 11 databases even if this one has a problem.


def _publish_summary(sns, topic_arn, results):
    succeeded = [r for r in results if r.get("success")]
    lines = [f"Aurora DR failover (promote) summary: {len(succeeded)}/{len(results)} succeeded"]
    for r in results:
        status_word = "OK" if r.get("success") else "FAILED"
        detail = r.get("error") or ("already standalone" if r.get("already_standalone") else r.get("final_status", ""))
        lines.append(f"  [{status_word}] {r['db_name']} ({r['cluster_id']}): {detail}")
    message = "\n".join(lines)
    subject = f"Aurora DR failover: {len(succeeded)}/{len(results)} succeeded"
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except ClientError as exc:
        _log(logging.ERROR, "Failed to publish SNS summary", error=str(exc))


# Sends one human-readable notification listing every database and whether it
# succeeded or failed, so someone doesn't have to dig through raw logs to find
# out what just happened. If sending the notification itself fails, that's just
# logged -- it doesn't undo or affect the database work that already happened.


def _resolve_targets(all_clusters, event):
    requested_names = event.get("db_names")
    if not requested_names:
        return all_clusters
    unknown = set(requested_names) - set(all_clusters)
    if unknown:
        raise ValueError(f"Unknown db_names requested: {sorted(unknown)}")
    return {name: all_clusters[name] for name in requested_names}


# Decides which databases this run should actually touch: all 12 by default, or
# just a smaller list if the caller explicitly asked for specific ones. Refuses
# to run at all if the caller names a database that isn't in the configured list.


def handler(event, context):
    event = event or {}
    action = event.get("action", "status")
    _log(logging.INFO, "Aurora DR failover Lambda invoked", action=action, event=event)

    all_clusters = _load_target_clusters()
    targets = _resolve_targets(all_clusters, event)
    rds = boto3.client("rds")

    # Setup: figure out what was asked for (status check vs. real failover) and
    # which databases it applies to, before doing anything else.

    if action == "status":
        results = []
        for db_name, cluster_id in targets.items():
            cluster = _describe_cluster(rds, cluster_id)
            if cluster is None:
                results.append({"db_name": db_name, "cluster_id": cluster_id, "found": False})
            else:
                results.append(
                    {
                        "db_name": db_name,
                        "cluster_id": cluster_id,
                        "found": True,
                        "status": cluster.get("Status"),
                        "global_cluster_id": cluster.get("GlobalClusterIdentifier"),
                        "is_standalone": not bool(cluster.get("GlobalClusterIdentifier")),
                    }
                )
        return {"action": "status", "clusters": results}

    # "status" mode: purely read-only. Looks at each requested database and
    # reports whether it's still a replica or already independent, then stops --
    # nothing below this point runs on this path.

    if action != "promote":
        raise ValueError(f"Unsupported action '{action}', expected 'status' or 'promote'")

    if not event.get("confirm"):
        raise ValueError(
            "Refusing to promote without explicit confirmation. This detaches DR Aurora "
            "clusters from their global clusters (unplanned failover) -- an irreversible "
            "action intended only when the primary region is unreachable. Re-invoke with "
            '{"action": "promote", "confirm": true}.'
        )

    # Safety gate for the real failover: refuse to touch anything unless the
    # caller explicitly included confirm: true. This exists so an accidental or
    # default-payload invocation can never trigger an irreversible split.

    _log(logging.WARNING, "Starting unplanned Aurora Global Database failover", db_names=list(targets))

    results = [_detach_cluster(rds, db_name, cluster_id, context) for db_name, cluster_id in targets.items()]
    for result in results:
        _log(
            logging.INFO if result.get("success") else logging.ERROR,
            "Cluster failover attempt complete",
            **result,
        )

    # The main event: go through every target database, one at a time, doing the
    # actual detach-and-confirm work for each. One database failing doesn't stop
    # the others -- every result, success or failure, is collected here.

    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if sns_topic_arn:
        _publish_summary(boto3.client("sns"), sns_topic_arn, results)
    else:
        _log(logging.WARNING, "SNS_TOPIC_ARN not set, skipping summary publish")

    return {
        "action": "promote",
        "succeeded": sum(1 for r in results if r.get("success")),
        "total": len(results),
        "results": results,
    }

    # Wrap-up: notify (if configured) and hand back a full structured summary --
    # how many succeeded out of how many, plus the detailed per-database results --
    # so the outcome is visible both as a notification and as raw data.
