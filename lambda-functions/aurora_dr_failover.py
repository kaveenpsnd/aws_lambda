"""Aurora Global Database region switchover -- reverses which region accepts writes.

Promotes the Aurora cluster in a chosen region to full read/write PRIMARY and
demotes the current primary to a read-only REPLICA, in one atomic AWS
operation (SwitchoverGlobalCluster). Replication is never broken -- its
direction is reversed -- so the two regions stay in sync afterwards and the
operation can be run again in the opposite direction to fail back.

This REPLACES an earlier version of this script that called
RemoveFromGlobalCluster ("detach"). Detach promoted the DR cluster but
destroyed the Global Database in the process, leaving two unrelated clusters
that diverged permanently with no way to resync. Switchover is the correct
primitive: it promotes AND keeps replication, and it is symmetric.

Requires both regions to be healthy and reachable -- that is a property of
SwitchoverGlobalCluster itself, not a limitation of this script. A genuine
primary-region outage is a different operation (FailoverGlobalCluster with
AllowDataLoss) and is deliberately NOT implemented here.

Manual invoke only, no automatic trigger.

Event payload:
  {"action": "status"}
      Read-only. Reports, per database: which region is currently WRITER,
      which is REPLICA, replication lag, and the global writer endpoint.

  {"action": "switchover", "target_region": "ap-southeast-1", "confirm": true}
      Promotes ap-southeast-1 to read/write and demotes the current primary to
      a replica. Naming the region that is already primary is a safe no-op.
      Run it again with the other region to reverse.

Env vars:
  GLOBAL_CLUSTERS -- required. JSON object mapping short db name -> Aurora
    Global Database identifier, e.g.
    {"identitydb": "adu-psql-identitydb-rnd-global", ...}
  MAX_LAG_MS -- optional, default 60000. Preflight refuses to switch over when
    the target's replication lag exceeds this.
  SNS_TOPIC_ARN -- optional. Summary is skipped (with a log warning) if unset.
  LOG_LEVEL -- optional, default INFO.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

RETRYABLE_ERROR_CODES = {
    "InvalidGlobalClusterStateFault",
    "InvalidDBClusterStateFault",
    "ThrottlingException",
    "RequestLimitExceeded",
}

MAX_ATTEMPTS = 4
BASE_BACKOFF_SECONDS = 5
POLL_INTERVAL_SECONDS = 15
POLL_SAFETY_MARGIN_MS = 30_000
DEFAULT_MAX_LAG_MS = 60_000


def _log(level, message, **fields):
    logger.log(level, json.dumps({"message": message, **fields}, default=str))


_CLIENTS = {}


def _rds(region):
    """One cached RDS client per region.

    Switchover is inherently cross-region and the AWS API is picky about which
    region each call goes to (see _switchover), so a single default-region
    client is not enough -- this Lambda runs in the DR region but must be able
    to issue calls against the primary region too.
    """
    if region not in _CLIENTS:
        _CLIENTS[region] = boto3.client("rds", region_name=region)
    return _CLIENTS[region]


def _region_from_arn(arn):
    # arn:aws:rds:<region>:<account>:cluster:<identifier>
    return arn.split(":")[3]


def _cluster_id_from_arn(arn):
    return arn.split(":")[-1]


def _load_global_clusters():
    raw = os.environ.get("GLOBAL_CLUSTERS")
    if not raw:
        raise RuntimeError(
            "GLOBAL_CLUSTERS env var is required (JSON object of db_name -> global cluster identifier)"
        )
    try:
        clusters = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"GLOBAL_CLUSTERS is not valid JSON: {exc}") from exc
    if not isinstance(clusters, dict) or not clusters:
        raise RuntimeError("GLOBAL_CLUSTERS must be a non-empty JSON object")
    return clusters


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


def _remaining_ms(context):
    if context is None:
        return None
    try:
        return context.get_remaining_time_in_millis()
    except Exception:
        return None


def _describe_global_cluster(region, global_cluster_id):
    try:
        resp = _rds(region).describe_global_clusters(
            GlobalClusterIdentifier=global_cluster_id
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "GlobalClusterNotFoundFault":
            return None
        raise
    clusters = resp.get("GlobalClusters", [])
    return clusters[0] if clusters else None


def _find_global_cluster(global_cluster_id, hint_regions):
    """Locate a global cluster without knowing which region to ask.

    DescribeGlobalClusters is a regional API call even though a global cluster
    is a global object: it only answers in regions that host a member. Which
    regions those are is exactly what we are trying to discover, so try the
    caller's own region first and then any hints before giving up.
    """
    candidates = [os.environ.get("AWS_REGION")] + list(hint_regions)
    seen = set()
    for region in candidates:
        if not region or region in seen:
            continue
        seen.add(region)
        try:
            found = _describe_global_cluster(region, global_cluster_id)
        except ClientError as exc:
            _log(
                logging.WARNING,
                "DescribeGlobalClusters failed in region, trying next",
                region=region,
                global_cluster_id=global_cluster_id,
                error=str(exc),
            )
            continue
        if found:
            return found, region
    return None, None


def _members(global_cluster):
    """Normalise GlobalClusterMembers into the shape the rest of this script uses.

    IsWriter is the authoritative answer to "which region accepts writes right
    now" -- it is read fresh from AWS on every invocation, which is what makes
    this script direction-agnostic. Nothing here is configured with a notion of
    which side is "primary" or "DR".
    """
    out = []
    for member in global_cluster.get("GlobalClusterMembers", []):
        arn = member["DBClusterArn"]
        out.append(
            {
                "arn": arn,
                "region": _region_from_arn(arn),
                "cluster_id": _cluster_id_from_arn(arn),
                "is_writer": bool(member.get("IsWriter")),
            }
        )
    return out


def _writer(members):
    return next((m for m in members if m["is_writer"]), None)


def _member_in_region(members, region):
    return next((m for m in members if m["region"] == region), None)


def _describe_db_cluster(region, cluster_id):
    try:
        resp = _rds(region).describe_db_clusters(DBClusterIdentifier=cluster_id)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "DBClusterNotFoundFault":
            return None
        raise
    clusters = resp.get("DBClusters", [])
    return clusters[0] if clusters else None


def _rpo_lag_ms(region, cluster_id):
    """Replication lag for a secondary, in milliseconds, or None.

    AuroraGlobalDBRPOLag is only published for SECONDARY clusters, in the
    secondary's own region -- there is no such metric on the writer, so None
    here means "this is the writer" or "no datapoint yet", not an error.
    """
    try:
        cw = boto3.client("cloudwatch", region_name=region)
        now = datetime.now(timezone.utc)
        resp = cw.get_metric_statistics(
            Namespace="AWS/RDS",
            MetricName="AuroraGlobalDBRPOLag",
            Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
            StartTime=now - timedelta(minutes=5),
            EndTime=now,
            Period=60,
            Statistics=["Average"],
        )
    except ClientError as exc:
        _log(
            logging.WARNING,
            "Could not read replication lag metric",
            region=region,
            cluster_id=cluster_id,
            error=str(exc),
        )
        return None
    points = sorted(resp.get("Datapoints", []), key=lambda p: p["Timestamp"])
    return int(points[-1]["Average"]) if points else None


def _member_detail(member, include_lag=True):
    """Full read/write picture for one member cluster.

    `access` is the field to read: it collapses the AWS-side flags into the
    plain answer -- can this region take writes right now, or not.
    """
    cluster = _describe_db_cluster(member["region"], member["cluster_id"])
    status = cluster.get("Status") if cluster else "not-found"
    instances = (cluster or {}).get("DBClusterMembers", [])
    detail = {
        "region": member["region"],
        "cluster_id": member["cluster_id"],
        "role": "WRITER" if member["is_writer"] else "REPLICA",
        "access": "read-write" if member["is_writer"] else "read-only",
        "status": status,
        # Only ever true on the current primary -- a read-only secondary has
        # instances but none of them is the cluster writer.
        "has_writer_instance": any(i.get("IsClusterWriter") for i in instances),
        "instance_count": len(instances),
        "cluster_endpoint": (cluster or {}).get("Endpoint"),
        "reader_endpoint": (cluster or {}).get("ReaderEndpoint"),
    }
    if include_lag and not member["is_writer"]:
        detail["rpo_lag_ms"] = _rpo_lag_ms(member["region"], member["cluster_id"])
    return detail


def _snapshot(db_name, global_cluster_id, global_cluster, include_lag=True):
    members = _members(global_cluster)
    writer = _writer(members)
    details = [_member_detail(m, include_lag=include_lag) for m in members]
    result = {
        "db_name": db_name,
        "global_cluster_id": global_cluster_id,
        "global_cluster_status": global_cluster.get("Status"),
        # Stable DNS name that always resolves to whichever region is currently
        # the writer. Point applications here and a switchover needs no config
        # change. Terraform's aws_rds_global_cluster does not expose this at the
        # provider version this fork pins, which is why it is surfaced here.
        "global_writer_endpoint": global_cluster.get("Endpoint"),
        "writer_region": writer["region"] if writer else None,
        "engine_version": global_cluster.get("EngineVersion"),
        "members": details,
    }
    failover_state = global_cluster.get("FailoverState")
    if failover_state:
        result["in_progress"] = {
            "status": failover_state.get("Status"),
            "from_region": _region_from_arn(failover_state["FromDbClusterArn"])
            if failover_state.get("FromDbClusterArn")
            else None,
            "to_region": _region_from_arn(failover_state["ToDbClusterArn"])
            if failover_state.get("ToDbClusterArn")
            else None,
        }
    return result


def _preflight(global_cluster, target_region, max_lag_ms):
    """Refuse the switchover unless every precondition AWS requires is met.

    SwitchoverGlobalCluster fails outright on an unhealthy or lagging cluster,
    but it can fail partway through a multi-database run. Checking first means
    a bad run is rejected before anything changes, rather than leaving some
    databases switched and others not.
    """
    members = _members(global_cluster)
    problems = []

    writer = _writer(members)
    if writer is None:
        problems.append("no member is currently the writer")

    target = _member_in_region(members, target_region)
    if target is None:
        problems.append(
            f"no member cluster in {target_region} "
            f"(members: {sorted(m['region'] for m in members)})"
        )
        return {"ok": False, "problems": problems, "already_primary": False}

    if writer and target["arn"] == writer["arn"]:
        return {"ok": True, "problems": [], "already_primary": True, "target": target}

    status = global_cluster.get("Status")
    if status != "available":
        problems.append(f"global cluster status is '{status}', expected 'available'")

    versions = set()
    for member in members:
        cluster = _describe_db_cluster(member["region"], member["cluster_id"])
        if cluster is None:
            problems.append(f"{member['region']}: cluster not found")
            continue
        if cluster.get("Status") != "available":
            problems.append(
                f"{member['region']}: cluster status is '{cluster.get('Status')}', expected 'available'"
            )
        # A headless cluster (zero instances) cannot be switched over to -- AWS
        # requires an instance to promote. Note this is an instance-COUNT check,
        # not an IsClusterWriter check: on a read-only secondary no instance is
        # ever the cluster writer, so testing IsClusterWriter here would reject
        # every legitimate switchover.
        if not cluster.get("DBClusterMembers"):
            problems.append(f"{member['region']}: cluster is headless, no instance to promote")
        versions.add(cluster.get("EngineVersion"))

    # Managed switchover requires identical major AND minor engine versions on
    # both sides. A minor upgrade applied to one region and not the other
    # silently breaks switchover, so surface it as a precondition rather than
    # letting AWS reject the call with a less obvious error.
    if len(versions) > 1:
        problems.append(f"engine versions differ across regions: {sorted(versions)}")

    lag = _rpo_lag_ms(target["region"], target["cluster_id"])
    if lag is not None and lag > max_lag_ms:
        problems.append(f"{target['region']}: replication lag {lag}ms exceeds {max_lag_ms}ms")

    return {
        "ok": not problems,
        "problems": problems,
        "already_primary": False,
        "target": target,
        "target_lag_ms": lag,
    }


def _initiate_switchover(api_region, global_cluster_id, target_arn):
    """Start the switchover. AWS does the promotion and the demotion together.

    Two API details that are easy to get wrong:
      - the call must be issued in the CURRENT PRIMARY's region, not the
        target's (failover, which this script does not implement, is the
        opposite);
      - TargetDbClusterIdentifier must be the target's full ARN, not its
        cluster identifier.
    """
    return _call_with_retry(
        lambda: _rds(api_region).switchover_global_cluster(
            GlobalClusterIdentifier=global_cluster_id,
            TargetDbClusterIdentifier=target_arn,
        ),
        description=f"switchover_global_cluster:{global_cluster_id}",
    )


def _promotion_complete(global_cluster, target_arn):
    """True once the target genuinely holds read/write, not just once AWS accepted the call.

    All four conditions matter: the global cluster has settled, the target owns
    the writer role, it has a real writer instance behind it, and the demoted
    side has come back as a healthy replica rather than being left broken.
    """
    if global_cluster.get("Status") != "available":
        return False, "global cluster still settling"
    if global_cluster.get("FailoverState"):
        return False, "switchover still in progress"

    members = _members(global_cluster)
    writer = _writer(members)
    if writer is None or writer["arn"] != target_arn:
        return False, "target is not the writer yet"

    for member in members:
        cluster = _describe_db_cluster(member["region"], member["cluster_id"])
        if cluster is None or cluster.get("Status") != "available":
            return False, f"{member['region']} not available yet"
        if member["arn"] == target_arn and not any(
            m.get("IsClusterWriter") for m in cluster.get("DBClusterMembers", [])
        ):
            return False, "promoted cluster has no writer instance yet"

    return True, "verified"


def _publish_summary(sns, topic_arn, target_region, results):
    succeeded = [r for r in results if r.get("success")]
    lines = [
        f"Aurora switchover to {target_region}: {len(succeeded)}/{len(results)} succeeded"
    ]
    for r in results:
        status_word = "OK" if r.get("success") else "FAILED"
        if r.get("already_primary"):
            detail = f"already primary in {target_region}, no change"
        elif r.get("error"):
            detail = r["error"]
        else:
            writer = r.get("writer_region")
            replica = ", ".join(
                m["region"] for m in r.get("members", []) if m["role"] == "REPLICA"
            )
            detail = f"WRITER={writer} REPLICA={replica} ({r.get('verified_state', 'unverified')})"
        lines.append(f"  [{status_word}] {r['db_name']}: {detail}")
    message = "\n".join(lines)
    subject = f"Aurora switchover to {target_region}: {len(succeeded)}/{len(results)} succeeded"
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except ClientError as exc:
        _log(logging.ERROR, "Failed to publish SNS summary", error=str(exc))


def _resolve_targets(all_clusters, event):
    requested_names = event.get("db_names")
    if not requested_names:
        return all_clusters
    unknown = set(requested_names) - set(all_clusters)
    if unknown:
        raise ValueError(f"Unknown db_names requested: {sorted(unknown)}")
    return {name: all_clusters[name] for name in requested_names}


def _handle_status(targets):
    results = []
    for db_name, global_cluster_id in targets.items():
        global_cluster, _ = _find_global_cluster(global_cluster_id, [])
        if global_cluster is None:
            results.append(
                {"db_name": db_name, "global_cluster_id": global_cluster_id, "found": False}
            )
            continue
        snapshot = _snapshot(db_name, global_cluster_id, global_cluster)
        snapshot["found"] = True
        results.append(snapshot)
    return {"action": "status", "databases": results}


def _handle_switchover(targets, event, context):
    target_region = event.get("target_region")
    if not target_region:
        raise ValueError(
            'target_region is required, e.g. {"action": "switchover", '
            '"target_region": "ap-southeast-1", "confirm": true}'
        )
    if not event.get("confirm"):
        raise ValueError(
            "Refusing to switch over without explicit confirmation. This promotes the "
            f"Aurora cluster in {target_region} to read/write and demotes the current "
            "primary to a read-only replica. Replication is preserved and the operation "
            "is reversible, but writes move region. Re-invoke with "
            f'{{"action": "switchover", "target_region": "{target_region}", "confirm": true}}.'
        )

    max_lag_ms = int(os.environ.get("MAX_LAG_MS", DEFAULT_MAX_LAG_MS))

    # Phase 1 -- preflight everything, then initiate everything, before waiting
    # on anything. Each switchover takes minutes, so starting them all up front
    # means the waits overlap instead of stacking up serially.
    pending = []
    results = []
    for db_name, global_cluster_id in targets.items():
        global_cluster, api_region = _find_global_cluster(global_cluster_id, [target_region])
        if global_cluster is None:
            results.append(
                {
                    "db_name": db_name,
                    "global_cluster_id": global_cluster_id,
                    "success": False,
                    "error": "global cluster not found",
                }
            )
            continue

        check = _preflight(global_cluster, target_region, max_lag_ms)
        if check["already_primary"]:
            snapshot = _snapshot(db_name, global_cluster_id, global_cluster)
            snapshot.update({"success": True, "already_primary": True, "verified_state": "verified"})
            results.append(snapshot)
            _log(logging.INFO, "Already primary in target region, nothing to do",
                 db_name=db_name, target_region=target_region)
            continue
        if not check["ok"]:
            results.append(
                {
                    "db_name": db_name,
                    "global_cluster_id": global_cluster_id,
                    "success": False,
                    "error": "preflight failed: " + "; ".join(check["problems"]),
                }
            )
            _log(logging.ERROR, "Preflight refused switchover",
                 db_name=db_name, problems=check["problems"])
            continue

        writer = _writer(_members(global_cluster))
        target = check["target"]
        try:
            _initiate_switchover(writer["region"], global_cluster_id, target["arn"])
        except ClientError as exc:
            results.append(
                {
                    "db_name": db_name,
                    "global_cluster_id": global_cluster_id,
                    "success": False,
                    "error": str(exc),
                    "error_code": exc.response.get("Error", {}).get("Code", ""),
                }
            )
            _log(logging.ERROR, "Switchover call failed", db_name=db_name, error=str(exc))
            continue

        _log(
            logging.WARNING,
            "Switchover initiated",
            db_name=db_name,
            from_region=writer["region"],
            to_region=target["region"],
            lag_ms=check.get("target_lag_ms"),
        )
        pending.append(
            {
                "db_name": db_name,
                "global_cluster_id": global_cluster_id,
                "target_arn": target["arn"],
                "api_region": api_region,
                "from_region": writer["region"],
            }
        )

    # Phase 2 -- poll until each promotion is genuinely complete: the target
    # holds read/write and the old primary has come back as a healthy replica.
    while pending:
        time.sleep(POLL_INTERVAL_SECONDS)
        still_pending = []
        for item in pending:
            global_cluster, _ = _find_global_cluster(
                item["global_cluster_id"], [item["api_region"], target_region]
            )
            if global_cluster is None:
                results.append({**item, "success": False, "error": "global cluster disappeared"})
                continue
            done, reason = _promotion_complete(global_cluster, item["target_arn"])
            if done:
                snapshot = _snapshot(item["db_name"], item["global_cluster_id"], global_cluster)
                snapshot.update(
                    {"success": True, "already_primary": False,
                     "from_region": item["from_region"], "verified_state": "verified"}
                )
                results.append(snapshot)
                _log(logging.INFO, "Switchover verified complete",
                     db_name=item["db_name"], writer_region=snapshot["writer_region"])
            else:
                still_pending.append(item)
        pending = still_pending

        remaining = _remaining_ms(context)
        if pending and remaining is not None and remaining < POLL_SAFETY_MARGIN_MS:
            for item in pending:
                global_cluster, _ = _find_global_cluster(
                    item["global_cluster_id"], [item["api_region"], target_region]
                )
                snapshot = (
                    _snapshot(item["db_name"], item["global_cluster_id"], global_cluster)
                    if global_cluster
                    else {"db_name": item["db_name"]}
                )
                snapshot.update(
                    {
                        "success": True,
                        "verified_state": "unverified",
                        "warning": (
                            "switchover was accepted by AWS but had not finished before this "
                            "Lambda ran out of time -- re-invoke with {\"action\": \"status\"} "
                            "to confirm"
                        ),
                    }
                )
                results.append(snapshot)
                _log(logging.WARNING, "Stopped polling before Lambda timeout",
                     db_name=item["db_name"])
            pending = []

    return results


def handler(event, context):
    event = event or {}
    action = event.get("action", "status")
    _log(logging.INFO, "Aurora switchover Lambda invoked", action=action, event=event)

    all_clusters = _load_global_clusters()
    targets = _resolve_targets(all_clusters, event)

    if action == "status":
        return _handle_status(targets)

    if action != "switchover":
        raise ValueError(
            f"Unsupported action '{action}', expected 'status' or 'switchover'. "
            "Note: the old 'promote' action (detach from global cluster) was removed -- "
            "it broke replication permanently. Use 'switchover', which promotes the target "
            "to read/write AND keeps replication, reversed."
        )

    results = _handle_switchover(targets, event, context)

    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if sns_topic_arn:
        _publish_summary(
            boto3.client("sns"), sns_topic_arn, event.get("target_region"), results
        )
    else:
        _log(logging.WARNING, "SNS_TOPIC_ARN not set, skipping summary publish")

    return {
        "action": "switchover",
        "target_region": event.get("target_region"),
        "succeeded": sum(1 for r in results if r.get("success")),
        "total": len(results),
        "results": results,
    }
