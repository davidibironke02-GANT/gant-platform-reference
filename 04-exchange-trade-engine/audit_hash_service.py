# 4.8 SHA-256 Audit Hash Service
# Lambda function computing and replicating cryptographic event hashes
# Five anchor points per trade: QC2_VVI, ESCROW_CREATED, LOADED_EXW,
# DELIVERED, COMPLETED
# Replicates to Aurora (primary), IBM Cloud Object Storage (secondary),
# Cloudflare R2 (tertiary)

import json
import hashlib
import logging
import boto3
import ibm_boto3
import requests
from datetime import datetime, timezone
from ibm_botocore.client import Config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client  = boto3.client("rds-data")
events_client  = boto3.client("events")
ssm_client     = boto3.client("ssm")

# ── Configuration ────────────────────────────────────────────────
AURORA_ARN      = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET   = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB       = "gant_platform"
EVENT_BUS       = "gant-platform"
RETRY_QUEUE_URL = ssm_client.get_parameter(
    Name="/gant/queues/hash_retry")["Parameter"]["Value"]

IBM_COS_ENDPOINT = ssm_client.get_parameter(
    Name="/gant/ibm/cos_endpoint")["Parameter"]["Value"]
IBM_API_KEY      = ssm_client.get_parameter(
    Name="/gant/ibm/api_key", WithDecryption=True)["Parameter"]["Value"]
IBM_COS_BUCKET   = "gant-audit-hashes"

CF_R2_ENDPOINT   = ssm_client.get_parameter(
    Name="/gant/cloudflare/r2_endpoint")["Parameter"]["Value"]
CF_ACCESS_KEY    = ssm_client.get_parameter(
    Name="/gant/cloudflare/access_key", WithDecryption=True)["Parameter"]["Value"]
CF_SECRET_KEY    = ssm_client.get_parameter(
    Name="/gant/cloudflare/secret_key", WithDecryption=True)["Parameter"]["Value"]
CF_R2_BUCKET     = "gant-audit-hashes"

VALID_EVENT_TYPES = {
    "QC2_VVI",
    "ESCROW_CREATED",
    "LOADED_EXW",
    "DELIVERED",
    "COMPLETED",
}


# ── IBM Cloud Object Storage client ──────────────────────────────
def get_ibm_cos_client():
    return ibm_boto3.client(
        "s3",
        ibm_api_key_id=IBM_API_KEY,
        ibm_service_instance_id="gant-cos-instance",
        config=Config(signature_version="oauth"),
        endpoint_url=IBM_COS_ENDPOINT,
    )


# ── Cloudflare R2 client ─────────────────────────────────────────
def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=CF_R2_ENDPOINT,
        aws_access_key_id=CF_ACCESS_KEY,
        aws_secret_access_key=CF_SECRET_KEY,
        region_name="auto",
    )


# ── Hash computation ─────────────────────────────────────────────
def compute_hash(payload: dict) -> str:
    """
    Serialise payload to canonical JSON with sorted keys and
    compute SHA-256 digest. Deterministic -- identical inputs
    always produce identical output.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ── Aurora write ─────────────────────────────────────────────────
def write_to_aurora(hash_record: dict) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO audit_hashes (
                event_type, reference_id, payload_hash_sha256,
                platform_timestamp, replicated_ibm, replicated_r2
            ) VALUES (
                :event_type, :reference_id, :payload_hash,
                :platform_timestamp, false, false
            )
        """,
        parameters=[
            {"name": "event_type",          "value": {"stringValue": hash_record["event_type"]}},
            {"name": "reference_id",        "value": {"stringValue": hash_record["reference_id"]}},
            {"name": "payload_hash",        "value": {"stringValue": hash_record["payload_hash_sha256"]}},
            {"name": "platform_timestamp",  "value": {"stringValue": hash_record["platform_timestamp"]}},
        ],
    )


# ── IBM Cloud Object Storage write ───────────────────────────────
def replicate_to_ibm(hash_record: dict) -> None:
    cos = get_ibm_cos_client()
    key = (
        f"{hash_record['event_type']}/"
        f"{hash_record['reference_id']}/"
        f"{hash_record['platform_timestamp']}.json"
    )
    cos.put_object(
        Bucket=IBM_COS_BUCKET,
        Key=key,
        Body=json.dumps(hash_record).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Replicated hash to IBM COS: {key}")


# ── Cloudflare R2 write ──────────────────────────────────────────
def replicate_to_r2(hash_record: dict) -> None:
    r2 = get_r2_client()
    key = (
        f"{hash_record['event_type']}/"
        f"{hash_record['reference_id']}/"
        f"{hash_record['platform_timestamp']}.json"
    )
    r2.put_object(
        Bucket=CF_R2_BUCKET,
        Key=key,
        Body=json.dumps(hash_record).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Replicated hash to Cloudflare R2: {key}")


# ── Retry queue routing ──────────────────────────────────────────
def route_to_retry_queue(hash_record: dict, error: str) -> None:
    sqs = boto3.client("sqs")
    sqs.send_message(
        QueueUrl=RETRY_QUEUE_URL,
        MessageBody=json.dumps({
            "hash_record": hash_record,
            "error":       error,
            "queued_at":   datetime.now(timezone.utc).isoformat(),
        }),
    )
    logger.warning(
        f"Hash replication failure routed to retry queue: {error}"
    )


# ── Lambda handler ───────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by EventBridge at each of five defined trade anchor points.
    Computes SHA-256 hash of the event payload and replicates the hash
    record to three independent stores. Replication failures route to
    retry queue -- hash service unavailability never blocks the parent
    trade state transition.

    Expected event format:
    {
        "event_type":   "QC2_VVI" | "ESCROW_CREATED" | "LOADED_EXW"
                        | "DELIVERED" | "COMPLETED",
        "reference_id": "TRD-20260426-0012" | "LENT-0045364-260426-01",
        "payload":      { ...event data dict... }
    }
    """
    event_type   = event.get("event_type")
    reference_id = event.get("reference_id")
    payload      = event.get("payload", {})

    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Invalid event_type: {event_type}")
    if not reference_id:
        raise ValueError("reference_id is required")

    platform_timestamp = datetime.now(timezone.utc).isoformat()
    payload_hash       = compute_hash(payload)

    hash_record = {
        "event_type":          event_type,
        "reference_id":        reference_id,
        "payload_hash_sha256": payload_hash,
        "platform_timestamp":  platform_timestamp,
    }

    # Primary write -- Aurora. This must succeed.
    # If Aurora is unavailable the Lambda fails and the
    # EventBridge rule retries automatically.
    write_to_aurora(hash_record)
    logger.info(
        f"Hash written to Aurora: {event_type} / {reference_id}"
    )

    # Secondary and tertiary writes -- IBM and R2.
    # Failures route to retry queue, never block the trade event.
    ibm_ok = r2_ok = True

    try:
        replicate_to_ibm(hash_record)
    except Exception as e:
        ibm_ok = False
        route_to_retry_queue(
            {**hash_record, "target": "IBM_COS"}, str(e)
        )

    try:
        replicate_to_r2(hash_record)
    except Exception as e:
        r2_ok = False
        route_to_retry_queue(
            {**hash_record, "target": "CF_R2"}, str(e)
        )

    # Publish HASH_COMMITTED to EventBridge regardless of
    # secondary replication status
    events_client.put_events(Entries=[{
        "Source":       "gant.audit",
        "DetailType":   "HASH_COMMITTED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "event_type":          event_type,
            "reference_id":        reference_id,
            "payload_hash_sha256": payload_hash,
            "platform_timestamp":  platform_timestamp,
            "replicated_ibm":      ibm_ok,
            "replicated_r2":       r2_ok,
        }),
    }])

    return {
        "status":              "HASH_COMMITTED",
        "event_type":          event_type,
        "reference_id":        reference_id,
        "payload_hash_sha256": payload_hash,
        "replicated_ibm":      ibm_ok,
        "replicated_r2":       r2_ok,
    }
