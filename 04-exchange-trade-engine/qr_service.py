# 4.6 QR Service
# Lambda functions managing the dynamic QR record lifecycle
# QR record accumulates verified data at five defined lifecycle points
# Three access tiers: PUBLIC, BUYER, CONTROLLED
# Physical label printed only at BAGGED_AND_RACKED state

import json
import logging
import secrets
import boto3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client = boto3.client("rds-data")
events_client = boto3.client("events")
ssm_client    = boto3.client("ssm")

AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"
EVENT_BUS     = "gant-platform"

# ── Valid update stages in lifecycle order ───────────────────────
VALID_STAGES = {
    "QC1_COMPLETE",
    "QC2_COMPLETE",
    "RACKED",
    "LOADED_EXW",
    "DSRE_UPDATE",
    "DELIVERED",
}

# ── Access tier definitions ───────────────────────────────────────
# PUBLIC: compliance status and shipment integrity hash only
# BUYER:  full quality record, compliance docs, DSRE timeline
# CONTROLLED: full audit log including all input/output records
ACCESS_TIERS = {"PUBLIC", "BUYER", "CONTROLLED"}


# ── 1. Create QR record at QC1 ───────────────────────────────────
def create_qr_record(event: dict, context) -> dict:
    """
    Triggered at QC1 completion when BatchID is first generated.
    Creates the QR record in Aurora with status QC1_COMPLETE
    and writes the intake snapshot. Generates a unique QR token
    tied to the BatchID. Physical label is not printed at this
    stage -- the record exists digitally only until BAGGED_AND_RACKED.

    Expected event format:
    {
        "batch_id":      "LENT-0045364-260426-01",
        "qc1_snapshot":  {
            "gross_weight_kg":      24600.0,
            "tare_weight_kg":       5000.0,
            "net_weight_kg":        19600.0,
            "moisture_pct":         13.2,
            "protein_pct":          24.1,
            "foreign_material_pct": 0.42,
            "intake_timestamp":     "2026-04-26T08:14:00Z"
        }
    }
    """
    batch_id     = event["batch_id"]
    qc1_snapshot = event["qc1_snapshot"]

    # Generate cryptographically secure QR token
    qr_token   = secrets.token_urlsafe(32)
    created_at = datetime.now(timezone.utc).isoformat()

    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO qr_records (
                qr_token, batch_id, status,
                qc1_snapshot, created_at, updated_at
            ) VALUES (
                :qr_token, :batch_id, 'QC1_COMPLETE',
                :qc1_snapshot, :created_at, :created_at
            )
        """,
        parameters=[
            {"name": "qr_token",     "value": {"stringValue": qr_token}},
            {"name": "batch_id",     "value": {"stringValue": batch_id}},
            {"name": "qc1_snapshot", "value": {"stringValue": json.dumps(qc1_snapshot)}},
            {"name": "created_at",   "value": {"stringValue": created_at}},
        ],
    )

    logger.info(f"QR record created for BatchID {batch_id}")

    return {
        "status":     "QR_RECORD_CREATED",
        "batch_id":   batch_id,
        "qr_token":   qr_token,
        "created_at": created_at,
    }


# ── 2. Update QR record at each lifecycle stage ──────────────────
def update_qr_record(event: dict, context) -> dict:
    """
    Appends new verified data to the QR record at each of the
    defined lifecycle update points. Each update is timestamped
    and appended -- the record is never overwritten, only extended.
    Publishes QR_UPDATED event to EventBridge on completion.

    Expected event format:
    {
        "batch_id":       "LENT-0045364-260426-01",
        "update_stage":   "QC2_COMPLETE" | "RACKED" | "LOADED_EXW"
                          | "DSRE_UPDATE" | "DELIVERED",
        "update_payload": { ...stage-specific data dict... }
    }
    """
    batch_id       = event["batch_id"]
    update_stage   = event["update_stage"]
    update_payload = event["update_payload"]

    if update_stage not in VALID_STAGES:
        raise ValueError(
            f"Invalid update_stage '{update_stage}'. "
            f"Valid stages: {sorted(VALID_STAGES)}"
        )

    updated_at = datetime.now(timezone.utc).isoformat()

    # Map update stage to the correct Aurora column
    stage_column_map = {
        "QC2_COMPLETE": "qc2_snapshot",
        "RACKED":       "rack_location",
        "LOADED_EXW":   "loaded_exw_payload",
        "DSRE_UPDATE":  "dsre_updates",
        "DELIVERED":    "delivery_payload",
    }
    column = stage_column_map.get(update_stage)

    # DSRE updates append to a JSONB array rather than replacing
    if update_stage == "DSRE_UPDATE":
        aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql="""
                UPDATE qr_records SET
                    dsre_updates = COALESCE(dsre_updates, '[]'::jsonb)
                        || :new_entry::jsonb,
                    status     = :status,
                    updated_at = :updated_at
                WHERE batch_id = :batch_id
            """,
            parameters=[
                {"name": "new_entry",  "value": {"stringValue": json.dumps(
                    {**update_payload, "appended_at": updated_at})}},
                {"name": "status",     "value": {"stringValue": update_stage}},
                {"name": "updated_at", "value": {"stringValue": updated_at}},
                {"name": "batch_id",   "value": {"stringValue": batch_id}},
            ],
        )
    else:
        aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql=f"""
                UPDATE qr_records SET
                    {column}   = :payload,
                    status     = :status,
                    updated_at = :updated_at
                WHERE batch_id = :batch_id
            """,
            parameters=[
                {"name": "payload",    "value": {"stringValue": json.dumps(update_payload)}},
                {"name": "status",     "value": {"stringValue": update_stage}},
                {"name": "updated_at", "value": {"stringValue": updated_at}},
                {"name": "batch_id",   "value": {"stringValue": batch_id}},
            ],
        )

    events_client.put_events(Entries=[{
        "Source":       "gant.qr",
        "DetailType":   "QR_UPDATED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "batch_id":     batch_id,
            "update_stage": update_stage,
            "updated_at":   updated_at,
        }),
    }])

    logger.info(
        f"QR record updated for BatchID {batch_id} "
        f"at stage {update_stage}"
    )

    return {
        "status":       "QR_UPDATED",
        "batch_id":     batch_id,
        "update_stage": update_stage,
        "updated_at":   updated_at,
    }


# ── 3. Resolve QR access by tier ─────────────────────────────────
def resolve_qr_access(event: dict, context) -> dict:
    """
    Resolves a QR token scan to the appropriate data payload
    based on the caller's access tier. Tier is determined from
    the authentication context provided by API Gateway.

    PUBLIC:     Compliance status indicator and SHA-256 audit
                hash proof only. No quality data. No identity.
    BUYER:      Full quality record, VVI breakdown, compliance
                document references, DSRE transit timeline.
                Requires valid TradeID binding to the BatchID.
    CONTROLLED: Full audit payload including all input and output
                logs. Time-bound single-use token issued to
                auditors and regulators only.

    Expected event format:
    {
        "qr_token":    "abc123...",
        "access_tier": "PUBLIC" | "BUYER" | "CONTROLLED",
        "trade_id":    "TRD-20260426-0012"   (required for BUYER)
    }
    """
    qr_token    = event["qr_token"]
    access_tier = event.get("access_tier", "PUBLIC").upper()
    trade_id    = event.get("trade_id")

    if access_tier not in ACCESS_TIERS:
        raise ValueError(
            f"Invalid access_tier '{access_tier}'. "
            f"Valid tiers: {sorted(ACCESS_TIERS)}"
        )

    # ── Fetch QR record ───────────────────────────────────────────
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT
                qr.batch_id,
                qr.status,
                qr.qc2_snapshot,
                qr.loaded_exw_payload,
                qr.dsre_updates,
                qr.delivery_payload,
                b.vvi_score,
                b.quality_score,
                b.submetric_scores,
                b.ai_recommended_price,
                b.crop_code
            FROM qr_records qr
            JOIN batches b ON b.batch_id = qr.batch_id
            WHERE qr.qr_token = :qr_token
        """,
        parameters=[{
            "name": "qr_token",
            "value": {"stringValue": qr_token}
        }],
    )

    if not result["records"]:
        raise ValueError(f"QR token not found: {qr_token}")

    row = result["records"][0]

    batch_id        = row[0]["stringValue"]
    status          = row[1]["stringValue"]
    qc2_snapshot    = json.loads(row[2]["stringValue"]) if not row[2].get("isNull") else None
    loaded_payload  = json.loads(row[3]["stringValue"]) if not row[3].get("isNull") else None
    dsre_updates    = json.loads(row[4]["stringValue"]) if not row[4].get("isNull") else []
    delivery        = json.loads(row[5]["stringValue"]) if not row[5].get("isNull") else None
    vvi_score       = row[6]["doubleValue"] if not row[6].get("isNull") else None
    quality_score   = row[7]["doubleValue"] if not row[7].get("isNull") else None
    submetric_scores= json.loads(row[8]["stringValue"]) if not row[8].get("isNull") else None
    ai_price        = row[9]["doubleValue"] if not row[9].get("isNull") else None
    crop_code       = row[10]["stringValue"]

    # ── Fetch SHA-256 audit hash for public integrity proof ───────
    hash_result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT payload_hash_sha256, platform_timestamp
            FROM audit_hashes
            WHERE reference_id = :batch_id
            ORDER BY platform_timestamp DESC
            LIMIT 1
        """,
        parameters=[{
            "name": "batch_id",
            "value": {"stringValue": batch_id}
        }],
    )
    audit_hash = None
    if hash_result["records"]:
        audit_hash = {
            "sha256":    hash_result["records"][0][0]["stringValue"],
            "timestamp": hash_result["records"][0][1]["stringValue"],
        }

    # ── PUBLIC response ───────────────────────────────────────────
    if access_tier == "PUBLIC":
        return {
            "access_tier":        "PUBLIC",
            "batch_id":           batch_id,
            "crop_code":          crop_code,
            "record_status":      status,
            "compliance_status":  "EXPORT_READY" if loaded_payload else "PENDING",
            "audit_hash_proof":   audit_hash,
        }

    # ── BUYER response ────────────────────────────────────────────
    if access_tier == "BUYER":
        if not trade_id:
            raise ValueError(
                "trade_id is required for BUYER tier access"
            )
        # Verify trade_id is bound to this batch_id
        trade_check = aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql="""
                SELECT trade_id FROM trades
                WHERE trade_id = :trade_id
                AND batch_id   = :batch_id
            """,
            parameters=[
                {"name": "trade_id", "value": {"stringValue": trade_id}},
                {"name": "batch_id", "value": {"stringValue": batch_id}},
            ],
        )
        if not trade_check["records"]:
            raise PermissionError(
                f"TradeID '{trade_id}' is not bound to "
                f"BatchID '{batch_id}'"
            )
        return {
            "access_tier":           "BUYER",
            "batch_id":              batch_id,
            "trade_id":              trade_id,
            "crop_code":             crop_code,
            "record_status":         status,
            "vvi_score":             vvi_score,
            "quality_score":         quality_score,
            "submetric_scores":      submetric_scores,
            "qc2_snapshot":          qc2_snapshot,
            "compliance_documents":  loaded_payload,
            "dsre_transit_timeline": dsre_updates,
            "delivery":              delivery,
            "audit_hash_proof":      audit_hash,
        }

    # ── CONTROLLED response ───────────────────────────────────────
    # Full audit payload -- all input and output records
    # Token validation for controlled access happens at API Gateway
    # before this Lambda is invoked
    return {
        "access_tier":           "CONTROLLED",
        "batch_id":              batch_id,
        "crop_code":             crop_code,
        "record_status":         status,
        "vvi_score":             vvi_score,
        "quality_score":         quality_score,
        "submetric_scores":      submetric_scores,
        "qc2_snapshot":          qc2_snapshot,
        "ai_recommended_price":  ai_price,
        "compliance_documents":  loaded_payload,
        "dsre_transit_timeline": dsre_updates,
        "delivery":              delivery,
        "audit_hash_proof":      audit_hash,
    }
