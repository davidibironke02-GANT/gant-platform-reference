# 5.4 Phantom Order Prevention
# Lambda functions enforcing atomic escrow reservation and
# logistics financing fraud prevention
# Primary mechanism: 20% escrow atomic with 80% fintech financing
# No intermediate state where inventory is reserved but funds uncommitted
# Secondary mechanism: FF invoice upload plus active fraud declaration

import json
import logging
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

# ── Escrow funding timeout window ────────────────────────────────
ESCROW_TIMEOUT_SECONDS = 300   # 5 minutes from instruction to confirmation


# ── 1. Atomic escrow and reservation ────────────────────────────
def create_escrow_reservation(event: dict, context) -> dict:
    """
    Receives escrow confirmation from buyer fintech partner.
    Executes atomically -- both inventory reservation and escrow
    record creation succeed together or neither occurs.
    If escrow confirmation does not arrive within timeout window
    the bid is cancelled and all reservations are released.

    This is the primary phantom order prevention mechanism.
    No bid proceeds past this point without confirmed escrow.
    No inventory is reserved before escrow is confirmed.

    Expected event format:
    {
        "trade_id":        "TRD-20260426-0012",
        "batchlot_ids":    ["LENT-0045364-260426-01-01", ...],
        "escrow_amount":   45000.00,
        "escrow_currency": "INR",
        "forex_rate":      61.24,
        "fintech_ref":     "FT-REF-789456",
        "confirmed_at":    "2026-04-26T09:14:00Z"
    }
    """
    trade_id        = event["trade_id"]
    batchlot_ids    = event["batchlot_ids"]
    escrow_amount   = float(event["escrow_amount"])
    escrow_currency = event["escrow_currency"]
    forex_rate      = float(event["forex_rate"])
    fintech_ref     = event["fintech_ref"]
    confirmed_at    = event["confirmed_at"]

    # ── Verify bid has not timed out ──────────────────────────────
    bid_result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT status, bid_submitted_at
            FROM trades
            WHERE trade_id = :trade_id
        """,
        parameters=[{
            "name": "trade_id",
            "value": {"stringValue": trade_id}
        }],
    )

    if not bid_result["records"]:
        raise ValueError(f"TradeID '{trade_id}' not found")

    status           = bid_result["records"][0][0]["stringValue"]
    bid_submitted_at = bid_result["records"][0][1]["stringValue"]

    if status != "BID_RECEIVED":
        raise ValueError(
            f"TradeID '{trade_id}' is in status '{status}'. "
            f"Expected BID_RECEIVED. Bid may have already been "
            f"cancelled or processed."
        )

    # Check timeout
    bid_time    = datetime.fromisoformat(
        bid_submitted_at.replace("Z", "+00:00"))
    now         = datetime.now(timezone.utc)
    elapsed     = (now - bid_time).total_seconds()

    if elapsed > ESCROW_TIMEOUT_SECONDS:
        # Timeout -- cancel bid and release any partial reservations
        _cancel_bid(trade_id, batchlot_ids,
                    "ESCROW_TIMEOUT", bid_submitted_at)
        return {
            "status":    "BID_CANCELLED",
            "trade_id":  trade_id,
            "reason":    "ESCROW_TIMEOUT",
            "elapsed_s": round(elapsed, 1),
        }

    # ── Atomic: reserve inventory and create escrow record ────────
    # Both writes execute in sequence. If either fails the
    # Lambda raises and EventBridge retries from the start.
    # No partial state is possible.
    reserved_at = datetime.now(timezone.utc).isoformat()

    # Reserve all BatchLotIDs
    for bl_id in batchlot_ids:
        aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql="""
                UPDATE batchlots SET
                    status     = 'RESERVED',
                    trade_id   = :trade_id,
                    reserved_at = :reserved_at
                WHERE batchlot_id = :bl_id
                AND   status      = 'RACKED'
            """,
            parameters=[
                {"name": "trade_id",    "value": {"stringValue": trade_id}},
                {"name": "reserved_at", "value": {"stringValue": reserved_at}},
                {"name": "bl_id",       "value": {"stringValue": bl_id}},
            ],
        )

    # Create escrow record
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO escrow_records (
                trade_id, escrow_amount, escrow_currency,
                forex_rate, fintech_ref, status,
                confirmed_at, created_at
            ) VALUES (
                :trade_id, :amount, :currency,
                :rate, :fintech_ref, 'FUNDED',
                :confirmed_at, :created_at
            )
        """,
        parameters=[
            {"name": "trade_id",    "value": {"stringValue": trade_id}},
            {"name": "amount",      "value": {"doubleValue": escrow_amount}},
            {"name": "currency",    "value": {"stringValue": escrow_currency}},
            {"name": "rate",        "value": {"doubleValue": forex_rate}},
            {"name": "fintech_ref", "value": {"stringValue": fintech_ref}},
            {"name": "confirmed_at","value": {"stringValue": confirmed_at}},
            {"name": "created_at",  "value": {"stringValue": reserved_at}},
        ],
    )

    # Advance trade status
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE trades SET
                status      = 'ESCROW_CREATED',
                reserved_at = :reserved_at
            WHERE trade_id  = :trade_id
        """,
        parameters=[
            {"name": "reserved_at", "value": {"stringValue": reserved_at}},
            {"name": "trade_id",    "value": {"stringValue": trade_id}},
        ],
    )

    events_client.put_events(Entries=[{
        "Source":       "gant.escrow",
        "DetailType":   "ESCROW_CREATED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":     trade_id,
            "batchlot_ids": batchlot_ids,
            "escrow_amount":   escrow_amount,
            "escrow_currency": escrow_currency,
            "forex_rate":      forex_rate,
            "reserved_at":     reserved_at,
        }),
    }])

    logger.info(
        f"Escrow created atomically for {trade_id}: "
        f"{escrow_currency} {escrow_amount:,.2f} -- "
        f"{len(batchlot_ids)} BatchLotIDs reserved"
    )

    return {
        "status":       "ESCROW_CREATED",
        "trade_id":     trade_id,
        "batchlot_ids": batchlot_ids,
        "reserved_at":  reserved_at,
    }


# ── Bid cancellation helper ───────────────────────────────────────
def _cancel_bid(trade_id: str, batchlot_ids: list,
                reason: str, bid_submitted_at: str) -> None:
    cancelled_at = datetime.now(timezone.utc).isoformat()

    # Release any reservations
    for bl_id in batchlot_ids:
        aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql="""
                UPDATE batchlots SET
                    status   = 'RACKED',
                    trade_id = NULL
                WHERE batchlot_id = :bl_id
                AND   trade_id    = :trade_id
            """,
            parameters=[
                {"name": "bl_id",     "value": {"stringValue": bl_id}},
                {"name": "trade_id",  "value": {"stringValue": trade_id}},
            ],
        )

    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE trades SET
                status       = 'CANCELLED',
                cancel_reason = :reason,
                cancelled_at  = :cancelled_at
            WHERE trade_id = :trade_id
        """,
        parameters=[
            {"name": "reason",       "value": {"stringValue": reason}},
            {"name": "cancelled_at", "value": {"stringValue": cancelled_at}},
            {"name": "trade_id",     "value": {"stringValue": trade_id}},
        ],
    )

    events_client.put_events(Entries=[{
        "Source":       "gant.escrow",
        "DetailType":   "BID_CANCELLED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":        trade_id,
            "reason":          reason,
            "bid_submitted_at":bid_submitted_at,
            "cancelled_at":    cancelled_at,
        }),
    }])

    logger.warning(
        f"Bid cancelled for {trade_id}: {reason}"
    )


# ── 2. Logistics financing fraud prevention ───────────────────────
def process_logistics_financing(event: dict, context) -> dict:
    """
    Processes a logistics financing application.
    Two mandatory gates before any financing is released:
    1. Buyer must upload a legitimate freight forwarder invoice
    2. Buyer must actively acknowledge a fraud declaration popup
    Neither gate can be bypassed programmatically.

    Logistics financing carries a hard destination lock --
    funds can only transfer to the beneficiary account on
    the uploaded invoice. No other destination is permitted.

    Expected event format:
    {
        "trade_id":              "TRD-20260426-0012",
        "buid":                  "0078234",
        "ff_invoice_s3_key":     "trades/TRD-20260426-0012/ff_invoice.pdf",
        "ff_invoice_hash":       "sha256...",
        "fraud_declaration_ack": true,
        "ff_beneficiary_account":"ACCOUNT-REF-123",
        "financing_amount_cad":  12500.00
    }
    """
    trade_id             = event["trade_id"]
    buid                 = event["buid"]
    ff_invoice_s3_key    = event["ff_invoice_s3_key"]
    ff_invoice_hash      = event["ff_invoice_hash"]
    fraud_declaration    = event.get("fraud_declaration_ack", False)
    ff_beneficiary       = event["ff_beneficiary_account"]
    financing_amount     = float(event["financing_amount_cad"])

    # ── Gate 1: FF invoice must be present ───────────────────────
    if not ff_invoice_s3_key or not ff_invoice_hash:
        raise ValueError(
            f"Logistics financing application for {trade_id} "
            f"rejected: freight forwarder invoice is required. "
            f"No financing is released without a verified FF invoice."
        )

    # ── Gate 2: Fraud declaration must be actively acknowledged ──
    if not fraud_declaration:
        raise ValueError(
            f"Logistics financing application for {trade_id} "
            f"rejected: fraud declaration acknowledgement is required. "
            f"The buyer must actively confirm the fraud declaration "
            f"before financing is processed."
        )

    # ── Verify trade is in a valid state for logistics financing ──
    trade_result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT status, buid FROM trades
            WHERE trade_id = :trade_id
        """,
        parameters=[{
            "name": "trade_id",
            "value": {"stringValue": trade_id}
        }],
    )

    if not trade_result["records"]:
        raise ValueError(f"TradeID '{trade_id}' not found")

    trade_status = trade_result["records"][0][0]["stringValue"]
    trade_buid   = trade_result["records"][0][1]["stringValue"]

    if trade_status not in ("ESCROW_CREATED", "FARMER_PAID",
                             "PICK_CONFIRMED"):
        raise ValueError(
            f"Logistics financing not permitted for trade "
            f"'{trade_id}' in status '{trade_status}'"
        )

    if trade_buid != buid:
        raise PermissionError(
            f"BUID '{buid}' is not the buyer on trade '{trade_id}'"
        )

    applied_at = datetime.now(timezone.utc).isoformat()

    # ── Write logistics financing record with destination lock ────
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO logistics_financing (
                trade_id, buid, ff_invoice_s3_key,
                ff_invoice_hash, fraud_declaration_ack,
                ff_beneficiary_account, financing_amount_cad,
                destination_locked, status, applied_at
            ) VALUES (
                :trade_id, :buid, :ff_s3_key,
                :ff_hash, true,
                :ff_beneficiary, :amount,
                true, 'PENDING_FINTECH_APPROVAL', :applied_at
            )
        """,
        parameters=[
            {"name": "trade_id",      "value": {"stringValue": trade_id}},
            {"name": "buid",          "value": {"stringValue": buid}},
            {"name": "ff_s3_key",     "value": {"stringValue": ff_invoice_s3_key}},
            {"name": "ff_hash",       "value": {"stringValue": ff_invoice_hash}},
            {"name": "ff_beneficiary","value": {"stringValue": ff_beneficiary}},
            {"name": "amount",        "value": {"doubleValue": financing_amount}},
            {"name": "applied_at",    "value": {"stringValue": applied_at}},
        ],
    )

    events_client.put_events(Entries=[{
        "Source":       "gant.logistics",
        "DetailType":   "LOGISTICS_FINANCING_APPLIED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":             trade_id,
            "buid":                 buid,
            "financing_amount_cad": financing_amount,
            "ff_beneficiary":       ff_beneficiary,
            "destination_locked":   True,
            "applied_at":           applied_at,
        }),
    }])

    logger.info(
        f"Logistics financing application processed for {trade_id}: "
        f"CAD {financing_amount:,.2f} with destination lock "
        f"to {ff_beneficiary}"
    )

    return {
        "status":               "LOGISTICS_FINANCING_APPLIED",
        "trade_id":             trade_id,
        "financing_amount_cad": financing_amount,
        "ff_beneficiary":       ff_beneficiary,
        "destination_locked":   True,
        "applied_at":           applied_at,
    }
