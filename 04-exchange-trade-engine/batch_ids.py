# 4.3 BatchID and BatchLotID Generation
# Lambda functions generating traceable identifiers at intake and bagging
# BatchID format:  CropCode-FUID-DDMMYY-SeqNo
# BatchLotID format: BatchID-BagNo
# Example BatchID:    LENT-0045364-260426-01
# Example BatchLotID: LENT-0045364-260426-01-23

import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client = boto3.client("rds-data")
events_client = boto3.client("events")
ssm_client    = boto3.client("ssm")

# ── Configuration ────────────────────────────────────────────────
AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"
EVENT_BUS     = "gant-platform"

VALID_CROP_CODES = {"LENT", "CHKP", "NBEAN", "PINTO", "BLKBN"}


# ── 1. BatchID Generation ────────────────────────────────────────
def generate_batch_id(event: dict, context) -> dict:
    """
    Triggered at weighbridge intake when a truck registers.
    Constructs a human-readable globally unique BatchID and
    writes the intake record to Aurora with status INTAKE_PENDING.

    Expected event format:
    {
        "crop_code":    "LENT",
        "fuid":         "0045364",
        "delivery_date": "2026-04-26",
        "truck_seq":    1
    }
    """
    crop_code     = event.get("crop_code", "").upper()
    fuid          = event.get("fuid", "")
    delivery_date = event.get("delivery_date", "")
    truck_seq     = event.get("truck_seq")

    # ── Input validation ─────────────────────────────────────────
    if crop_code not in VALID_CROP_CODES:
        raise ValueError(
            f"Invalid crop_code '{crop_code}'. "
            f"Must be one of: {sorted(VALID_CROP_CODES)}"
        )
    if not fuid or not fuid.isdigit():
        raise ValueError(f"Invalid FUID: '{fuid}'. Must be numeric.")
    if not delivery_date:
        raise ValueError("delivery_date is required")
    if not isinstance(truck_seq, int) or truck_seq < 1:
        raise ValueError("truck_seq must be a positive integer")

    # ── Construct BatchID ────────────────────────────────────────
    # Format date component as DDMMYY
    date_obj   = datetime.strptime(delivery_date, "%Y-%m-%d")
    date_part  = date_obj.strftime("%d%m%y")
    seq_part   = str(truck_seq).zfill(2)
    batch_id   = f"{crop_code}-{fuid.zfill(7)}-{date_part}-{seq_part}"

    # ── Duplicate detection ──────────────────────────────────────
    check = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="SELECT batch_id FROM batches WHERE batch_id = :batch_id",
        parameters=[{
            "name": "batch_id",
            "value": {"stringValue": batch_id}
        }],
    )
    if check["records"]:
        raise ValueError(
            f"BatchID '{batch_id}' already exists. "
            f"Check truck sequence number for FUID {fuid} on {delivery_date}."
        )

    # ── Write to Aurora ──────────────────────────────────────────
    intake_timestamp = datetime.now(timezone.utc).isoformat()

    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO batches (
                batch_id, crop_code, fuid, delivery_date,
                truck_seq, status, intake_timestamp
            ) VALUES (
                :batch_id, :crop_code, :fuid, :delivery_date,
                :truck_seq, 'INTAKE_PENDING', :intake_timestamp
            )
        """,
        parameters=[
            {"name": "batch_id",         "value": {"stringValue": batch_id}},
            {"name": "crop_code",        "value": {"stringValue": crop_code}},
            {"name": "fuid",             "value": {"stringValue": fuid.zfill(7)}},
            {"name": "delivery_date",    "value": {"stringValue": delivery_date}},
            {"name": "truck_seq",        "value": {"longValue": truck_seq}},
            {"name": "intake_timestamp", "value": {"stringValue": intake_timestamp}},
        ],
    )

    logger.info(f"BatchID generated: {batch_id}")

    return {
        "batch_id":         batch_id,
        "status":           "INTAKE_PENDING",
        "intake_timestamp": intake_timestamp,
    }


# ── 2. BatchLotID Generation ─────────────────────────────────────
def generate_batchlot_ids(event: dict, context) -> dict:
    """
    Triggered at bagging line completion when bag count is confirmed.
    Generates one BatchLotID per bag, writes all records to Aurora
    in a single batch insert, and publishes BATCHLOT_GENERATION_COMPLETE
    to EventBridge.

    Expected event format:
    {
        "batch_id":  "LENT-0045364-260426-01",
        "bag_count": 44
    }
    """
    batch_id  = event.get("batch_id")
    bag_count = event.get("bag_count")

    if not batch_id:
        raise ValueError("batch_id is required")
    if not isinstance(bag_count, int) or bag_count < 1:
        raise ValueError("bag_count must be a positive integer")

    # ── Verify BatchID exists and is in correct state ────────────
    check = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT status FROM batches
            WHERE batch_id = :batch_id
        """,
        parameters=[{
            "name": "batch_id",
            "value": {"stringValue": batch_id}
        }],
    )
    if not check["records"]:
        raise ValueError(f"BatchID '{batch_id}' not found in Aurora")

    current_status = check["records"][0][0]["stringValue"]
    if current_status != "CLEANED_PENDING_BAGGING":
        raise ValueError(
            f"BatchID '{batch_id}' is in status '{current_status}'. "
            f"Expected 'CLEANED_PENDING_BAGGING' before bagging."
        )

    # ── Generate BatchLotIDs ─────────────────────────────────────
    production_timestamp = datetime.now(timezone.utc).isoformat()
    batchlot_ids = [
        f"{batch_id}-{str(i).zfill(2)}"
        for i in range(1, bag_count + 1)
    ]

    # ── Batch insert all BatchLotIDs ─────────────────────────────
    # Build parameterised multi-row insert
    value_clauses = []
    parameters    = []

    for i, bl_id in enumerate(batchlot_ids):
        value_clauses.append(
            f"(:bl_id_{i}, :batch_id_{i}, 'PRODUCED', :prod_ts_{i})"
        )
        parameters.extend([
            {"name": f"bl_id_{i}",   "value": {"stringValue": bl_id}},
            {"name": f"batch_id_{i}","value": {"stringValue": batch_id}},
            {"name": f"prod_ts_{i}", "value": {"stringValue": production_timestamp}},
        ])

    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql=f"""
            INSERT INTO batchlots (batchlot_id, batch_id, status, production_timestamp)
            VALUES {", ".join(value_clauses)}
        """,
        parameters=parameters,
    )

    # ── Update Batch status to BAGGED_AND_RACKED ─────────────────
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE batches
            SET status = 'BAGGED_AND_RACKED',
                bag_count = :bag_count,
                bagging_timestamp = :prod_ts
            WHERE batch_id = :batch_id
        """,
        parameters=[
            {"name": "bag_count", "value": {"longValue": bag_count}},
            {"name": "prod_ts",   "value": {"stringValue": production_timestamp}},
            {"name": "batch_id",  "value": {"stringValue": batch_id}},
        ],
    )

    # ── Publish BATCHLOT_GENERATION_COMPLETE ─────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.wms",
        "DetailType":   "BATCHLOT_GENERATION_COMPLETE",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "batch_id":              batch_id,
            "bag_count":             bag_count,
            "batchlot_ids":          batchlot_ids,
            "production_timestamp":  production_timestamp,
        }),
    }])

    logger.info(
        f"Generated {bag_count} BatchLotIDs for BatchID {batch_id}"
    )

    return {
        "batch_id":     batch_id,
        "bag_count":    bag_count,
        "batchlot_ids": batchlot_ids,
        "status":       "BAGGED_AND_RACKED",
    }
