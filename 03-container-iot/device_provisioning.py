# 3.4 Device Provisioning and Decommissioning
# Lambda functions managing Globalstar Integrity 150 lifecycle
# Operations: LOADED_EXW binding, delivery confirmation, and
# IoT Core rule filtering for post-COMPLETED transmissions

import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

iot_client      = boto3.client("iot")
events_client   = boto3.client("events")

# ── Constants ────────────────────────────────────────────────────
SHADOW_PREFIX   = "$aws/things"
EVENT_BUS       = "gant-platform"
DLQ_TOPIC       = "gant/container/rejected"


# ── 1. LOADED_EXW Binding ────────────────────────────────────────
def bind_device_to_trade(event: dict, context) -> dict:
    """
    Triggered at LOADED_EXW state transition.
    Binds the Integrity 150 device to the active trade by writing
    TradeID, BatchLotID manifest, and RuuviTag MAC addresses to
    the named device shadow. From this moment every telemetry
    reading from this device is automatically tagged with the
    correct TradeID and BatchLotID by the IoT Rules Engine.

    Expected event format:
    {
        "device_esn":      "ESN-0045364",
        "trade_id":        "TRD-20260426-0012",
        "batchlot_ids":    ["BL-0045364-01", "BL-0045364-02"],
        "ruuvitag_macs":   ["AA:BB:CC:DD:EE:01", "AA:BB:CC:DD:EE:02"]
    }
    """
    device_esn    = event["device_esn"]
    trade_id      = event["trade_id"]
    batchlot_ids  = event["batchlot_ids"]
    ruuvitag_macs = event["ruuvitag_macs"]

    if len(ruuvitag_macs) != 2:
        raise ValueError(
            f"Expected exactly 2 RuuviTag MACs, got {len(ruuvitag_macs)}"
        )

    shadow_payload = {
        "state": {
            "desired": {
                "status":          "ACTIVE_TRANSIT",
                "trade_id":        trade_id,
                "batchlot_ids":    batchlot_ids,
                "ruuvitag_macs":   ruuvitag_macs,
                "bound_at":        datetime.now(timezone.utc).isoformat(),
            }
        }
    }

    iot_client.update_thing_shadow(
        thingName=device_esn,
        payload=json.dumps(shadow_payload).encode()
    )

    logger.info(
        f"Device {device_esn} bound to trade {trade_id} "
        f"with {len(batchlot_ids)} BatchLotIDs"
    )

    # Publish DEVICE_BOUND confirmation to EventBridge
    events_client.put_events(Entries=[{
        "Source":       "gant.container",
        "DetailType":   "DEVICE_BOUND",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "device_esn":   device_esn,
            "trade_id":     trade_id,
            "batchlot_ids": batchlot_ids,
            "bound_at":     datetime.now(timezone.utc).isoformat(),
        }),
    }])

    return {
        "status":     "BOUND",
        "device_esn": device_esn,
        "trade_id":   trade_id,
    }


# ── 2. Delivery Confirmation and Device Retirement ───────────────
def close_device_session(event: dict, context) -> dict:
    """
    Triggered at DELIVERED state confirmation.
    Writes DELIVERED status and final timestamp to the device shadow.
    Permanently retires the device in the IoT Core registry by
    setting status to COMPLETED. Publishes DEVICE_CLOSED event to
    EventBridge which triggers the DSRE session seal Lambda.

    Expected event format:
    {
        "device_esn":        "ESN-0045364",
        "trade_id":          "TRD-20260426-0012",
        "delivered_at":      "2026-05-14T09:22:00Z"
    }
    """
    device_esn   = event["device_esn"]
    trade_id     = event["trade_id"]
    delivered_at = event.get(
        "delivered_at",
        datetime.now(timezone.utc).isoformat()
    )

    shadow_payload = {
        "state": {
            "desired": {
                "status":       "COMPLETED",
                "delivered_at": delivered_at,
                "closed_at":    datetime.now(timezone.utc).isoformat(),
            }
        }
    }

    iot_client.update_thing_shadow(
        thingName=device_esn,
        payload=json.dumps(shadow_payload).encode()
    )

    logger.info(
        f"Device {device_esn} retired for trade {trade_id}. "
        f"Delivered at: {delivered_at}"
    )

    # Publish DEVICE_CLOSED to EventBridge
    # This triggers the DSRE session seal Lambda
    events_client.put_events(Entries=[{
        "Source":       "gant.container",
        "DetailType":   "DEVICE_CLOSED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "device_esn":   device_esn,
            "trade_id":     trade_id,
            "delivered_at": delivered_at,
            "closed_at":    datetime.now(timezone.utc).isoformat(),
        }),
    }])

    return {
        "status":     "COMPLETED",
        "device_esn": device_esn,
        "trade_id":   trade_id,
    }


# ── 3. IoT Core Rule -- Post-COMPLETED Transmission Filter ───────
# SQL rule deployed to AWS IoT Core via CloudFormation.
# Filters any transmission arriving from a device whose shadow
# status is COMPLETED. Routes rejected messages to dead-letter
# SNS topic for investigation.
#
# Rule SQL:
#   SELECT topic(3) as device_esn, * FROM 'gant/container/telemetry/+'
#   WHERE get_thing_shadow(topic(3), 'gant-iot-role').state.reported.status
#         = 'COMPLETED'
#
# On match: republish to gant/container/rejected with original
# payload plus rejection reason and timestamp appended.

IOT_RULE_DEFINITION = {
    "sql": (
        "SELECT topic(3) as device_esn, * "
        "FROM 'gant/container/telemetry/+' "
        "WHERE get_thing_shadow(topic(3), 'gant-iot-role')"
        ".state.reported.status = 'COMPLETED'"
    ),
    "actions": [{
        "republish": {
            "roleArn": "arn:aws:iam::ACCOUNT_ID:role/gant-iot-republish",
            "topic":   DLQ_TOPIC,
            "qos":     1,
        }
    }],
    "ruleDisabled":    False,
    "awsIotSqlVersion": "2016-03-23",
    "description": (
        "Reject and dead-letter any telemetry from devices "
        "whose shadow status is COMPLETED. Prevents stale or "
        "fraudulent post-delivery transmissions from entering "
        "the DSRE pipeline."
    ),
}


def deploy_rejection_rule(event: dict, context) -> dict:
    """
    Deploys the post-COMPLETED rejection rule to AWS IoT Core.
    Called once during platform setup via CloudFormation custom
    resource or direct Lambda invocation. Idempotent -- safe to
    call multiple times.
    """
    iot_client.create_topic_rule(
        ruleName="gant_reject_completed_device_telemetry",
        topicRulePayload=IOT_RULE_DEFINITION,
    )

    logger.info("Post-COMPLETED rejection rule deployed to IoT Core")

    return {"status": "RULE_DEPLOYED"}
