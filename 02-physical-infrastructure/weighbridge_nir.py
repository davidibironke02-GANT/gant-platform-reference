# 2.6 Weighbridge and NIR Integration
# Lambda running on AWS IoT Greengrass edge node
# Reads weighbridge via Modbus TCP, NIR via RS-232
# Publishes structured payload to AWS IoT Core MQTT

import json
import time
import logging
import boto3
import serial
from datetime import datetime, timezone
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Configuration ────────────────────────────────────────────────
WEIGHBRIDGE_HOST    = "192.168.1.10"
WEIGHBRIDGE_PORT    = 502
WEIGHBRIDGE_UNIT_ID = 1

# Modbus register map -- confirm with weighbridge manufacturer
REG_GROSS_WEIGHT    = 100   # float32, 2 registers, kg
REG_TARE_WEIGHT     = 102   # float32, 2 registers, kg
REG_NET_WEIGHT      = 104   # float32, 2 registers, kg

NIR_PORT            = "/dev/ttyUSB0"
NIR_BAUD            = 9600
NIR_TIMEOUT         = 5

MQTT_TOPIC_QC1      = "gant/facility/qc1"
MQTT_TOPIC_QC2      = "gant/facility/qc2"
DLQ_TOPIC           = "gant/facility/dlq"

MAX_RETRIES         = 3
RETRY_BACKOFF       = [1, 2, 4]  # seconds

iot_client = boto3.client("iot-data")


# ── Weighbridge read (Modbus TCP) ────────────────────────────────
def read_weighbridge(batch_id: str) -> dict:
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            client = ModbusTcpClient(
                host=WEIGHBRIDGE_HOST,
                port=WEIGHBRIDGE_PORT,
                timeout=3
            )
            client.connect()
            gross = client.read_holding_registers(
                REG_GROSS_WEIGHT, count=2, slave=WEIGHBRIDGE_UNIT_ID)
            tare  = client.read_holding_registers(
                REG_TARE_WEIGHT,  count=2, slave=WEIGHBRIDGE_UNIT_ID)
            net   = client.read_holding_registers(
                REG_NET_WEIGHT,   count=2, slave=WEIGHBRIDGE_UNIT_ID)
            client.close()

            def f32(r):
                return round(
                    BinaryPayloadDecoder.fromRegisters(
                        r.registers,
                        byteorder=Endian.BIG,
                        wordorder=Endian.BIG
                    ).decode_32bit_float(), 2)

            return {
                "gross_weight_kg": f32(gross),
                "tare_weight_kg":  f32(tare),
                "net_weight_kg":   f32(net),
            }

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])

    raise RuntimeError(
        f"Weighbridge failed after {MAX_RETRIES} attempts: {last_error}")


# ── NIR instrument read (RS-232) ─────────────────────────────────
def read_nir(batch_id: str) -> dict:
    # Command and response format: confirm with NIR manufacturer
    # Expected CSV response: "moisture,protein,foreign_material"
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            with serial.Serial(
                port=NIR_PORT,
                baudrate=NIR_BAUD,
                timeout=NIR_TIMEOUT
            ) as ser:
                ser.write(b"READ\r\n")
                resp = ser.readline().decode("ascii").strip()

                if not resp:
                    raise ValueError("NIR returned empty response")

                parts = resp.split(",")
                if len(parts) < 3:
                    raise ValueError(f"Malformed NIR response: '{resp}'")

                return {
                    "moisture_pct":         round(float(parts[0]), 2),
                    "protein_pct":          round(float(parts[1]), 2),
                    "foreign_material_pct": round(float(parts[2]), 2),
                }

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])

    raise RuntimeError(
        f"NIR failed after {MAX_RETRIES} attempts: {last_error}")


# ── Publish to IoT Core ──────────────────────────────────────────
def publish_qc(batch_id, zone, weight_data, nir_data):
    topic   = MQTT_TOPIC_QC1 if zone == "QC1_INTAKE" else MQTT_TOPIC_QC2
    payload = {
        "batch_id":  batch_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "zone":      zone,
        "readings":  {**weight_data, **nir_data},
    }
    iot_client.publish(
        topic=topic,
        qos=1,
        payload=json.dumps(payload)
    )
    logger.info(f"Published {zone} for batch {batch_id} to {topic}")


# ── Dead-letter queue routing ────────────────────────────────────
def route_to_dlq(batch_id, zone, error):
    iot_client.publish(
        topic=DLQ_TOPIC,
        qos=1,
        payload=json.dumps({
            "batch_id":  batch_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "zone":      zone,
            "error":     error,
            "action":    "Manual QC entry or instrument inspection required",
        })
    )
    logger.error(f"Routed {zone} failure for batch {batch_id} to DLQ")


# ── Lambda handler ───────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by WMS at QC1 (INTAKE_PENDING) and QC2 (PROCESSING_IN_PROGRESS)
    Event: { "batch_id": "LENT-0045364-260426-01",
             "zone": "QC1_INTAKE" | "QC2_POST_PROCESSING" }
    """
    batch_id = event.get("batch_id")
    zone     = event.get("zone")

    if not batch_id or zone not in (
        "QC1_INTAKE", "QC2_POST_PROCESSING"
    ):
        raise ValueError(f"Invalid event: {event}")

    try:
        weight_data = read_weighbridge(batch_id)
        nir_data    = read_nir(batch_id)
        publish_qc(batch_id, zone, weight_data, nir_data)
        return {
            "status":   "SUCCESS",
            "batch_id": batch_id,
            "zone":     zone,
            "readings": {**weight_data, **nir_data},
        }

    except RuntimeError as e:
        route_to_dlq(batch_id, zone, str(e))
        return {
            "status":   "FAILED_TO_DLQ",
            "batch_id": batch_id,
            "error":    str(e),
        }
