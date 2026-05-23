# 6.3 DSRE Aggregation Pipeline
# Lambda function running hourly DSRE computation during active transit
# Four domains: Commodity Condition (0.35), Route Security (0.25),
#               Geopolitical (0.20), Logistics Infrastructure (0.20)
# Missing reading protocol with confidence degradation
# Geopolitical hard override: Domain 3 below 20 forces Red regardless
# Session opens at LOADED_EXW, seals at DELIVERED

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

# ── Domain weights ────────────────────────────────────────────────
DOMAIN_WEIGHTS = {
    "commodity_condition":      0.35,
    "route_security":           0.25,
    "geopolitical":             0.20,
    "logistics_infrastructure": 0.20,
}

# ── DSRE band thresholds ──────────────────────────────────────────
DSRE_BANDS = [
    (0,  30,  "GREEN",  "Shipment normal"),
    (31, 60,  "YELLOW", "Advisory issued"),
    (61, 75,  "ORANGE", "Escalated advisory"),
    (76, 100, "RED",    "Critical alert"),
]

# ── Missing reading protocol ──────────────────────────────────────
MAX_GAP_CYCLES          = 3
CONFIDENCE_DEGRADATION  = 0.10   # 10% per consecutive gap cycle
MAX_DEGRADATION         = 0.30   # 30% maximum at 3 cycles

# ── Geopolitical hard override threshold ─────────────────────────
GEO_HARD_OVERRIDE_FLOOR = 20

# ── Source precedence weights per domain ─────────────────────────
COMMODITY_WEIGHTS = {
    "temperature":  0.35,
    "humidity":     0.35,
    "shock":        0.20,
    "gps_dwell":    0.10,
}
ROUTE_WEIGHTS = {
    "ais_vessel":   0.60,
    "port_edi":     0.40,
}
GEO_WEIGHTS = {
    "acled":        0.45,
    "imo":          0.40,
    "un_ocha":      0.15,
}
LOGISTICS_WEIGHTS = {
    "port_edi":     0.55,
    "ais_vessel":   0.45,
}

# ── Scoring bands with linear interpolation ───────────────────────
TEMP_BANDS = [
    (-5.0,  2.0, 80.0, 100.0),
    (2.0,   5.0, 55.0,  80.0),
    (5.0,  15.0,  5.0,  55.0),
    (15.0, 25.0,  0.0,   5.0),
    (25.0, 60.0, 60.0, 100.0),
]
HUMIDITY_BANDS = [
    (0.0,  45.0,  5.0,  20.0),
    (45.0, 60.0,  0.0,   5.0),
    (60.0, 70.0, 20.0,  50.0),
    (70.0, 80.0, 50.0,  75.0),
    (80.0, 100.0,75.0, 100.0),
]
SHOCK_BANDS = [
    (0.0,  0.5,  0.0, 10.0),
    (0.5,  1.5, 10.0, 35.0),
    (1.5,  3.0, 35.0, 65.0),
    (3.0,  5.0, 65.0, 85.0),
    (5.0, 20.0, 85.0, 100.0),
]
DWELL_BANDS = [
    (0.0,  1.0,  0.0,  10.0),
    (1.0,  2.0, 10.0,  30.0),
    (2.0,  3.0, 30.0,  55.0),
    (3.0,  5.0, 55.0,  80.0),
    (5.0, 10.0, 80.0, 100.0),
]
GENERIC_BANDS = [
    (0, 1,  0.0, 15.0),
    (1, 2, 15.0, 35.0),
    (2, 3, 35.0, 60.0),
    (3, 4, 60.0, 80.0),
    (4, 5, 80.0, 100.0),
]


# ── Linear interpolation within band ─────────────────────────────
def interpolate(value: float, bands: list) -> float:
    for (il, iu, sl, su) in bands:
        if il <= value <= iu:
            rng = iu - il
            if rng == 0:
                return sl
            return round(sl + ((value - il) / rng) * (su - sl), 4)
    return bands[0][2] if value < bands[0][0] else bands[-1][3]


# ── Weighted effective input ──────────────────────────────────────
def weighted_input(signals: dict, weights: dict) -> float:
    return round(
        sum(weights.get(k, 0.0) * v for k, v in signals.items()), 4)


# ── Band classification ───────────────────────────────────────────
def classify_band(score: float) -> tuple:
    for (lo, hi, band, desc) in DSRE_BANDS:
        if lo <= score <= hi:
            return band, desc
    return "RED", "Critical alert"


# ── Fetch last confirmed IoT reading ─────────────────────────────
def fetch_last_iot_reading(trade_id: str) -> dict | None:
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT temperature_c, humidity_pct, shock_g,
                   gps_dwell_hours, reading_timestamp,
                   consecutive_gap_cycles
            FROM dsre_iot_readings
            WHERE trade_id = :trade_id
            ORDER BY reading_timestamp DESC
            LIMIT 1
        """,
        parameters=[{
            "name": "trade_id",
            "value": {"stringValue": trade_id}
        }],
    )
    if not result["records"]:
        return None
    row = result["records"][0]
    return {
        "temperature_c":          row[0]["doubleValue"],
        "humidity_pct":           row[1]["doubleValue"],
        "shock_g":                row[2]["doubleValue"],
        "gps_dwell_hours":        row[3]["doubleValue"],
        "reading_timestamp":      row[4]["stringValue"],
        "consecutive_gap_cycles": int(row[5]["longValue"]),
    }


# ── Domain 1: Commodity Condition ────────────────────────────────
def compute_commodity_domain(iot: dict | None,
                              gap_cycles: int) -> dict:
    """
    Applies missing reading protocol when IoT readings absent.
    Confidence degrades 10% per consecutive gap cycle up to 30%.
    """
    is_gap = iot is None
    confidence_factor = 1.0

    if is_gap:
        degradation       = min(gap_cycles * CONFIDENCE_DEGRADATION,
                                MAX_DEGRADATION)
        confidence_factor = 1.0 - degradation
        logger.warning(
            f"Domain 1 gap cycle {gap_cycles}. "
            f"Confidence factor: {confidence_factor:.2f}"
        )
        # Use last confirmed reading carried forward
        if gap_cycles == 0:
            return {
                "domain_score":    0.0,
                "confidence":      1.0,
                "is_gap":          True,
                "signals":         {},
                "gap_note": "No prior IoT reading available",
            }

    signals = {
        "temperature": interpolate(iot["temperature_c"], TEMP_BANDS),
        "humidity":    interpolate(iot["humidity_pct"], HUMIDITY_BANDS),
        "shock":       interpolate(iot["shock_g"], SHOCK_BANDS),
        "gps_dwell":   interpolate(iot["gps_dwell_hours"], DWELL_BANDS),
    }

    eff_input    = weighted_input(signals, COMMODITY_WEIGHTS)
    domain_score = round(eff_input * confidence_factor, 4)

    return {
        "domain_score":    domain_score,
        "confidence":      confidence_factor,
        "is_gap":          is_gap,
        "signals":         signals,
        "reading_ts":      iot.get("reading_timestamp") if iot else None,
    }


# ── Domain 2: Route Security ──────────────────────────────────────
def compute_route_domain(ais_level: float,
                          port_edi_level: float) -> dict:
    signals      = {"ais_vessel": ais_level, "port_edi": port_edi_level}
    eff_input    = weighted_input(signals, ROUTE_WEIGHTS)
    domain_score = interpolate(eff_input, GENERIC_BANDS)
    return {
        "domain_score": domain_score,
        "signals":      signals,
    }


# ── Domain 3: Geopolitical ────────────────────────────────────────
def compute_geopolitical_domain(acled: float, imo: float,
                                  ocha: float,
                                  lloyds_active: bool) -> dict:
    signals      = {"acled": acled, "imo": imo, "un_ocha": ocha}
    eff_input    = weighted_input(signals, GEO_WEIGHTS)
    domain_score = interpolate(eff_input, GENERIC_BANDS)
    hard_override = False

    # Hard override: confirmed attacks override composite to Red
    if lloyds_active or domain_score < GEO_HARD_OVERRIDE_FLOOR:
        if lloyds_active:
            hard_override = True
            logger.warning(
                "DSRE Domain 3 hard override triggered by "
                "Lloyd's active attack signal"
            )

    return {
        "domain_score":   domain_score,
        "signals":        {**signals,
                           "lloyds_active": lloyds_active},
        "hard_override":  hard_override,
    }


# ── Domain 4: Logistics Infrastructure ───────────────────────────
def compute_logistics_domain(port_edi_level: float,
                               ais_level: float) -> dict:
    signals      = {"port_edi": port_edi_level,
                    "ais_vessel": ais_level}
    eff_input    = weighted_input(signals, LOGISTICS_WEIGHTS)
    domain_score = interpolate(eff_input, GENERIC_BANDS)
    return {
        "domain_score": domain_score,
        "signals":      signals,
    }


# ── DSRE aggregate score ──────────────────────────────────────────
def compute_aggregate(d1: float, d2: float,
                       d3: float, d4: float,
                       geo_hard_override: bool) -> float:
    composite = round(
        DOMAIN_WEIGHTS["commodity_condition"]      * d1 +
        DOMAIN_WEIGHTS["route_security"]           * d2 +
        DOMAIN_WEIGHTS["geopolitical"]             * d3 +
        DOMAIN_WEIGHTS["logistics_infrastructure"] * d4,
        4
    )
    # Geopolitical hard override forces Red regardless of composite
    if geo_hard_override:
        return 100.0
    return composite


# ── Log cycle to Aurora ───────────────────────────────────────────
def log_dsre_cycle(trade_id: str, cycle_data: dict) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO dsre_log (
                trade_id, aggregate_score, band,
                domain_breakdown, is_gap_cycle,
                computed_at, logic_version
            ) VALUES (
                :trade_id, :score, :band,
                :breakdown, :is_gap,
                :computed_at, :version
            )
        """,
        parameters=[
            {"name": "trade_id",   "value": {"stringValue": trade_id}},
            {"name": "score",      "value": {"doubleValue": cycle_data["aggregate_score"]}},
            {"name": "band",       "value": {"stringValue": cycle_data["band"]}},
            {"name": "breakdown",  "value": {"stringValue": json.dumps(cycle_data["domain_breakdown"])}},
            {"name": "is_gap",     "value": {"booleanValue": cycle_data["is_gap_cycle"]}},
            {"name": "computed_at","value": {"stringValue": cycle_data["computed_at"]}},
            {"name": "version",    "value": {"stringValue": cycle_data["logic_version"]}},
        ],
    )


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered hourly by EventBridge Scheduler for every active trade.
    Computes DSRE aggregate score across four domains.
    Applies missing reading protocol for Domain 1 on IoT gap cycles.
    Applies geopolitical hard override when confirmed attack detected.
    Logs full computation to Aurora and publishes DSRE_CYCLE_COMPLETE.

    Expected event format:
    {
        "trade_id":       "TRD-20260426-0012",
        "corridor_code":  "CA-IN",
        "iot_reading":    {
            "temperature_c":   4.2,
            "humidity_pct":    58.0,
            "shock_g":         0.3,
            "gps_dwell_hours": 0.0
        },
        "external_signals": {
            "ais_level":        1.2,
            "port_edi_level":   0.8,
            "acled_level":      1.5,
            "imo_level":        1.0,
            "ocha_level":       0.5,
            "lloyds_active":    false
        },
        "gap_cycles":     0,
        "logic_version":  "DSRE-1.0.0"
    }
    """
    trade_id      = event["trade_id"]
    iot_raw       = event.get("iot_reading")
    ext           = event.get("external_signals", {})
    gap_cycles    = int(event.get("gap_cycles", 0))
    logic_version = event.get("logic_version", "DSRE-1.0.0")
    computed_at   = datetime.now(timezone.utc).isoformat()

    # If no fresh IoT reading provided fetch last confirmed
    iot = iot_raw or fetch_last_iot_reading(trade_id)

    # ── Domain computations ───────────────────────────────────────
    d1 = compute_commodity_domain(iot, gap_cycles)
    d2 = compute_route_domain(
        ext.get("ais_level", 0.0),
        ext.get("port_edi_level", 0.0))
    d3 = compute_geopolitical_domain(
        ext.get("acled_level", 0.0),
        ext.get("imo_level", 0.0),
        ext.get("ocha_level", 0.0),
        ext.get("lloyds_active", False))
    d4 = compute_logistics_domain(
        ext.get("port_edi_level", 0.0),
        ext.get("ais_level", 0.0))

    # ── Aggregate ─────────────────────────────────────────────────
    aggregate = compute_aggregate(
        d1["domain_score"], d2["domain_score"],
        d3["domain_score"], d4["domain_score"],
        d3["hard_override"])

    band, band_desc = classify_band(aggregate)

    cycle_data = {
        "trade_id":        trade_id,
        "aggregate_score": aggregate,
        "band":            band,
        "band_description":band_desc,
        "domain_breakdown":{
            "d1_commodity":   d1,
            "d2_route":       d2,
            "d3_geopolitical":d3,
            "d4_logistics":   d4,
        },
        "is_gap_cycle":    iot is None,
        "computed_at":     computed_at,
        "logic_version":   logic_version,
    }

    # ── Log to Aurora ─────────────────────────────────────────────
    log_dsre_cycle(trade_id, cycle_data)

    # ── Publish DSRE_CYCLE_COMPLETE ───────────────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.dsre",
        "DetailType":   "DSRE_CYCLE_COMPLETE",
        "EventBusName": EVENT_BUS,
        "Detail":       json.dumps(cycle_data),
    }])

    logger.info(
        f"DSRE cycle complete for {trade_id}: "
        f"score={aggregate:.2f} band={band} "
        f"gap={iot is None}"
    )

    return cycle_data
