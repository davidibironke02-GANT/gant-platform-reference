# 4.4 VVI Computation Engine
# Lambda function computing the Verified Value Index at QC2 completion
# VVI = 0.10 x PriceRef + 0.40 x Quality + 0.25 x Traceability
#       + 0.20 x Compliance + 0.05 x ESG
# Quality score uses crop-specific submetric weights with linear
# interpolation within scoring bands

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

QUALITY_FLOOR = 60.0


# ── Crop-specific submetric weights ──────────────────────────────
CROP_WEIGHTS = {
    "LENT":  {
        "size_uniformity":   0.30,
        "color_integrity":   0.25,
        "moisture":          0.15,
        "foreign_material":  0.15,
        "protein":           0.15,
    },
    "CHKP":  {
        "size_uniformity":   0.35,
        "color_integrity":   0.25,
        "moisture":          0.15,
        "foreign_material":  0.15,
        "protein":           0.10,
    },
    "NBEAN": {
        "size_uniformity":   0.30,
        "color_integrity":   0.25,
        "moisture":          0.15,
        "foreign_material":  0.15,
        "protein":           0.15,
    },
    "PINTO": {
        "size_uniformity":   0.30,
        "color_integrity":   0.25,
        "moisture":          0.15,
        "foreign_material":  0.15,
        "protein":           0.15,
    },
    "BLKBN": {
        "size_uniformity":   0.30,
        "color_integrity":   0.25,
        "moisture":          0.15,
        "foreign_material":  0.15,
        "protein":           0.15,
    },
}

# ── Scoring band tables ───────────────────────────────────────────
# Each band: (input_lower, input_upper, score_lower, score_upper)
# Linear interpolation applied within each band.
# Lower anchor score at input_lower, upper anchor at input_upper.

SCORING_BANDS = {
    "size_uniformity": [
        (0.00, 0.80, 0.0,  40.0),
        (0.80, 0.88, 40.0, 65.0),
        (0.88, 0.93, 65.0, 82.0),
        (0.93, 0.97, 82.0, 93.0),
        (0.97, 1.00, 93.0, 100.0),
    ],
    "color_integrity": [
        (0.00, 0.75, 0.0,  35.0),
        (0.75, 0.85, 35.0, 62.0),
        (0.85, 0.92, 62.0, 80.0),
        (0.92, 0.97, 80.0, 93.0),
        (0.97, 1.00, 93.0, 100.0),
    ],
    "moisture": [
        # Lower moisture is better for pulse crops
        # Input is moisture percentage; lower = higher score
        (14.0, 100.0, 0.0,   0.0),
        (12.0,  14.0, 0.0,  40.0),
        (10.0,  12.0, 40.0, 70.0),
        ( 8.0,  10.0, 70.0, 88.0),
        ( 0.0,   8.0, 88.0, 100.0),
    ],
    "foreign_material": [
        # Lower foreign material is better
        # Input is foreign material percentage
        ( 2.0, 100.0, 0.0,   0.0),
        ( 1.0,   2.0, 0.0,  35.0),
        ( 0.5,   1.0, 35.0, 65.0),
        ( 0.2,   0.5, 65.0, 85.0),
        ( 0.0,   0.2, 85.0, 100.0),
    ],
    "protein": [
        ( 0.0, 18.0, 0.0,  30.0),
        (18.0, 21.0, 30.0, 55.0),
        (21.0, 24.0, 55.0, 75.0),
        (24.0, 27.0, 75.0, 90.0),
        (27.0, 35.0, 90.0, 100.0),
    ],
}


# ── Linear interpolation within band ─────────────────────────────
def interpolate_score(value: float, bands: list) -> float:
    """
    Apply linear interpolation within the matching scoring band.
    Formula: Score = Lower Anchor + ((Input - Band Lower) /
                     (Band Upper - Band Lower)) x (Upper Anchor - Lower Anchor)
    Returns 0.0 if value falls below all bands.
    Returns upper anchor of last band if value exceeds all bands.
    """
    for (input_lower, input_upper, score_lower, score_upper) in bands:
        if input_lower <= value <= input_upper:
            band_range  = input_upper - input_lower
            score_range = score_upper - score_lower
            if band_range == 0:
                return score_lower
            position = (value - input_lower) / band_range
            return round(score_lower + position * score_range, 4)

    # Value outside all bands -- clamp to nearest boundary
    if value < bands[0][0]:
        return bands[0][2]
    return bands[-1][3]


# ── Quality score computation ─────────────────────────────────────
def compute_quality_score(crop_code: str, measurements: dict) -> dict:
    """
    Compute weighted quality score from QC2 measurements.
    Returns quality score out of 100 plus full submetric breakdown.
    """
    weights  = CROP_WEIGHTS[crop_code]
    submetric_scores = {}
    quality_score    = 0.0

    for submetric, weight in weights.items():
        raw_value = measurements.get(submetric)
        if raw_value is None:
            raise ValueError(
                f"Missing measurement for submetric '{submetric}' "
                f"on crop '{crop_code}'"
            )
        bands = SCORING_BANDS[submetric]
        score = interpolate_score(float(raw_value), bands)
        submetric_scores[submetric] = {
            "raw_value":   raw_value,
            "score":       score,
            "weight":      weight,
            "contribution": round(score * weight, 4),
        }
        quality_score += score * weight

    return {
        "quality_score":    round(quality_score, 4),
        "submetric_scores": submetric_scores,
    }


# ── ESG status fetch ──────────────────────────────────────────────
def get_esg_status(fuid: str) -> bool:
    """
    Retrieve ESG certification status for the farmer from Aurora.
    Returns True if certified, False otherwise.
    ESG score is 100 for certified farmers and 0 otherwise.
    """
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT esg_certified FROM farmers
            WHERE fuid = :fuid
        """,
        parameters=[{
            "name": "fuid",
            "value": {"stringValue": fuid}
        }],
    )
    if not result["records"]:
        raise ValueError(f"FUID '{fuid}' not found in Aurora")
    return result["records"][0][0]["booleanValue"]


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by QC2_COMPLETE event from WMS.
    Computes VVI and writes to Aurora. Publishes VVI_COMPUTED to
    EventBridge. If quality score falls below floor of 60, publishes
    QUALITY_FLOOR_FAILED and rejects the lot.

    Expected event format:
    {
        "batch_id":   "LENT-0045364-260426-01",
        "crop_code":  "LENT",
        "fuid":       "0045364",
        "measurements": {
            "size_uniformity":  0.94,
            "color_integrity":  0.91,
            "moisture":         9.8,
            "foreign_material": 0.18,
            "protein":          25.4
        }
    }
    """
    batch_id     = event["batch_id"]
    crop_code    = event["crop_code"].upper()
    fuid         = event["fuid"]
    measurements = event["measurements"]

    if crop_code not in CROP_WEIGHTS:
        raise ValueError(
            f"Unsupported crop_code '{crop_code}'. "
            f"Supported: {sorted(CROP_WEIGHTS.keys())}"
        )

    # ── Compute quality score ─────────────────────────────────────
    quality_result = compute_quality_score(crop_code, measurements)
    quality_score  = quality_result["quality_score"]

    # ── Quality floor check ───────────────────────────────────────
    if quality_score < QUALITY_FLOOR:
        events_client.put_events(Entries=[{
            "Source":       "gant.vvi",
            "DetailType":   "QUALITY_FLOOR_FAILED",
            "EventBusName": EVENT_BUS,
            "Detail": json.dumps({
                "batch_id":     batch_id,
                "crop_code":    crop_code,
                "quality_score": quality_score,
                "floor":        QUALITY_FLOOR,
            }),
        }])
        logger.warning(
            f"BatchID {batch_id} failed quality floor: "
            f"{quality_score:.2f} < {QUALITY_FLOOR}"
        )
        return {
            "status":        "QUALITY_FLOOR_FAILED",
            "batch_id":      batch_id,
            "quality_score": quality_score,
            "floor":         QUALITY_FLOOR,
        }

    # ── Fixed component scores ────────────────────────────────────
    # Traceability and compliance score 100 on all listed lots
    # as a platform guarantee
    price_ref_score    = 100.0
    traceability_score = 100.0
    compliance_score   = 100.0

    # ESG scores 100 for certified farmers only
    esg_certified  = get_esg_status(fuid)
    esg_score      = 100.0 if esg_certified else 0.0

    # ── VVI formula ───────────────────────────────────────────────
    # VVI = 0.10 x PriceRef + 0.40 x Quality + 0.25 x Traceability
    #       + 0.20 x Compliance + 0.05 x ESG
    vvi_score = round(
        (0.10 * price_ref_score)  +
        (0.40 * quality_score)    +
        (0.25 * traceability_score) +
        (0.20 * compliance_score) +
        (0.05 * esg_score),
        4
    )

    vvi_timestamp = datetime.now(timezone.utc).isoformat()

    # ── Write to Aurora ───────────────────────────────────────────
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE batches SET
                vvi_score            = :vvi_score,
                quality_score        = :quality_score,
                esg_certified        = :esg_certified,
                esg_score            = :esg_score,
                submetric_scores     = :submetric_scores,
                status               = 'CLEANED_PENDING_BAGGING',
                vvi_computed_at      = :vvi_timestamp
            WHERE batch_id = :batch_id
        """,
        parameters=[
            {"name": "vvi_score",       "value": {"doubleValue": vvi_score}},
            {"name": "quality_score",   "value": {"doubleValue": quality_score}},
            {"name": "esg_certified",   "value": {"booleanValue": esg_certified}},
            {"name": "esg_score",       "value": {"doubleValue": esg_score}},
            {"name": "submetric_scores","value": {"stringValue": json.dumps(
                quality_result["submetric_scores"])}},
            {"name": "vvi_timestamp",   "value": {"stringValue": vvi_timestamp}},
            {"name": "batch_id",        "value": {"stringValue": batch_id}},
        ],
    )

    # ── Publish VVI_COMPUTED ──────────────────────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.vvi",
        "DetailType":   "VVI_COMPUTED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "batch_id":          batch_id,
            "crop_code":         crop_code,
            "vvi_score":         vvi_score,
            "quality_score":     quality_score,
            "esg_score":         esg_score,
            "vvi_computed_at":   vvi_timestamp,
        }),
    }])

    logger.info(
        f"VVI computed for {batch_id}: "
        f"VVI={vvi_score:.2f} Quality={quality_score:.2f} ESG={esg_score:.0f}"
    )

    return {
        "status":           "VVI_COMPUTED",
        "batch_id":         batch_id,
        "vvi_score":        vvi_score,
        "quality_score":    quality_score,
        "esg_score":        esg_score,
        "submetric_scores": quality_result["submetric_scores"],
        "vvi_computed_at":  vvi_timestamp,
    }
