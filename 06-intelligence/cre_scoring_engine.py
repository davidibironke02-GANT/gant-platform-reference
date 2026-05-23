# 6.2 CRE Scoring Engine
# Lambda function computing the Corridor Risk Engine composite score
# Three domains: Tariff (0.35), Environmental (0.25), Geopolitical (0.40)
# Source hierarchy and weighted conflict resolution per Appendix C
# Linear interpolation within all scoring bands
# 72-hour WTO data freshness check with manual override protocol

import json
import logging
import boto3
import requests
from datetime import datetime, timezone, timedelta

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
    "tariff":        0.35,
    "environmental": 0.25,
    "geopolitical":  0.40,
}

# ── CRE decision thresholds ───────────────────────────────────────
CRE_PASS_THRESHOLD  = 31   # 0-30 Green: pass
CRE_BLOCK_THRESHOLD = 76   # 76-100 Red: block

# ── WTO data freshness ceiling ────────────────────────────────────
WTO_MAX_AGE_HOURS = 72

# ── Source precedence weights per domain ─────────────────────────
TARIFF_WEIGHTS = {
    "wto_api":              0.70,
    "destination_customs":  0.30,
}
ENVIRONMENTAL_WEIGHTS = {
    "copernicus":           0.60,
    "noaa":                 0.40,
}
GEOPOLITICAL_WEIGHTS = {
    "acled":                0.45,
    "imo":                  0.40,
    "un_ocha":              0.15,
}

# ── Hard override thresholds ──────────────────────────────────────
GEOPOLITICAL_HARD_OVERRIDE_THRESHOLD = 20
LLOYDS_ESCALATION_FLOOR              = 55

# ── Scoring band tables with linear interpolation ────────────────
# Format: (input_lower, input_upper, score_lower, score_upper)

TARIFF_BANDS = [
    (0.00,  5.00,  5.0, 10.0),
    (5.00, 10.00, 10.0, 25.0),
    (10.0, 20.00, 25.0, 50.0),
    (20.0, 30.00, 50.0, 75.0),
    (30.0, 100.0, 75.0, 100.0),
]

ENVIRONMENTAL_BANDS = [
    (0,  1,  0.0,  15.0),   # No advisory
    (1,  2, 15.0,  40.0),   # Watch issued
    (2,  3, 40.0,  65.0),   # Warning issued
    (3,  4, 65.0,  85.0),   # Severe warning
    (4,  5, 85.0, 100.0),   # Emergency declaration
]

GEOPOLITICAL_BANDS = [
    (0,  1,  0.0, 15.0),    # No conflict signals
    (1,  2, 15.0, 35.0),    # Low-level advisory
    (2,  3, 35.0, 60.0),    # Active advisory
    (3,  4, 60.0, 80.0),    # Elevated conflict
    (4,  5, 80.0, 100.0),   # Active conflict confirmed
]


# ── Linear interpolation within band ─────────────────────────────
def interpolate_score(value: float, bands: list) -> float:
    """
    Score = Lower Anchor + ((Input - Band Lower) /
            (Band Upper - Band Lower)) x (Upper Anchor - Lower Anchor)
    Deterministic -- identical inputs always produce identical output.
    """
    for (il, iu, sl, su) in bands:
        if il <= value <= iu:
            band_range  = iu - il
            score_range = su - sl
            if band_range == 0:
                return sl
            return round(sl + ((value - il) / band_range) * score_range, 4)
    if value < bands[0][0]:
        return bands[0][2]
    return bands[-1][3]


# ── Weighted effective input ──────────────────────────────────────
def weighted_input(signals: dict, weights: dict) -> float:
    """
    Effective Input = sum(source_weight x source_signal)
    Applies source precedence model from Appendix C Section 3.2.
    """
    total = 0.0
    for source, weight in weights.items():
        signal = signals.get(source, 0.0)
        total += weight * signal
    return round(total, 4)


# ── WTO API freshness check ───────────────────────────────────────
def check_wto_freshness(corridor_code: str) -> tuple:
    """
    Verify cached WTO tariff rate is within 72-hour freshness window.
    Returns (rate, fetched_at, is_fresh, is_manual_override).
    If stale and API unavailable: bid is held.
    """
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT tariff_rate, fetched_at,
                   is_manual_override, override_source
            FROM wto_tariff_cache
            WHERE corridor_code = :corridor_code
            ORDER BY fetched_at DESC
            LIMIT 1
        """,
        parameters=[{
            "name": "corridor_code",
            "value": {"stringValue": corridor_code}
        }],
    )

    if not result["records"]:
        return None, None, False, False

    row          = result["records"][0]
    rate         = row[0]["doubleValue"]
    fetched_at   = datetime.fromisoformat(
        row[1]["stringValue"].replace("Z", "+00:00"))
    is_override  = row[2]["booleanValue"]
    age_hours    = (datetime.now(timezone.utc) - fetched_at
                    ).total_seconds() / 3600

    # Manual overrides expire after 96 hours
    override_ttl = 96 if is_override else WTO_MAX_AGE_HOURS
    is_fresh     = age_hours <= override_ttl

    return rate, fetched_at, is_fresh, is_override


# ── Fetch live WTO tariff ─────────────────────────────────────────
def fetch_wto_tariff(corridor_code: str,
                     commodity_hs_code: str) -> float:
    wto_api_url = ssm_client.get_parameter(
        Name="/gant/external/wto_api_url")["Parameter"]["Value"]
    wto_api_key = ssm_client.get_parameter(
        Name="/gant/external/wto_api_key",
        WithDecryption=True)["Parameter"]["Value"]

    response = requests.get(
        f"{wto_api_url}/tariffs",
        params={
            "corridor": corridor_code,
            "hs_code":  commodity_hs_code,
        },
        headers={"Authorization": f"Bearer {wto_api_key}"},
        timeout=5,
    )
    response.raise_for_status()
    data = response.json()
    rate = float(data["applied_tariff_rate_pct"])

    # Store to cache
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO wto_tariff_cache (
                corridor_code, tariff_rate, fetched_at,
                is_manual_override
            ) VALUES (
                :corridor_code, :rate, :fetched_at, false
            )
            ON CONFLICT (corridor_code)
            DO UPDATE SET
                tariff_rate        = EXCLUDED.tariff_rate,
                fetched_at         = EXCLUDED.fetched_at,
                is_manual_override = false
        """,
        parameters=[
            {"name": "corridor_code", "value": {"stringValue": corridor_code}},
            {"name": "rate",          "value": {"doubleValue": rate}},
            {"name": "fetched_at",    "value": {
                "stringValue": datetime.now(timezone.utc).isoformat()}},
        ],
    )
    return rate


# ── Fetch external domain signals ─────────────────────────────────
def fetch_environmental_signals(corridor_code: str) -> dict:
    """
    Fetch Copernicus and NOAA environmental severity signals.
    Returns normalised severity on 0-5 scale per source.
    """
    cop_api  = ssm_client.get_parameter(
        Name="/gant/external/copernicus_api_url")["Parameter"]["Value"]
    noaa_api = ssm_client.get_parameter(
        Name="/gant/external/noaa_api_url")["Parameter"]["Value"]

    signals = {}
    try:
        r = requests.get(f"{cop_api}/severity/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["copernicus"] = float(r.json()["severity_level"])
    except Exception as e:
        logger.warning(f"Copernicus unavailable: {e}")
        signals["copernicus"] = 0.0

    try:
        r = requests.get(f"{noaa_api}/advisory/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["noaa"] = float(r.json()["advisory_level"])
    except Exception as e:
        logger.warning(f"NOAA unavailable: {e}")
        signals["noaa"] = 0.0

    return signals


def fetch_geopolitical_signals(corridor_code: str) -> dict:
    """
    Fetch ACLED, IMO, UN OCHA, and Lloyd's geopolitical signals.
    Returns normalised conflict level on 0-5 scale per source.
    Lloyd's returns boolean -- True triggers escalation floor.
    """
    acled_api = ssm_client.get_parameter(
        Name="/gant/external/acled_api_url")["Parameter"]["Value"]
    imo_api   = ssm_client.get_parameter(
        Name="/gant/external/imo_api_url")["Parameter"]["Value"]
    ocha_api  = ssm_client.get_parameter(
        Name="/gant/external/ocha_api_url")["Parameter"]["Value"]

    signals = {
        "acled":   0.0,
        "imo":     0.0,
        "un_ocha": 0.0,
        "lloyds_active_attack": False,
    }

    try:
        r = requests.get(f"{acled_api}/conflict/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["acled"] = float(r.json()["conflict_level"])
    except Exception as e:
        logger.warning(f"ACLED unavailable: {e}")

    try:
        r = requests.get(f"{imo_api}/advisory/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["imo"] = float(r.json()["advisory_level"])
    except Exception as e:
        logger.warning(f"IMO unavailable: {e}")

    try:
        r = requests.get(f"{ocha_api}/status/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["un_ocha"] = float(r.json()["threat_level"])
    except Exception as e:
        logger.warning(f"UN OCHA unavailable: {e}")

    # Lloyd's escalation check -- separate from scoring weight
    try:
        lloyds_api = ssm_client.get_parameter(
            Name="/gant/external/lloyds_api_url")["Parameter"]["Value"]
        r = requests.get(f"{lloyds_api}/attacks/{corridor_code}",
                         timeout=5)
        r.raise_for_status()
        signals["lloyds_active_attack"] = r.json().get(
            "active_attack", False)
    except Exception as e:
        logger.warning(f"Lloyd's unavailable: {e}")

    return signals


# ── Domain score computation ──────────────────────────────────────
def compute_tariff_domain(tariff_rate: float,
                           customs_rate: float) -> dict:
    signals       = {"wto_api": tariff_rate,
                     "destination_customs": customs_rate}
    eff_input     = weighted_input(signals, TARIFF_WEIGHTS)
    domain_score  = interpolate_score(eff_input, TARIFF_BANDS)
    return {
        "effective_input": eff_input,
        "domain_score":    domain_score,
        "signals":         signals,
    }


def compute_environmental_domain(signals: dict) -> dict:
    eff_input    = weighted_input(signals, ENVIRONMENTAL_WEIGHTS)
    domain_score = interpolate_score(eff_input, ENVIRONMENTAL_BANDS)
    return {
        "effective_input": eff_input,
        "domain_score":    domain_score,
        "signals":         signals,
    }


def compute_geopolitical_domain(signals: dict) -> dict:
    """
    Applies Lloyd's hard escalation floor if active attack reported.
    Geopolitical hard override: if effective input maps below score
    threshold of 20 but Lloyd's reports active attack, floor at 55.
    """
    geo_signals  = {k: v for k, v in signals.items()
                    if k in GEOPOLITICAL_WEIGHTS}
    eff_input    = weighted_input(geo_signals, GEOPOLITICAL_WEIGHTS)
    domain_score = interpolate_score(eff_input, GEOPOLITICAL_BANDS)

    lloyds_active = signals.get("lloyds_active_attack", False)
    escalated     = False

    if lloyds_active and domain_score < LLOYDS_ESCALATION_FLOOR:
        domain_score = LLOYDS_ESCALATION_FLOOR
        escalated    = True
        logger.warning(
            f"Lloyd's active attack signal -- geopolitical domain "
            f"score floored at {LLOYDS_ESCALATION_FLOOR}"
        )

    return {
        "effective_input":  eff_input,
        "domain_score":     domain_score,
        "signals":          signals,
        "lloyds_escalated": escalated,
    }


# ── CRE composite score ───────────────────────────────────────────
def compute_cre_composite(tariff_score: float,
                           env_score: float,
                           geo_score: float) -> float:
    return round(
        DOMAIN_WEIGHTS["tariff"]        * tariff_score +
        DOMAIN_WEIGHTS["environmental"] * env_score    +
        DOMAIN_WEIGHTS["geopolitical"]  * geo_score,
        4
    )


# ── CRE decision ──────────────────────────────────────────────────
def cre_decision(composite: float) -> str:
    if composite <= 30:
        return "PASS_GREEN"
    elif composite <= 60:
        return "PASS_YELLOW"
    elif composite <= 75:
        return "PASS_ORANGE"
    else:
        return "BLOCK_RED"


# ── Log computation to Aurora ─────────────────────────────────────
def log_cre_computation(trade_id: str, corridor_code: str,
                         composite: float, decision: str,
                         domain_breakdown: dict,
                         computed_at: str,
                         logic_version: str) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO cre_log (
                trade_id, corridor_code, composite_score,
                decision, domain_breakdown, computed_at,
                logic_version
            ) VALUES (
                :trade_id, :corridor_code, :composite,
                :decision, :breakdown, :computed_at,
                :logic_version
            )
        """,
        parameters=[
            {"name": "trade_id",      "value": {"stringValue": trade_id}},
            {"name": "corridor_code", "value": {"stringValue": corridor_code}},
            {"name": "composite",     "value": {"doubleValue": composite}},
            {"name": "decision",      "value": {"stringValue": decision}},
            {"name": "breakdown",     "value": {"stringValue": json.dumps(domain_breakdown)}},
            {"name": "computed_at",   "value": {"stringValue": computed_at}},
            {"name": "logic_version", "value": {"stringValue": logic_version}},
        ],
    )


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered at bid submission. Computes CRE composite score
    across three domains using weighted source hierarchy and
    linear band interpolation. Publishes CRE_DECISION to
    EventBridge. Holds bid if WTO data is stale.

    Expected event format:
    {
        "trade_id":         "TRD-20260426-0012",
        "corridor_code":    "CA-IN",
        "commodity_hs":     "0713.40",
        "customs_rate":     8.5,
        "logic_version":    "CRE-1.0.0"
    }
    """
    trade_id       = event["trade_id"]
    corridor_code  = event["corridor_code"]
    commodity_hs   = event["commodity_hs"]
    customs_rate   = float(event.get("customs_rate", 0.0))
    logic_version  = event.get("logic_version", "CRE-1.0.0")
    computed_at    = datetime.now(timezone.utc).isoformat()

    # ── WTO freshness check ───────────────────────────────────────
    cached_rate, fetched_at, is_fresh, is_override = \
        check_wto_freshness(corridor_code)

    if cached_rate is not None and is_fresh:
        wto_rate   = cached_rate
        wto_source = "MANUAL_OVERRIDE" if is_override else "WTO_CACHE"
    else:
        # Attempt live fetch
        try:
            wto_rate   = fetch_wto_tariff(corridor_code, commodity_hs)
            wto_source = "WTO_LIVE"
        except Exception as e:
            logger.error(
                f"WTO API unavailable and cache stale for "
                f"{corridor_code}: {e}. Bid held."
            )
            events_client.put_events(Entries=[{
                "Source":       "gant.cre",
                "DetailType":   "BID_HELD_WTO_STALE",
                "EventBusName": EVENT_BUS,
                "Detail": json.dumps({
                    "trade_id":     trade_id,
                    "reason":       "WTO_DATA_STALE",
                    "computed_at":  computed_at,
                }),
            }])
            return {
                "status":     "BID_HELD",
                "trade_id":   trade_id,
                "reason":     "WTO_DATA_STALE",
                "computed_at": computed_at,
            }

    # ── Fetch domain signals ──────────────────────────────────────
    env_signals = fetch_environmental_signals(corridor_code)
    geo_signals = fetch_geopolitical_signals(corridor_code)

    # ── Compute domain scores ─────────────────────────────────────
    tariff_result = compute_tariff_domain(wto_rate, customs_rate)
    env_result    = compute_environmental_domain(env_signals)
    geo_result    = compute_geopolitical_domain(geo_signals)

    # ── Composite score ───────────────────────────────────────────
    composite = compute_cre_composite(
        tariff_result["domain_score"],
        env_result["domain_score"],
        geo_result["domain_score"],
    )
    decision  = cre_decision(composite)

    domain_breakdown = {
        "tariff":        tariff_result,
        "environmental": env_result,
        "geopolitical":  geo_result,
        "wto_source":    wto_source,
    }

    # ── Log to Aurora ─────────────────────────────────────────────
    log_cre_computation(trade_id, corridor_code, composite,
                        decision, domain_breakdown,
                        computed_at, logic_version)

    # ── Publish CRE_DECISION ──────────────────────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.cre",
        "DetailType":   "CRE_DECISION",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":        trade_id,
            "corridor_code":   corridor_code,
            "composite_score": composite,
            "decision":        decision,
            "domain_breakdown":domain_breakdown,
            "computed_at":     computed_at,
            "logic_version":   logic_version,
        }),
    }])

    logger.info(
        f"CRE computed for {trade_id}: "
        f"composite={composite:.2f} decision={decision} "
        f"corridor={corridor_code}"
    )

    return {
        "status":          "CRE_DECISION",
        "trade_id":        trade_id,
        "corridor_code":   corridor_code,
        "composite_score": composite,
        "decision":        decision,
        "domain_breakdown":domain_breakdown,
        "computed_at":     computed_at,
    }
