# 5.3 FOREX Rate Fetch and Cache Logic
# Lambda function managing live exchange rate fetching and caching
# Primary source: Open Exchange Rates
# Fallback source: Fixer.io
# Cache: ElastiCache Redis with 5-minute TTL
# Stale rate holds bid -- no trade proceeds on expired rate

import json
import logging
import time
import boto3
import redis
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client = boto3.client("rds-data")
ssm_client    = boto3.client("ssm")

AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"

# ── ElastiCache Redis connection ─────────────────────────────────
REDIS_HOST    = ssm_client.get_parameter(
    Name="/gant/elasticache/host")["Parameter"]["Value"]
REDIS_PORT    = int(ssm_client.get_parameter(
    Name="/gant/elasticache/port")["Parameter"]["Value"])
CACHE_TTL     = 300   # 5 minutes -- absolute ceiling, non-negotiable

redis_client  = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    ssl=True,
)

# ── API configuration ─────────────────────────────────────────────
OXR_APP_ID    = ssm_client.get_parameter(
    Name="/gant/external/oxr_app_id",
    WithDecryption=True)["Parameter"]["Value"]
FIXER_API_KEY = ssm_client.get_parameter(
    Name="/gant/external/fixer_api_key",
    WithDecryption=True)["Parameter"]["Value"]

OXR_BASE_URL   = "https://openexchangerates.org/api/latest.json"
FIXER_BASE_URL = "https://data.fixer.io/api/latest"

# ── Status flags ──────────────────────────────────────────────────
STATUS_FRESH       = "FRESH"
STATUS_CACHED      = "CACHED"
STATUS_STALE       = "STALE"
STATUS_UNAVAILABLE = "UNAVAILABLE"


# ── Cache key construction ────────────────────────────────────────
def cache_key(from_currency: str, to_currency: str) -> str:
    return f"forex:{from_currency}:{to_currency}"


# ── Fetch from Open Exchange Rates ───────────────────────────────
def fetch_from_oxr(to_currency: str) -> float:
    """
    Fetch CAD to target currency rate from Open Exchange Rates.
    OXR returns rates relative to USD base. Convert via USD.
    """
    response = requests.get(
        OXR_BASE_URL,
        params={"app_id": OXR_APP_ID, "symbols": f"CAD,{to_currency}"},
        timeout=3,
    )
    response.raise_for_status()
    rates      = response.json()["rates"]
    cad_to_usd = 1 / rates["CAD"]
    usd_to_tgt = rates[to_currency]
    return round(cad_to_usd * usd_to_tgt, 6)


# ── Fetch from Fixer.io ───────────────────────────────────────────
def fetch_from_fixer(to_currency: str) -> float:
    """
    Fetch CAD to target currency rate from Fixer.io.
    Fixer returns rates relative to EUR base. Convert via EUR.
    """
    response = requests.get(
        FIXER_BASE_URL,
        params={
            "access_key": FIXER_API_KEY,
            "symbols":    f"CAD,{to_currency}",
        },
        timeout=3,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise ValueError(
            f"Fixer.io error: {data.get('error', 'unknown')}"
        )
    rates      = data["rates"]
    eur_to_cad = rates["CAD"]
    eur_to_tgt = rates[to_currency]
    return round(eur_to_tgt / eur_to_cad, 6)


# ── Write rate to ElastiCache ────────────────────────────────────
def write_to_cache(from_currency: str, to_currency: str,
                   rate: float, source: str) -> None:
    key     = cache_key(from_currency, to_currency)
    payload = json.dumps({
        "rate":       rate,
        "source":     source,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    })
    redis_client.setex(key, CACHE_TTL, payload)
    logger.info(
        f"Rate cached: {from_currency}/{to_currency} = "
        f"{rate} from {source}"
    )


# ── Read rate from ElastiCache ────────────────────────────────────
def read_from_cache(from_currency: str,
                    to_currency: str) -> dict | None:
    key     = cache_key(from_currency, to_currency)
    cached  = redis_client.get(key)
    if cached:
        return json.loads(cached)
    return None


# ── Read stale rate from Aurora audit log ────────────────────────
def read_stale_from_aurora(from_currency: str,
                            to_currency: str) -> dict | None:
    """
    Last-resort lookup. If ElastiCache is empty and both APIs
    are down, check Aurora for the most recently logged rate.
    Returns None if no prior rate exists for this pair.
    """
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT rate, source, fetched_at
            FROM forex_rate_log
            WHERE from_currency = :from_cur
            AND   to_currency   = :to_cur
            ORDER BY fetched_at DESC
            LIMIT 1
        """,
        parameters=[
            {"name": "from_cur", "value": {"stringValue": from_currency}},
            {"name": "to_cur",   "value": {"stringValue": to_currency}},
        ],
    )
    if not result["records"]:
        return None
    row = result["records"][0]
    return {
        "rate":       row[0]["doubleValue"],
        "source":     row[1]["stringValue"],
        "fetched_at": row[2]["stringValue"],
    }


# ── Log rate fetch event to Aurora ───────────────────────────────
def log_rate_to_aurora(from_currency: str, to_currency: str,
                        rate: float, source: str,
                        trade_id: str | None) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO forex_rate_log (
                from_currency, to_currency, rate,
                source, trade_id, fetched_at
            ) VALUES (
                :from_cur, :to_cur, :rate,
                :source, :trade_id, :fetched_at
            )
        """,
        parameters=[
            {"name": "from_cur",   "value": {"stringValue": from_currency}},
            {"name": "to_cur",     "value": {"stringValue": to_currency}},
            {"name": "rate",       "value": {"doubleValue": rate}},
            {"name": "source",     "value": {"stringValue": source}},
            {"name": "trade_id",   "value": {"stringValue": trade_id} if trade_id
                                             else {"isNull": True}},
            {"name": "fetched_at", "value": {
                "stringValue": datetime.now(timezone.utc).isoformat()}},
        ],
    )


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Fetches CAD to buyer local currency exchange rate.
    Returns rate with status flag indicating data freshness.
    Bid proceeds only on FRESH or CACHED status.
    STALE and UNAVAILABLE hold the bid and notify the buyer.

    Resolution order:
    1. ElastiCache -- within 5-minute TTL
    2. Open Exchange Rates live API
    3. Fixer.io live API
    4. Aurora last logged rate -- STALE status, bid held
    5. No rate available -- UNAVAILABLE, bid held

    Expected event format:
    {
        "from_currency": "CAD",
        "to_currency":   "INR",
        "trade_id":      "TRD-20260426-0012"  (optional)
    }
    """
    from_currency = event.get("from_currency", "CAD").upper()
    to_currency   = event["to_currency"].upper()
    trade_id      = event.get("trade_id")

    # ── 1. ElastiCache check ──────────────────────────────────────
    cached = read_from_cache(from_currency, to_currency)
    if cached:
        logger.info(
            f"Cache hit: {from_currency}/{to_currency} = "
            f"{cached['rate']} from {cached['source']}"
        )
        return {
            "rate":         cached["rate"],
            "source":       cached["source"],
            "fetched_at":   cached["fetched_at"],
            "status":       STATUS_CACHED,
            "bid_proceeds": True,
        }

    # ── 2. Open Exchange Rates ────────────────────────────────────
    try:
        rate = fetch_from_oxr(to_currency)
        write_to_cache(from_currency, to_currency, rate, "OXR")
        log_rate_to_aurora(from_currency, to_currency,
                           rate, "OXR", trade_id)
        logger.info(
            f"Fresh rate from OXR: "
            f"{from_currency}/{to_currency} = {rate}"
        )
        return {
            "rate":         rate,
            "source":       "OXR",
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "status":       STATUS_FRESH,
            "bid_proceeds": True,
        }
    except Exception as e:
        logger.warning(f"OXR unavailable: {e}. Trying Fixer.io.")

    # ── 3. Fixer.io fallback ──────────────────────────────────────
    try:
        rate = fetch_from_fixer(to_currency)
        write_to_cache(from_currency, to_currency, rate, "FIXER")
        log_rate_to_aurora(from_currency, to_currency,
                           rate, "FIXER", trade_id)
        logger.info(
            f"Fresh rate from Fixer.io: "
            f"{from_currency}/{to_currency} = {rate}"
        )
        return {
            "rate":         rate,
            "source":       "FIXER",
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "status":       STATUS_FRESH,
            "bid_proceeds": True,
        }
    except Exception as e:
        logger.warning(f"Fixer.io unavailable: {e}. "
                       f"Checking Aurora for stale rate.")

    # ── 4. Aurora stale rate -- bid held ──────────────────────────
    stale = read_stale_from_aurora(from_currency, to_currency)
    if stale:
        logger.error(
            f"Both FOREX APIs unavailable. Stale rate found in "
            f"Aurora for {from_currency}/{to_currency}: "
            f"{stale['rate']} from {stale['fetched_at']}. "
            f"Bid held."
        )
        return {
            "rate":         stale["rate"],
            "source":       stale["source"],
            "fetched_at":   stale["fetched_at"],
            "status":       STATUS_STALE,
            "bid_proceeds": False,
            "hold_reason":  "FOREX_APIS_UNAVAILABLE_STALE_RATE",
        }

    # ── 5. No rate available -- bid held ──────────────────────────
    logger.error(
        f"No FOREX rate available for "
        f"{from_currency}/{to_currency}. Bid held."
    )
    return {
        "rate":         None,
        "source":       None,
        "fetched_at":   None,
        "status":       STATUS_UNAVAILABLE,
        "bid_proceeds": False,
        "hold_reason":  "FOREX_RATE_UNAVAILABLE",
    }
