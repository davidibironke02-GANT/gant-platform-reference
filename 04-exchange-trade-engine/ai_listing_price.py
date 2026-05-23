# 4.5 AI Listing Price Engine
# Lambda function computing the recommended listing price at VVI completion
# Formula: AI Price = Benchmark + $100 processing fee + Quality Premium
#          + Traceability Premium + Compliance Premium + ESG Premium
# Benchmark source: Saskatchewan pulse spot market (weekly publication)
# Cache: 15-minute in-memory, Aurora weekly price on API failure
# All amounts in CAD per MT

import json
import logging
import boto3
import requests
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

# ── Fixed fee ────────────────────────────────────────────────────
PROCESSING_FEE = 100.00   # CAD/MT

# ── Maximum premium pool: $200 CAD/MT distributed by VVI weight ──
# Weights as proportions of the 90% quality-side subtotal
# (0.40 + 0.25 + 0.20 + 0.05 = 0.90)
MAX_QUALITY_PREMIUM      = 88.89   # 0.40 / 0.90 x 200
MAX_TRACEABILITY_PREMIUM = 55.56   # 0.25 / 0.90 x 200
MAX_COMPLIANCE_PREMIUM   = 44.44   # 0.20 / 0.90 x 200
MAX_ESG_PREMIUM          = 11.11   # 0.05 / 0.90 x 200

# ── In-memory benchmark cache (15-minute TTL) ────────────────────
BENCHMARK_CACHE: dict = {}
CACHE_TTL_SECONDS      = 900   # 15 minutes

# ── Crop endpoint mapping ─────────────────────────────────────────
BENCHMARK_ENDPOINTS = {
    "LENT":  "/v1/prices/lentils/green",
    "CHKP":  "/v1/prices/chickpeas/kabuli",
    "NBEAN": "/v1/prices/beans/navy",
    "PINTO": "/v1/prices/beans/pinto",
    "BLKBN": "/v1/prices/beans/black",
}


# ── Store weekly benchmark to Aurora ─────────────────────────────
def store_weekly_benchmark(crop_code: str, price: float,
                            price_week: str) -> None:
    """
    Persist the fetched weekly price to Aurora benchmark_cache.
    price_week is the ISO date of the week the price relates to
    e.g. "2026-04-21" for the week of 21 April 2026.
    Uses upsert so only the most recent weekly record is kept
    per crop per week.
    """
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO benchmark_cache
                (crop_code, benchmark_price, price_week, fetched_at)
            VALUES (:crop_code, :price, :price_week, :fetched_at)
            ON CONFLICT (crop_code, price_week)
            DO UPDATE SET
                benchmark_price = EXCLUDED.benchmark_price,
                fetched_at      = EXCLUDED.fetched_at
        """,
        parameters=[
            {"name": "crop_code",  "value": {"stringValue": crop_code}},
            {"name": "price",      "value": {"doubleValue": price}},
            {"name": "price_week", "value": {"stringValue": price_week}},
            {"name": "fetched_at", "value": {
                "stringValue": datetime.now(timezone.utc).isoformat()}},
        ],
    )


# ── Fetch last weekly price from Aurora ───────────────────────────
def fetch_last_weekly_benchmark(crop_code: str) -> tuple:
    """
    Retrieve the most recently stored weekly benchmark price from
    Aurora. Used only when the live API is unavailable.
    Returns (price, price_week, is_live=False).
    Raises RuntimeError if Aurora has no record for this crop --
    listing is held and operations team alerted.
    """
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT benchmark_price, price_week
            FROM benchmark_cache
            WHERE crop_code = :crop_code
            ORDER BY price_week DESC
            LIMIT 1
        """,
        parameters=[{
            "name": "crop_code",
            "value": {"stringValue": crop_code}
        }],
    )
    if not result["records"]:
        raise RuntimeError(
            f"No weekly benchmark available in Aurora for crop "
            f"'{crop_code}'. Live API unavailable and no prior "
            f"weekly price has been stored. Listing held pending "
            f"benchmark data recovery."
        )
    price      = result["records"][0][0]["doubleValue"]
    price_week = result["records"][0][1]["stringValue"]
    logger.warning(
        f"Live API unavailable for {crop_code}. Using Aurora "
        f"weekly price: CAD {price:.2f}/MT for week of {price_week}"
    )
    return price, price_week, False


# ── Benchmark fetch ───────────────────────────────────────────────
def fetch_benchmark(crop_code: str) -> tuple:
    """
    Fetch Saskatchewan pulse spot price.
    Returns (price, price_week, is_live).

    Resolution order:
    1. In-memory cache if within 15-minute TTL
    2. Live API call -- store to Aurora weekly cache on success
    3. Aurora last weekly price if API unavailable
    4. RuntimeError if Aurora cache empty -- listing held
    """
    now = datetime.now(timezone.utc).timestamp()

    # 1. In-memory cache
    cached = BENCHMARK_CACHE.get(crop_code)
    if cached and (now - cached["fetched_at"]) < CACHE_TTL_SECONDS:
        logger.info(f"In-memory benchmark cache hit for {crop_code}")
        return (
            cached["price"],
            cached["price_week"],
            cached["is_live"],
        )

    # 2. Live API call
    api_base = ssm_client.get_parameter(
        Name="/gant/external/pulse_market_api_url")["Parameter"]["Value"]
    api_key  = ssm_client.get_parameter(
        Name="/gant/external/pulse_market_api_key",
        WithDecryption=True)["Parameter"]["Value"]

    endpoint = BENCHMARK_ENDPOINTS.get(crop_code)
    if not endpoint:
        raise ValueError(
            f"No benchmark endpoint configured for crop '{crop_code}'"
        )

    try:
        response = requests.get(
            f"{api_base}{endpoint}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=5,
        )
        response.raise_for_status()
        data       = response.json()
        price      = float(data["price_cad_per_mt"])
        price_week = data["price_week"]   # ISO date of weekly release

        store_weekly_benchmark(crop_code, price, price_week)

        BENCHMARK_CACHE[crop_code] = {
            "price":      price,
            "price_week": price_week,
            "is_live":    True,
            "fetched_at": now,
        }
        logger.info(
            f"Live benchmark fetched for {crop_code}: "
            f"CAD {price:.2f}/MT for week of {price_week}"
        )
        return price, price_week, True

    except Exception as e:
        logger.warning(
            f"Pulse market API unavailable for {crop_code}: {e}. "
            f"Falling back to Aurora last weekly price."
        )
        # 3. Aurora last weekly -- raises if empty
        return fetch_last_weekly_benchmark(crop_code)


# ── Premium computation ───────────────────────────────────────────
def compute_premiums(quality_score: float,
                     esg_score: float) -> dict:
    quality_premium      = round(
        (quality_score / 100) * MAX_QUALITY_PREMIUM, 2)
    traceability_premium = round(MAX_TRACEABILITY_PREMIUM, 2)
    compliance_premium   = round(MAX_COMPLIANCE_PREMIUM, 2)
    esg_premium          = round(
        (esg_score / 100) * MAX_ESG_PREMIUM, 2)

    return {
        "quality_premium":      quality_premium,
        "traceability_premium": traceability_premium,
        "compliance_premium":   compliance_premium,
        "esg_premium":          esg_premium,
        "total_premium":        round(
            quality_premium + traceability_premium +
            compliance_premium + esg_premium, 2
        ),
    }


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by VVI_COMPUTED event from EventBridge.
    Fetches pulse spot benchmark, computes premium stack,
    calculates AI recommended price and farmer net estimate,
    writes to Aurora, and publishes LISTING_PRICE_COMPUTED.

    The response includes price_week so the farmer interface
    can display whether the recommendation is based on this
    week's price or the most recent prior weekly price,
    giving the farmer full transparency before they set their
    own listing price.

    Expected event format:
    {
        "batch_id":      "LENT-0045364-260426-01",
        "crop_code":     "LENT",
        "quality_score": 84.60,
        "esg_score":     100.0
    }
    """
    batch_id      = event["batch_id"]
    crop_code     = event["crop_code"].upper()
    quality_score = float(event["quality_score"])
    esg_score     = float(event["esg_score"])

    if crop_code not in BENCHMARK_ENDPOINTS:
        raise ValueError(
            f"Unsupported crop_code '{crop_code}'. "
            f"Supported: {sorted(BENCHMARK_ENDPOINTS.keys())}"
        )

    # Fetch benchmark -- raises RuntimeError if both API
    # and Aurora cache are empty, holding the listing
    benchmark, price_week, is_live = fetch_benchmark(crop_code)

    premiums = compute_premiums(quality_score, esg_score)

    ai_price = round(
        benchmark + PROCESSING_FEE + premiums["total_premium"], 2)

    # Farmer net: AI price less 10% trade fee less processing fee
    farmer_net_estimate = round(
        (ai_price * 0.90) - PROCESSING_FEE, 2)

    computed_at = datetime.now(timezone.utc).isoformat()

    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE batches SET
                ai_recommended_price      = :ai_price,
                farmer_net_estimate       = :farmer_net,
                benchmark_price           = :benchmark,
                benchmark_price_week      = :price_week,
                benchmark_is_live         = :is_live,
                processing_fee            = :proc_fee,
                quality_premium           = :qual_prem,
                traceability_premium      = :trace_prem,
                compliance_premium        = :comp_prem,
                esg_premium               = :esg_prem,
                listing_price_computed_at = :computed_at
            WHERE batch_id = :batch_id
        """,
        parameters=[
            {"name": "ai_price",    "value": {"doubleValue": ai_price}},
            {"name": "farmer_net",  "value": {"doubleValue": farmer_net_estimate}},
            {"name": "benchmark",   "value": {"doubleValue": benchmark}},
            {"name": "price_week",  "value": {"stringValue": price_week}},
            {"name": "is_live",     "value": {"booleanValue": is_live}},
            {"name": "proc_fee",    "value": {"doubleValue": PROCESSING_FEE}},
            {"name": "qual_prem",   "value": {"doubleValue": premiums["quality_premium"]}},
            {"name": "trace_prem",  "value": {"doubleValue": premiums["traceability_premium"]}},
            {"name": "comp_prem",   "value": {"doubleValue": premiums["compliance_premium"]}},
            {"name": "esg_prem",    "value": {"doubleValue": premiums["esg_premium"]}},
            {"name": "computed_at", "value": {"stringValue": computed_at}},
            {"name": "batch_id",    "value": {"stringValue": batch_id}},
        ],
    )

    events_client.put_events(Entries=[{
        "Source":       "gant.listing",
        "DetailType":   "LISTING_PRICE_COMPUTED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "batch_id":             batch_id,
            "crop_code":            crop_code,
            "ai_recommended_price": ai_price,
            "farmer_net_estimate":  farmer_net_estimate,
            "benchmark":            benchmark,
            "benchmark_price_week": price_week,
            "benchmark_is_live":    is_live,
            "processing_fee":       PROCESSING_FEE,
            "premiums":             premiums,
            "computed_at":          computed_at,
        }),
    }])

    logger.info(
        f"Listing price computed for {batch_id}: "
        f"AI={ai_price:.2f} Farmer net={farmer_net_estimate:.2f} "
        f"Benchmark={benchmark:.2f} week of {price_week} "
        f"({'live' if is_live else 'last weekly'})"
    )

    return {
        "status":               "LISTING_PRICE_COMPUTED",
        "batch_id":             batch_id,
        "ai_recommended_price": ai_price,
        "farmer_net_estimate":  farmer_net_estimate,
        "benchmark":            benchmark,
        "benchmark_price_week": price_week,
        "benchmark_is_live":    is_live,
        "processing_fee":       PROCESSING_FEE,
        "premiums":             premiums,
        "computed_at":          computed_at,
    }
