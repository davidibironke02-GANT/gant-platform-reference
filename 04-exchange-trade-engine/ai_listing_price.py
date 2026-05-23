# 4.5 AI Listing Price Engine
# Lambda function computing the recommended listing price at VVI completion
# Formula: AI Price = Benchmark + $100 processing fee + Quality Premium
#          + Traceability Premium + Compliance Premium + ESG Premium
# Maximum premium above benchmark and processing fee: $200 CAD/MT
# All amounts in CAD per MT

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

# ── Fixed fee ────────────────────────────────────────────────────
PROCESSING_FEE = 100.00   # CAD/MT -- fixed platform fee

# ── Maximum premium pool distributed across four components ──────
# Distributed using VVI weights as proportions of the 90% subtotal
# excluding price reference (0.40 + 0.25 + 0.20 + 0.05 = 0.90)
MAX_QUALITY_PREMIUM      = 88.89   # 0.40 / 0.90 x 200
MAX_TRACEABILITY_PREMIUM = 55.56   # 0.25 / 0.90 x 200
MAX_COMPLIANCE_PREMIUM   = 44.44   # 0.20 / 0.90 x 200
MAX_ESG_PREMIUM          = 11.11   # 0.05 / 0.90 x 200

# ── Commodity benchmark cache ─────────────────────────────────────
# Saskatchewan pulse spot prices cached for 15 minutes
# Fetched from Saskatchewan Pulse Growers market data API
BENCHMARK_CACHE: dict = {}
CACHE_TTL_SECONDS = 900   # 15 minutes

# ── Crop benchmark API endpoint mapping ───────────────────────────
BENCHMARK_ENDPOINTS = {
    "LENT":  "/v1/prices/lentils/green",
    "CHKP":  "/v1/prices/chickpeas/kabuli",
    "NBEAN": "/v1/prices/beans/navy",
    "PINTO": "/v1/prices/beans/pinto",
    "BLKBN": "/v1/prices/beans/black",
}

# ── Fallback benchmarks (CAD/MT) used if API unavailable ─────────
FALLBACK_BENCHMARKS = {
    "LENT":  720.00,
    "CHKP":  610.00,
    "NBEAN": 480.00,
    "PINTO": 500.00,
    "BLKBN": 490.00,
}


# ── Benchmark fetch with 15-minute cache ──────────────────────────
def fetch_benchmark(crop_code: str) -> tuple:
    """
    Fetch live commodity benchmark from Saskatchewan pulse market API.
    Returns (benchmark_price, benchmark_timestamp, is_live).
    Falls back to static benchmark if API unavailable.
    Cache expires after 15 minutes.
    """
    now = datetime.now(timezone.utc).timestamp()

    cached = BENCHMARK_CACHE.get(crop_code)
    if cached and (now - cached["fetched_at"]) < CACHE_TTL_SECONDS:
        logger.info(f"Benchmark cache hit for {crop_code}")
        return (
            cached["price"],
            cached["timestamp"],
            cached["is_live"],
        )

    # Fetch from API
    spg_base_url = ssm_client.get_parameter(
        Name="/gant/external/spg_api_url")["Parameter"]["Value"]
    spg_api_key  = ssm_client.get_parameter(
        Name="/gant/external/spg_api_key",
        WithDecryption=True)["Parameter"]["Value"]

    endpoint = BENCHMARK_ENDPOINTS.get(crop_code)
    if not endpoint:
        raise ValueError(f"No benchmark endpoint for crop_code '{crop_code}'")

    try:
        import requests
        response = requests.get(
            f"{spg_base_url}{endpoint}",
            headers={"Authorization": f"Bearer {spg_api_key}"},
            timeout=5,
        )
        response.raise_for_status()
        data      = response.json()
        price     = float(data["price_cad_per_mt"])
        timestamp = data["price_timestamp"]
        is_live   = True

        BENCHMARK_CACHE[crop_code] = {
            "price":      price,
            "timestamp":  timestamp,
            "is_live":    True,
            "fetched_at": now,
        }
        logger.info(
            f"Live benchmark fetched for {crop_code}: "
            f"CAD {price:.2f}/MT at {timestamp}"
        )
        return price, timestamp, is_live

    except Exception as e:
        logger.warning(
            f"Benchmark API unavailable for {crop_code}: {e}. "
            f"Using fallback benchmark."
        )
        fallback  = FALLBACK_BENCHMARKS[crop_code]
        timestamp = datetime.now(timezone.utc).isoformat()
        return fallback, timestamp, False


# ── Premium computation ───────────────────────────────────────────
def compute_premiums(quality_score: float, esg_score: float) -> dict:
    """
    Compute the four premium components from VVI subscores.
    Traceability and compliance premiums are fixed platform guarantees.
    Quality and ESG premiums scale with their respective scores.
    """
    quality_premium      = round((quality_score / 100) * MAX_QUALITY_PREMIUM, 2)
    traceability_premium = round(MAX_TRACEABILITY_PREMIUM, 2)
    compliance_premium   = round(MAX_COMPLIANCE_PREMIUM, 2)
    esg_premium          = round((esg_score / 100) * MAX_ESG_PREMIUM, 2)

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
    Fetches live benchmark, computes premium stack, calculates
    AI recommended price and farmer net estimate, writes to Aurora,
    and publishes LISTING_PRICE_COMPUTED.

    Expected event format:
    {
        "batch_id":      "LENT-0045364-260426-01",
        "crop_code":     "LENT",
        "vvi_score":     87.42,
        "quality_score": 84.60,
        "esg_score":     100.0
    }
    """
    batch_id      = event["batch_id"]
    crop_code     = event["crop_code"].upper()
    quality_score = float(event["quality_score"])
    esg_score     = float(event["esg_score"])

    if crop_code not in FALLBACK_BENCHMARKS:
        raise ValueError(f"Unsupported crop_code '{crop_code}'")

    # ── Fetch benchmark ───────────────────────────────────────────
    benchmark, benchmark_timestamp, is_live = fetch_benchmark(crop_code)

    # ── Compute premiums ──────────────────────────────────────────
    premiums = compute_premiums(quality_score, esg_score)

    # ── AI recommended price ──────────────────────────────────────
    # Benchmark + processing fee + full premium stack
    ai_price = round(
        benchmark +
        PROCESSING_FEE +
        premiums["total_premium"],
        2
    )

    # ── Farmer net estimate ───────────────────────────────────────
    # After platform 10% trade fee deducted from AI price,
    # then processing fee subtracted
    farmer_net_estimate = round(
        (ai_price * 0.90) - PROCESSING_FEE,
        2
    )

    computed_at = datetime.now(timezone.utc).isoformat()

    # ── Write to Aurora ───────────────────────────────────────────
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            UPDATE batches SET
                ai_recommended_price    = :ai_price,
                farmer_net_estimate     = :farmer_net,
                benchmark_price         = :benchmark,
                benchmark_timestamp     = :bench_ts,
                benchmark_is_live       = :is_live,
                processing_fee          = :proc_fee,
                quality_premium         = :qual_prem,
                traceability_premium    = :trace_prem,
                compliance_premium      = :comp_prem,
                esg_premium             = :esg_prem,
                listing_price_computed_at = :computed_at
            WHERE batch_id = :batch_id
        """,
        parameters=[
            {"name": "ai_price",    "value": {"doubleValue": ai_price}},
            {"name": "farmer_net",  "value": {"doubleValue": farmer_net_estimate}},
            {"name": "benchmark",   "value": {"doubleValue": benchmark}},
            {"name": "bench_ts",    "value": {"stringValue": benchmark_timestamp}},
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

    # ── Publish LISTING_PRICE_COMPUTED ────────────────────────────
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
            "benchmark_timestamp":  benchmark_timestamp,
            "benchmark_is_live":    is_live,
            "processing_fee":       PROCESSING_FEE,
            "premiums":             premiums,
            "computed_at":          computed_at,
        }),
    }])

    logger.info(
        f"Listing price computed for {batch_id}: "
        f"AI={ai_price:.2f} Farmer net={farmer_net_estimate:.2f} "
        f"Benchmark={benchmark:.2f} ({'live' if is_live else 'fallback'})"
    )

    return {
        "status":               "LISTING_PRICE_COMPUTED",
        "batch_id":             batch_id,
        "ai_recommended_price": ai_price,
        "farmer_net_estimate":  farmer_net_estimate,
        "benchmark":            benchmark,
        "benchmark_is_live":    is_live,
        "processing_fee":       PROCESSING_FEE,
        "premiums":             premiums,
        "computed_at":          computed_at,
    }
