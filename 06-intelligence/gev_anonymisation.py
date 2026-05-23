# 6.6 GEV Anonymisation Pipeline
# Nightly Lambda accumulating anonymised corridor intelligence
# from completed trades into the God Eye View dataset
# All farmer and buyer identity stripped before writing to S3
# Aggregated at corridor level -- no individual trade is recoverable
# Minimum batch threshold: 10 completed trades per corridor
# per nightly run before corridor data is written to dataset

import json
import logging
import hashlib
import boto3
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client = boto3.client("rds-data")
s3_client     = boto3.client("s3")
ssm_client    = boto3.client("ssm")

AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"

GEV_BUCKET    = ssm_client.get_parameter(
    Name="/gant/s3/gev_bucket")["Parameter"]["Value"]

# ── Minimum trade threshold before corridor is written ───────────
MIN_CORRIDOR_TRADES = 10

# ── Fields stripped entirely before anonymisation ────────────────
# These fields never appear in the GEV dataset under any condition
IDENTITY_FIELDS = {
    "fuid", "buid", "farmer_legal_name", "farmer_address",
    "buyer_legal_name", "buyer_address", "buyer_account_ref",
    "fintech_ref", "container_id", "device_esn",
    "qr_token", "trade_id", "batch_id", "batchlot_ids",
}


# ── Fetch completed trades from the past 24 hours ────────────────
def fetch_completed_trades(run_date: str) -> list:
    """
    Fetch all trades that reached COMPLETED status in the
    24-hour window ending at the nightly run timestamp.
    Returns list of trade records with all fields intact.
    Identity stripping happens in anonymise_trade().
    """
    window_start = (
        datetime.fromisoformat(run_date) - timedelta(hours=24)
    ).isoformat()

    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT
                t.trade_id, t.fuid, t.buid,
                t.corridor_code, t.crop_code,
                t.total_volume_mt, t.agreed_price_cad,
                t.trade_date, t.completed_at,
                b.vvi_score, b.quality_score,
                b.submetric_scores, b.esg_certified,
                b.benchmark_price, b.benchmark_price_week,
                d.aggregate_scores, d.band_sequence,
                d.peak_score, d.peak_band,
                d.total_cycles, d.geo_override_triggered,
                d.gap_cycles_total
            FROM trades t
            JOIN batches b          ON b.batch_id    = t.batch_id
            LEFT JOIN dsre_sessions d ON d.trade_id  = t.trade_id
            WHERE t.status       = 'COMPLETED'
            AND   t.completed_at >= :window_start
            AND   t.completed_at <  :run_date
        """,
        parameters=[
            {"name": "window_start",
             "value": {"stringValue": window_start}},
            {"name": "run_date",
             "value": {"stringValue": run_date}},
        ],
    )

    trades = []
    for row in result["records"]:
        trades.append({
            "trade_id":             row[0]["stringValue"],
            "fuid":                 row[1]["stringValue"],
            "buid":                 row[2]["stringValue"],
            "corridor_code":        row[3]["stringValue"],
            "crop_code":            row[4]["stringValue"],
            "total_volume_mt":      row[5]["doubleValue"],
            "agreed_price_cad":     row[6]["doubleValue"],
            "trade_date":           row[7]["stringValue"],
            "completed_at":         row[8]["stringValue"],
            "vvi_score":            row[9]["doubleValue"],
            "quality_score":        row[10]["doubleValue"],
            "submetric_scores":     json.loads(row[11]["stringValue"])
                                    if not row[11].get("isNull") else {},
            "esg_certified":        row[12]["booleanValue"]
                                    if not row[12].get("isNull") else False,
            "benchmark_price":      row[13]["doubleValue"]
                                    if not row[13].get("isNull") else None,
            "benchmark_price_week": row[14]["stringValue"]
                                    if not row[14].get("isNull") else None,
            "dsre_aggregate_scores":json.loads(row[15]["stringValue"])
                                    if not row[15].get("isNull") else [],
            "dsre_band_sequence":   json.loads(row[16]["stringValue"])
                                    if not row[16].get("isNull") else [],
            "dsre_peak_score":      row[17]["doubleValue"]
                                    if not row[17].get("isNull") else None,
            "dsre_peak_band":       row[18]["stringValue"]
                                    if not row[18].get("isNull") else None,
            "dsre_total_cycles":    int(row[19]["longValue"])
                                    if not row[19].get("isNull") else 0,
            "geo_override_triggered":row[20]["booleanValue"]
                                    if not row[20].get("isNull") else False,
            "gap_cycles_total":     int(row[21]["longValue"])
                                    if not row[21].get("isNull") else 0,
        })

    logger.info(f"Fetched {len(trades)} completed trades for GEV run")
    return trades


# ── Anonymise individual trade record ────────────────────────────
def anonymise_trade(trade: dict) -> dict:
    """
    Strips all identity fields and replaces trade_id,
    fuid, and buid with one-way SHA-256 salted hashes.
    The hash allows internal deduplication without
    enabling reverse lookup of any identity.
    No original identifier survives in the output.
    """
    salt = ssm_client.get_parameter(
        Name="/gant/gev/anonymisation_salt",
        WithDecryption=True)["Parameter"]["Value"]

    def salted_hash(value: str) -> str:
        return hashlib.sha256(
            f"{salt}{value}".encode("utf-8")
        ).hexdigest()[:16]

    anon = {k: v for k, v in trade.items()
            if k not in IDENTITY_FIELDS}

    # Replace identity references with opaque hashes
    anon["trade_ref"]  = salted_hash(trade["trade_id"])
    anon["farmer_ref"] = salted_hash(trade["fuid"])
    anon["buyer_ref"]  = salted_hash(trade["buid"])

    return anon


# ── Aggregate corridor statistics ────────────────────────────────
def aggregate_corridor(corridor_code: str,
                        trades: list) -> dict:
    """
    Computes corridor-level aggregates from anonymised trades.
    No individual trade is recoverable from the aggregate.
    Minimum 10 trades required before corridor is written.
    """
    volumes    = [t["total_volume_mt"] for t in trades]
    prices     = [t["agreed_price_cad"] for t in trades]
    vvi_scores = [t["vvi_score"] for t in trades]
    dsre_peaks = [t["dsre_peak_score"] for t in trades
                  if t["dsre_peak_score"] is not None]
    gap_totals = [t["gap_cycles_total"] for t in trades]

    return {
        "corridor_code":          corridor_code,
        "trade_count":            len(trades),
        "total_volume_mt":        round(sum(volumes), 2),
        "avg_volume_mt":          round(sum(volumes) / len(volumes), 2),
        "avg_price_cad":          round(sum(prices) / len(prices), 2),
        "price_range_cad": {
            "min": round(min(prices), 2),
            "max": round(max(prices), 2),
        },
        "avg_vvi_score":          round(sum(vvi_scores) / len(vvi_scores), 4),
        "vvi_range": {
            "min": round(min(vvi_scores), 4),
            "max": round(max(vvi_scores), 4),
        },
        "avg_dsre_peak_score":    round(sum(dsre_peaks) / len(dsre_peaks), 2)
                                  if dsre_peaks else None,
        "geo_override_rate":      round(
            sum(1 for t in trades if t["geo_override_triggered"])
            / len(trades), 4),
        "avg_gap_cycles":         round(sum(gap_totals) / len(gap_totals), 2),
        "esg_certified_rate":     round(
            sum(1 for t in trades if t["esg_certified"])
            / len(trades), 4),
        "crop_distribution":      {
            crop: sum(1 for t in trades if t["crop_code"] == crop)
            for crop in set(t["crop_code"] for t in trades)
        },
        "individual_trades":      [anonymise_trade(t) for t in trades],
    }


# ── Write corridor record to S3 ───────────────────────────────────
def write_to_gev(corridor_code: str, run_date: str,
                  corridor_data: dict) -> str:
    """
    Writes corridor GEV record to S3 with Object Lock.
    Key format: gev/YYYY/MM/DD/corridor_code.json
    Object Lock ensures records cannot be modified after write.
    """
    date_part = run_date[:10].replace("-", "/")
    key       = f"gev/{date_part}/{corridor_code}.json"

    payload   = {
        "gev_schema_version": "1.0",
        "run_date":           run_date,
        "corridor_code":      corridor_code,
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "data":               corridor_data,
    }

    s3_client.put_object(
        Bucket=GEV_BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=datetime(
            2046, 1, 1, tzinfo=timezone.utc),
    )

    logger.info(
        f"GEV record written for {corridor_code}: "
        f"{corridor_data['trade_count']} trades, "
        f"{corridor_data['total_volume_mt']:.0f} MT -- s3://{GEV_BUCKET}/{key}"
    )

    return key


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered nightly by EventBridge Scheduler at 02:00 UTC.
    Fetches all trades completed in the prior 24-hour window,
    strips all identity data, aggregates by corridor, and
    writes corridor GEV records to S3 with Object Lock.

    Corridors with fewer than 10 completed trades in the window
    are held and accumulated across subsequent nightly runs
    until the threshold is reached. This prevents any corridor
    record from being statistically re-identifiable from a
    very small sample.

    Expected event format:
    {
        "run_date": "2026-04-27T02:00:00Z"
    }
    """
    run_date = event.get(
        "run_date",
        datetime.now(timezone.utc).isoformat()
    )

    # ── Fetch completed trades ────────────────────────────────────
    trades = fetch_completed_trades(run_date)

    if not trades:
        logger.info("No completed trades in window. GEV run complete.")
        return {
            "status":          "NO_TRADES",
            "run_date":        run_date,
            "corridors_written": 0,
        }

    # ── Group by corridor ─────────────────────────────────────────
    by_corridor: dict = {}
    for trade in trades:
        corridor = trade["corridor_code"]
        by_corridor.setdefault(corridor, []).append(trade)

    # ── Process each corridor ─────────────────────────────────────
    written      = []
    held         = []
    gev_manifest = []

    for corridor_code, corridor_trades in by_corridor.items():

        # ── Check pending accumulation from prior runs ────────────
        pending = fetch_pending_corridor_trades(
            corridor_code, run_date)
        all_trades = corridor_trades + pending

        if len(all_trades) < MIN_CORRIDOR_TRADES:
            # Accumulate -- do not write yet
            store_pending_trades(
                corridor_code, corridor_trades, run_date)
            held.append({
                "corridor_code":   corridor_code,
                "trade_count":     len(all_trades),
                "threshold":       MIN_CORRIDOR_TRADES,
            })
            logger.info(
                f"Corridor {corridor_code} held: "
                f"{len(all_trades)} trades below "
                f"threshold of {MIN_CORRIDOR_TRADES}"
            )
            continue

        # ── Aggregate and write ───────────────────────────────────
        corridor_data = aggregate_corridor(
            corridor_code, all_trades)
        s3_key        = write_to_gev(
            corridor_code, run_date, corridor_data)

        # Clear pending accumulation for this corridor
        clear_pending_trades(corridor_code)

        written.append(corridor_code)
        gev_manifest.append({
            "corridor_code": corridor_code,
            "trade_count":   len(all_trades),
            "total_volume_mt":corridor_data["total_volume_mt"],
            "s3_key":        s3_key,
        })

    logger.info(
        f"GEV nightly run complete: "
        f"{len(written)} corridors written, "
        f"{len(held)} held for accumulation"
    )

    return {
        "status":            "GEV_RUN_COMPLETE",
        "run_date":          run_date,
        "corridors_written": len(written),
        "corridors_held":    len(held),
        "gev_manifest":      gev_manifest,
    }


# ── Pending trade accumulation helpers ───────────────────────────
def fetch_pending_corridor_trades(corridor_code: str,
                                   run_date: str) -> list:
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT trade_payload FROM gev_pending_trades
            WHERE corridor_code = :corridor_code
            AND   accumulated_before = false
        """,
        parameters=[{
            "name": "corridor_code",
            "value": {"stringValue": corridor_code}
        }],
    )
    return [
        json.loads(row[0]["stringValue"])
        for row in result["records"]
    ]


def store_pending_trades(corridor_code: str,
                          trades: list,
                          run_date: str) -> None:
    for trade in trades:
        aurora_client.execute_statement(
            resourceArn=AURORA_ARN,
            secretArn=AURORA_SECRET,
            database=AURORA_DB,
            sql="""
                INSERT INTO gev_pending_trades (
                    corridor_code, trade_payload,
                    stored_at, accumulated_before
                ) VALUES (
                    :corridor_code, :payload,
                    :stored_at, false
                )
            """,
            parameters=[
                {"name": "corridor_code",
                 "value": {"stringValue": corridor_code}},
                {"name": "payload",
                 "value": {"stringValue": json.dumps(trade)}},
                {"name": "stored_at",
                 "value": {"stringValue": run_date}},
            ],
        )


def clear_pending_trades(corridor_code: str) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            DELETE FROM gev_pending_trades
            WHERE corridor_code = :corridor_code
        """,
        parameters=[{
            "name": "corridor_code",
            "value": {"stringValue": corridor_code}
        }],
    )
