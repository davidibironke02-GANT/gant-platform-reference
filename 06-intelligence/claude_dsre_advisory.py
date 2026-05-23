# 6.5 Claude Advisory Layer
# Lambda function generating plain-language DSRE buyer advisories
# Receives Granite DSRE output -- score, band, domain breakdown,
# geospatial temporal correlation results
# Returns structured advisory with situation summary, primary driver,
# commodity status, and recommended buyer actions
# Claude does not recompute. Claude does not override. Claude explains.

import json
import logging
import boto3
import anthropic
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client    = boto3.client("rds-data")
events_client    = boto3.client("events")
ssm_client       = boto3.client("ssm")

AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"
EVENT_BUS     = "gant-platform"

# ── Claude client ────────────────────────────────────────────────
claude_client = anthropic.Anthropic(
    api_key=ssm_client.get_parameter(
        Name="/gant/anthropic/api_key",
        WithDecryption=True)["Parameter"]["Value"]
)

CLAUDE_MODEL   = "claude-sonnet-4-20250514"
MAX_TOKENS     = 1000

# ── System prompt ────────────────────────────────────────────────
SYSTEM_PROMPT = """You are the GANT Exchange DSRE advisory engine.
You receive structured DSRE computation output from WatsonX.ai Granite
and produce plain-language buyer intelligence advisories.

Your role is strictly explanatory. You explain what Granite computed.
You do not recompute scores. You do not add information Granite did not
produce. You do not speculate beyond the provided data.

Every advisory must follow this exact structure:
1. SITUATION: One sentence stating current risk level and primary driver.
2. PRIMARY DRIVER: Two to three sentences explaining the dominant domain
   score and what caused it, with specific data from the correlation results.
3. COMMODITY STATUS: One sentence on Domain 1 condition -- temperature,
   humidity, and shock readings relative to acceptable thresholds.
4. REMAINING RISK: One sentence on macro conditions on the remaining route
   from current GPS position forward.
5. RECOMMENDED ACTIONS: Two to four specific, actionable steps the buyer
   should consider given the current score and band.

Use precise numbers from the computation. Name the specific node, corridor
segment, or condition driving the score. Do not use vague language like
'some risk' or 'possible issues'. If the score is Green, say so clearly
and confirm the shipment is tracking normally.

Write in second person. The buyer is reading this at any hour and needs
to understand it immediately and act on it without calling anyone."""


# ── Build Granite output summary for Claude ───────────────────────
def build_granite_summary(dsre_data: dict) -> str:
    """
    Structures the Granite DSRE output into a precise prompt
    that gives Claude exactly what it needs and nothing more.
    """
    d1 = dsre_data["domain_breakdown"]["d1_commodity"]
    d2 = dsre_data["domain_breakdown"]["d2_route"]
    d3 = dsre_data["domain_breakdown"]["d3_geopolitical"]
    d4 = dsre_data["domain_breakdown"]["d4_logistics"]

    geo_note = ""
    if d3.get("hard_override"):
        geo_note = "\nGEOPOLITICAL HARD OVERRIDE ACTIVE: " \
                   "Confirmed active attack on corridor. " \
                   "Score overridden to RED regardless of other domains."

    gap_note = ""
    if d1.get("is_gap"):
        gap_note = (
            f"\nDOMAIN 1 GAP CYCLE: IoT readings not yet received. "
            f"Last confirmed reading carried forward with "
            f"{round((1 - d1.get('confidence', 1.0)) * 100)}% "
            f"confidence degradation applied."
        )

    return f"""DSRE COMPUTATION OUTPUT -- DO NOT RECOMPUTE

Trade ID:          {dsre_data['trade_id']}
Computed at:       {dsre_data['computed_at']}
Aggregate Score:   {dsre_data['aggregate_score']:.2f}
Band:              {dsre_data['band']} -- {dsre_data['band_description']}
{geo_note}{gap_note}

DOMAIN 1 -- COMMODITY CONDITION (weight 0.35)
Score:             {d1['domain_score']:.2f}
Confidence:        {d1.get('confidence', 1.0):.2f}
Temperature score: {d1.get('signals', {}).get('temperature', 'N/A')}
Humidity score:    {d1.get('signals', {}).get('humidity', 'N/A')}
Shock score:       {d1.get('signals', {}).get('shock', 'N/A')}
GPS dwell score:   {d1.get('signals', {}).get('gps_dwell', 'N/A')}
Gap cycle:         {d1.get('is_gap', False)}

DOMAIN 2 -- ROUTE SECURITY (weight 0.25)
Score:             {d2['domain_score']:.2f}
AIS vessel signal: {d2.get('signals', {}).get('ais_vessel', 'N/A')}
Port EDI signal:   {d2.get('signals', {}).get('port_edi', 'N/A')}

DOMAIN 3 -- GEOPOLITICAL (weight 0.20)
Score:             {d3['domain_score']:.2f}
ACLED signal:      {d3.get('signals', {}).get('acled', 'N/A')}
IMO signal:        {d3.get('signals', {}).get('imo', 'N/A')}
UN OCHA signal:    {d3.get('signals', {}).get('un_ocha', 'N/A')}
Lloyd's active:    {d3.get('signals', {}).get('lloyds_active', False)}
Hard override:     {d3.get('hard_override', False)}

DOMAIN 4 -- LOGISTICS INFRASTRUCTURE (weight 0.20)
Score:             {d4['domain_score']:.2f}
Port EDI signal:   {d4.get('signals', {}).get('port_edi', 'N/A')}
AIS vessel signal: {d4.get('signals', {}).get('ais_vessel', 'N/A')}

GEOSPATIAL TEMPORAL CORRELATION RESULTS
{json.dumps(dsre_data.get('correlation_results', {}), indent=2)}

Produce the buyer advisory now following the five-section structure.
Do not include section numbers or labels in the output.
Write as flowing paragraphs the buyer reads directly."""


# ── Call Claude API ───────────────────────────────────────────────
def generate_advisory(granite_summary: str) -> str:
    message = claude_client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": granite_summary,
        }],
    )
    return message.content[0].text


# ── Write advisory to Aurora ──────────────────────────────────────
def write_advisory(trade_id: str, advisory: str,
                   aggregate_score: float, band: str,
                   generated_at: str) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO dsre_advisories (
                trade_id, advisory_text, aggregate_score,
                band, generated_at
            ) VALUES (
                :trade_id, :advisory, :score,
                :band, :generated_at
            )
        """,
        parameters=[
            {"name": "trade_id",     "value": {"stringValue": trade_id}},
            {"name": "advisory",     "value": {"stringValue": advisory}},
            {"name": "score",        "value": {"doubleValue": aggregate_score}},
            {"name": "band",         "value": {"stringValue": band}},
            {"name": "generated_at", "value": {"stringValue": generated_at}},
        ],
    )


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by DSRE_CYCLE_COMPLETE event from EventBridge.
    Builds a structured prompt from Granite's DSRE output,
    calls Claude API, stores the advisory in Aurora, and
    publishes ADVISORY_GENERATED for WebSocket push to buyer.

    Claude receives only what Granite computed. The system prompt
    enforces the five-section structure and prohibits speculation
    or recomputation. WatsonX.governance monitors Claude output
    for coherence and drift from the underlying computation.

    Expected event format: full DSRE cycle_data dict as produced
    by the DSRE aggregation pipeline handler.
    """
    trade_id       = event["trade_id"]
    aggregate_score= float(event["aggregate_score"])
    band           = event["band"]
    generated_at   = datetime.now(timezone.utc).isoformat()

    # ── Build Granite summary ─────────────────────────────────────
    granite_summary = build_granite_summary(event)

    # ── Generate advisory ─────────────────────────────────────────
    try:
        advisory = generate_advisory(granite_summary)
    except Exception as e:
        logger.error(f"Claude API unavailable for {trade_id}: {e}")
        advisory = (
            f"Advisory generation temporarily unavailable. "
            f"Current DSRE score: {aggregate_score:.0f} -- {band}. "
            f"Please review domain breakdown in the dashboard."
        )

    # ── Write to Aurora ───────────────────────────────────────────
    write_advisory(trade_id, advisory, aggregate_score,
                   band, generated_at)

    # ── Publish ADVISORY_GENERATED ────────────────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.advisory",
        "DetailType":   "ADVISORY_GENERATED",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":        trade_id,
            "aggregate_score": aggregate_score,
            "band":            band,
            "advisory_text":   advisory,
            "generated_at":    generated_at,
        }),
    }])

    logger.info(
        f"Advisory generated for {trade_id}: "
        f"score={aggregate_score:.2f} band={band}"
    )

    return {
        "status":          "ADVISORY_GENERATED",
        "trade_id":        trade_id,
        "aggregate_score": aggregate_score,
        "band":            band,
        "advisory_text":   advisory,
        "generated_at":    generated_at,
    }
