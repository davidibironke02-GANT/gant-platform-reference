# 4.7 Compliance Document Generation
# Lambda function assembling the full export documentation package
# Triggered at LOADED_EXW state transition
# Documents: phytosanitary certificate reference, certificate of origin,
# commercial invoice, packing list, bill of lading template
# Identity vault accessed once -- only point where real names enter docs

import json
import logging
import hashlib
import boto3
from datetime import datetime, timezone
from jinja2 import Environment, BaseLoader

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

aurora_client = boto3.client("rds-data")
events_client = boto3.client("events")
s3_client     = boto3.client("s3")
ssm_client    = boto3.client("ssm")

AURORA_ARN    = ssm_client.get_parameter(
    Name="/gant/aurora/cluster_arn")["Parameter"]["Value"]
AURORA_SECRET = ssm_client.get_parameter(
    Name="/gant/aurora/secret_arn")["Parameter"]["Value"]
AURORA_DB     = "gant_platform"
EVENT_BUS     = "gant-platform"
DOCS_BUCKET   = ssm_client.get_parameter(
    Name="/gant/s3/documents_bucket")["Parameter"]["Value"]

# ── Document types in the required compliance package ────────────
DOCUMENT_TYPES = [
    "phytosanitary_certificate_reference",
    "certificate_of_origin",
    "commercial_invoice",
    "packing_list",
    "bill_of_lading_template",
]


# ── Fetch trade record from Aurora ───────────────────────────────
def fetch_trade_record(trade_id: str) -> dict:
    result = aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            SELECT
                t.trade_id, t.batch_id, t.fuid, t.buid,
                t.corridor_code, t.trade_date, t.total_volume_mt,
                t.agreed_price_cad, t.container_id,
                b.crop_code, b.vvi_score, b.quality_score,
                b.submetric_scores, b.batchlot_ids
            FROM trades t
            JOIN batches b ON b.batch_id = t.batch_id
            WHERE t.trade_id = :trade_id
        """,
        parameters=[{
            "name": "trade_id",
            "value": {"stringValue": trade_id}
        }],
    )
    if not result["records"]:
        raise ValueError(f"TradeID '{trade_id}' not found in Aurora")

    row = result["records"][0]
    return {
        "trade_id":        row[0]["stringValue"],
        "batch_id":        row[1]["stringValue"],
        "fuid":            row[2]["stringValue"],
        "buid":            row[3]["stringValue"],
        "corridor_code":   row[4]["stringValue"],
        "trade_date":      row[5]["stringValue"],
        "total_volume_mt": row[6]["doubleValue"],
        "agreed_price":    row[7]["doubleValue"],
        "container_id":    row[8]["stringValue"],
        "crop_code":       row[9]["stringValue"],
        "vvi_score":       row[10]["doubleValue"],
        "quality_score":   row[11]["doubleValue"],
        "submetric_scores":json.loads(row[12]["stringValue"]),
        "batchlot_ids":    json.loads(row[13]["stringValue"]),
    }


# ── Resolve FUID and BUID to legal names via identity vault ──────
def resolve_identities(fuid: str, buid: str) -> dict:
    """
    Single scoped read-only API call to the identity vault service.
    This is the only point in the compliance document pipeline
    where real legal names are injected into generated documents.
    The identity vault is isolated from the primary database.
    No identity data is logged or stored outside the vault.
    """
    identity_api = ssm_client.get_parameter(
        Name="/gant/internal/identity_vault_url")["Parameter"]["Value"]
    vault_token  = ssm_client.get_parameter(
        Name="/gant/internal/identity_vault_token",
        WithDecryption=True)["Parameter"]["Value"]

    import requests
    response = requests.post(
        f"{identity_api}/v1/resolve",
        headers={"Authorization": f"Bearer {vault_token}"},
        json={"fuid": fuid, "buid": buid},
        timeout=5,
    )
    response.raise_for_status()
    data = response.json()

    return {
        "farmer_legal_name":    data["farmer_legal_name"],
        "farmer_address":       data["farmer_address"],
        "buyer_legal_name":     data["buyer_legal_name"],
        "buyer_address":        data["buyer_address"],
        "buyer_jurisdiction":   data["buyer_jurisdiction"],
    }


# ── Fetch versioned Jinja2 template from S3 ──────────────────────
def fetch_template(doc_type: str) -> str:
    obj = s3_client.get_object(
        Bucket=DOCS_BUCKET,
        Key=f"templates/{doc_type}/latest.html",
    )
    return obj["Body"].read().decode("utf-8")


# ── Render document from template ────────────────────────────────
def render_document(template_str: str,
                    context: dict) -> str:
    env      = Environment(loader=BaseLoader())
    template = env.from_string(template_str)
    return template.render(**context)


# ── Generate PDF from rendered HTML ──────────────────────────────
def render_pdf(html: str) -> bytes:
    from weasyprint import HTML
    return HTML(string=html).write_pdf()


# ── Store document to S3 with Object Lock ────────────────────────
def store_document(trade_id: str, doc_type: str,
                   pdf_bytes: bytes) -> str:
    key = f"trades/{trade_id}/documents/{doc_type}.pdf"
    s3_client.put_object(
        Bucket=DOCS_BUCKET,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=datetime(2036, 1, 1,
                                           tzinfo=timezone.utc),
    )
    return key


# ── Compute SHA-256 hash of document PDF ─────────────────────────
def hash_document(pdf_bytes: bytes) -> str:
    return hashlib.sha256(pdf_bytes).hexdigest()


# ── Write document record to Aurora ──────────────────────────────
def write_document_record(trade_id: str, doc_type: str,
                           s3_key: str, doc_hash: str,
                           generated_at: str) -> None:
    aurora_client.execute_statement(
        resourceArn=AURORA_ARN,
        secretArn=AURORA_SECRET,
        database=AURORA_DB,
        sql="""
            INSERT INTO compliance_documents (
                trade_id, document_type, s3_key,
                document_hash_sha256, generated_at
            ) VALUES (
                :trade_id, :doc_type, :s3_key,
                :doc_hash, :generated_at
            )
        """,
        parameters=[
            {"name": "trade_id",     "value": {"stringValue": trade_id}},
            {"name": "doc_type",     "value": {"stringValue": doc_type}},
            {"name": "s3_key",       "value": {"stringValue": s3_key}},
            {"name": "doc_hash",     "value": {"stringValue": doc_hash}},
            {"name": "generated_at", "value": {"stringValue": generated_at}},
        ],
    )


# ── Lambda handler ────────────────────────────────────────────────
def handler(event: dict, context) -> dict:
    """
    Triggered by LOADED_EXW event from EventBridge.
    Assembles the full compliance document package for the trade.
    Resolves farmer and buyer identities from the isolated vault
    once -- identities flow into documents but are never logged.
    Each document is rendered from a versioned Jinja2 template,
    converted to PDF, hashed with SHA-256, stored in S3 with
    Object Lock, and recorded in Aurora against the TradeID.
    On completion publishes COMPLIANCE_PACKAGE_COMPLETE.

    Expected event format:
    {
        "trade_id": "TRD-20260426-0012"
    }
    """
    trade_id = event["trade_id"]

    # ── Fetch trade record ────────────────────────────────────────
    trade = fetch_trade_record(trade_id)

    # ── Resolve identities -- single vault call ───────────────────
    identities = resolve_identities(trade["fuid"], trade["buid"])

    # ── Build template rendering context ─────────────────────────
    # Identities injected here. Never logged beyond this point.
    generated_at = datetime.now(timezone.utc).isoformat()
    render_context = {
        **trade,
        **identities,
        "generated_at":  generated_at,
        "platform_name": "GANT Exchange",
    }

    document_manifest = []

    # ── Generate each document in the package ────────────────────
    for doc_type in DOCUMENT_TYPES:
        try:
            template_str = fetch_template(doc_type)
            html         = render_document(template_str,
                                           render_context)
            pdf_bytes    = render_pdf(html)
            doc_hash     = hash_document(pdf_bytes)
            s3_key       = store_document(trade_id, doc_type,
                                          pdf_bytes)
            write_document_record(trade_id, doc_type,
                                  s3_key, doc_hash, generated_at)

            document_manifest.append({
                "document_type":      doc_type,
                "s3_key":             s3_key,
                "document_hash_sha256": doc_hash,
                "generated_at":       generated_at,
            })

            logger.info(
                f"Document generated for {trade_id}: "
                f"{doc_type} -- SHA-256: {doc_hash[:16]}..."
            )

        except Exception as e:
            logger.error(
                f"Failed to generate {doc_type} "
                f"for trade {trade_id}: {e}"
            )
            raise

    # ── Publish COMPLIANCE_PACKAGE_COMPLETE ───────────────────────
    events_client.put_events(Entries=[{
        "Source":       "gant.compliance",
        "DetailType":   "COMPLIANCE_PACKAGE_COMPLETE",
        "EventBusName": EVENT_BUS,
        "Detail": json.dumps({
            "trade_id":          trade_id,
            "document_count":    len(document_manifest),
            "document_manifest": document_manifest,
            "generated_at":      generated_at,
        }),
    }])

    logger.info(
        f"Compliance package complete for {trade_id}: "
        f"{len(document_manifest)} documents generated"
    )

    return {
        "status":            "COMPLIANCE_PACKAGE_COMPLETE",
        "trade_id":          trade_id,
        "document_count":    len(document_manifest),
        "document_manifest": document_manifest,
        "generated_at":      generated_at,
    }
