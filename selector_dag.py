from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable , Param
import logging
import json
from ollama import Client
from pypdf import PdfReader
from io import BytesIO
import requests

# =============================================================================
# Configuration
# =============================================================================

# Mapping: document_type_code → target processing DAG ID
# Multiple document types can map to the same processing DAG
DOCUMENT_TYPE_TO_DAG = {
    "RFP_PUBLIC_PENSION":   "rfp_public_pension_processing_dag",
    "RFP_CORP_PENSION":     "rfp_corporate_pension_processing_dag",
    "RFP_ENDOWMENT":        "rfp_endowment_foundation_processing_dag",
    "RFP_INSURANCE":        "rfp_insurance_company_processing_dag",
    "RFP_OCIO":             "rfp_ocio_processing_dag",
    "RFP_SUBADVISORY":      "rfp_subadvisory_processing_dag",
    "RFP_WRAP_SMA":         "rfp_wrap_sma_processing_dag",
    "RFP_BD_PLATFORM":      "rfp_broker_dealer_platform_processing_dag",
    "RFP_GATEKEEPER":       "rfp_gatekeeper_review_processing_dag",
    "RFP_CONSULTANT":       "rfp_consultant_strategy_processing_dag",
}

AUTO_DETECTION_MODEL = "rfp/autogeneration:0.3"
OLLAMA_HOST = Variable.get("ltai.v1.rfp.OLLAMA_HOST", default_var="http://agentomatic:8000")
RFP_API_BASE = "http://agentconnector:8000"  # Change if needed

default_args = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": 60,
}

def get_ai_response(prompt, conversation_history=None,headers=None):
    try:
        logging.debug(f"Query received: {prompt}")
        
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")
        headers=headers
        client = Client(host=OLLAMA_HOST, headers=headers)
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model '{AUTO_DETECTION_MODEL}'")

        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model=AUTO_DETECTION_MODEL,
            messages=messages,
            stream=False
        )
        content = response['message']['content'].strip()
        if not content:
            raise ValueError("Empty response from AI")
        return content
    except Exception as e:
        logging.error(f"AI classification failed: {e}")
        raise

# =============================================================================
# Task 1: Fetch PDF from API using project_id
# =============================================================================
def fetch_pdf_from_api(**context):
    conf = context["dag_run"].conf or {}
    project_id = conf.get("project_id")
    
    if not project_id:
        raise ValueError("project_id is required in dag_run.conf")

    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/rfpfile"
    
    logging.info(f"Downloading and extracting text for project_id={project_id}")

    try:
        response = requests.get(url, timeout=90)
        response.raise_for_status()

        pdf_bytes = response.content
        if len(pdf_bytes) == 0:
            raise ValueError("Downloaded PDF is empty")

        logging.info(f"Downloaded PDF: {len(pdf_bytes):,} bytes")

        # Extract text directly here
        reader = PdfReader(BytesIO(pdf_bytes))
        text = ""
        page_count = len(reader.pages)
        
        for i, page in enumerate(reader.pages, 1):
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
            if i % 10 == 0 or i == page_count:
                logging.info(f"Extracted text from {i}/{page_count} pages...")

        if not text.strip():
            text = "[NO_TEXT_EXTRACTED - Likely scanned/image-based PDF]"
            logging.warning("No text extracted from PDF")

        logging.info(f"Successfully extracted {len(text):,} characters")

        # ONLY push plain text → safe for JSON/XCom
        context["ti"].xcom_push(key="extracted_text", value=text)
        return text

    except Exception as e:
        logging.error(f"Failed during download or text extraction: {e}")
        raise

# =============================================================================
# Task 2: Extract text from downloaded PDF
# =============================================================================
def extract_text_from_pdf(**context):
    # This gets the return value of the previous task
    pdf_bytes = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api")

    if not isinstance(pdf_bytes, (bytes, bytearray)):
        raise TypeError(f"Expected bytes, got {type(pdf_bytes)}")

    reader = PdfReader(BytesIO(pdf_bytes))  # Now safe — pdf_bytes is real bytes
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"

    if not text.strip():
        text = "[NO_TEXT_EXTRACTED - possibly scanned PDF]"
        logging.warning("No text extracted — document may be image-based")

    logging.info(f"Extracted {len(text):,} characters from PDF")
    context["ti"].xcom_push(key="extracted_text", value=text)
    return text

# =============================================================================
# Task 3: Classify document type using AI agent
# =============================================================================
def classify_document_with_ai(**context):
    extracted_text = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api", key="extracted_text")
    workspace_uuid = context["dag_run"].conf.get("workspace_uuid", "")
    headers = {"WORKSPACE_UUID": workspace_uuid} if workspace_uuid else None
    if not extracted_text or len(extracted_text.strip()) < 50:
        raise ValueError("Insufficient text extracted for classification")

    # Truncate if too long (Ollama has context limits)
    preview = extracted_text[:1000] + "..." if len(extracted_text) > 1000 else extracted_text

    prompt = f"""
You are an expert classifier for investment management RFP and due diligence documents.

Here are the valid document types:

- RFP_PUBLIC_PENSION → Public Pension Plan RFP
- RFP_CORP_PENSION → Corporate Pension Plan RFP
- RFP_ENDOWMENT → Endowment or Foundation RFP
- RFP_INSURANCE → Insurance Company General Account or Asset Management RFP
- RFP_OCIO → Outsourced CIO (OCIO) RFP
- RFP_SUBADVISORY → Sub-Advisory Mandate RFP
- RFP_WRAP_SMA → Wrap Fee / SMA Program Questionnaire
- RFP_BD_PLATFORM → Broker-Dealer Platform Questionnaire
- RFP_GATEKEEPER → Investment Consultant or Gatekeeper Database Update
- RFP_CONSULTANT → Consultant Strategy-Level RFP

Analyze the document below and return ONLY the exact document_type_code (e.g. RFP_PUBLIC_PENSION) with no quotes, explanation, or extra text.

Document content :
{extracted_text}

Answer with only the code:
"""

    try:
        detected_type = get_ai_response(prompt,headers=headers).strip().upper()
        logging.info(f"AI classified document as: {detected_type}")

        if detected_type not in DOCUMENT_TYPE_TO_DAG:
            # Try to recover by matching known patterns
            for key in DOCUMENT_TYPE_TO_DAG.keys():
                if key.replace("_", " ").lower() in detected_type.lower():
                    detected_type = key
                    logging.info(f"Corrected ambiguous AI output to: {detected_type}")
                    break
            else:
                raise ValueError(f"Invalid document type returned by AI: {detected_type}")

        context["ti"].xcom_push(key="document_type", value=detected_type)
        return detected_type

    except Exception as e:
        logging.error(f"Document classification failed: {e}")
        raise

# =============================================================================
# Task 4: Map to target DAG
# =============================================================================
def get_target_dag_id(**context):
    doc_type = context["ti"].xcom_pull(task_ids="classify_document_with_ai", key="document_type")
    target_dag = DOCUMENT_TYPE_TO_DAG[doc_type]
    logging.info(f"Mapped {doc_type} → {target_dag}")
    return target_dag

# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="document_selector_dag",
    default_args=default_args,
    description="Fetches RFP PDF via API, classifies it using AI, then triggers correct processing DAG",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "rfp", "ai-classifier", "document-routing"],
    max_active_runs=10,
    render_template_as_native_obj=True,  # Required for Param to work properly
    params={
        "project_id": Param(
            type="integer",
            minimum=1,
            title="Project ID",
            description="The numeric ID of the RFP project (e.g., 123)",
            examples=[123, 456],
        ),
        "workspace_uuid": Param(
            type="string",
            title="Workspace UUID",
            description="The UUID of the workspace (e.g., '550e8400-e29b-41d4-a716-446655440000')",
            examples=["550e8400-e29b-41d4-a716-446655440000"],
        ),
        # Optional: let user override auto-detection if they know the type
        "document_type": Param(
            default="AUTO",
            type="string",
            enum=["AUTO"] + list(DOCUMENT_TYPE_TO_DAG.keys()),
            title="Document Type (optional)",
            description="Leave as AUTO for AI classification, or force a specific type",
        ),
    },
) as dag:
    def check_fast_path(**context):
        conf = context["dag_run"].conf or {}
        doc_type = conf.get("document_type", "AUTO").strip().upper()

        if doc_type != "AUTO" and doc_type in DOCUMENT_TYPE_TO_DAG:
            logging.info(f"Fast-path activated! User forced document_type = {doc_type}")
            context["ti"].xcom_push(key="document_type", value=doc_type)
            return "map_to_processing_dag"  # Skip everything → go straight to mapping
        else:
            logging.info("No valid document_type in conf → proceeding with AI classification")
            return "fetch_pdf_from_api"
    fast_path_branch = BranchPythonOperator(
        task_id="fast_path_or_full_flow",
        python_callable=check_fast_path,
        provide_context=True,
    )
    
    fetch_pdf = PythonOperator(
        task_id="fetch_pdf_from_api",
        python_callable=fetch_pdf_from_api,
        provide_context=True,
    )

    extract_text = PythonOperator(
        task_id="extract_text_from_pdf",
        python_callable=extract_text_from_pdf,
        provide_context=True,
    )

    classify_doc = PythonOperator(
        task_id="classify_document_with_ai",
        python_callable=classify_document_with_ai,
        provide_context=True,
    )

    map_dag = PythonOperator(
        task_id="map_to_processing_dag",
        python_callable=get_target_dag_id,
        provide_context=True,
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing_dag",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='map_to_processing_dag') }}",
        wait_for_completion=True,
        reset_dag_run=True,
        execution_date="{{ execution_date }}",
        conf="{{ dag_run.conf }}",  # Passes project_id, etc.
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    finalize = PythonOperator(
        task_id="log_completion",
        python_callable=lambda **ctx: logging.info(
            f"Selector DAG completed successfully for project_id={ctx['dag_run'].conf.get('project_id')}"
        ),
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Flow
    fast_path_branch >> [fetch_pdf, map_dag]  # Branch: either full flow or direct to map

    fetch_pdf >> classify_doc >> map_dag
    map_dag >> trigger_processing >> finalize