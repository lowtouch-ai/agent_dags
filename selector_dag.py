from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.models import Variable, Param
import logging
from ollama import Client
from pypdf import PdfReader
from io import BytesIO
import requests

# =============================================================================
# Configuration
# =============================================================================

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
RFP_API_BASE = "http://agentconnector:8000"

default_args = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": 60,
}

def get_ai_response(prompt, conversation_history=None,headers=None):
    try:
        logging.debug(f"Query received: {prompt[:200]}...")
        
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")
        
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

def set_project_status(project_id, status, headers):
    """Helper to update project status via API"""
    try:
        url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
        payload = {"status": status}
        requests.patch(url, json=payload, headers=headers, timeout=10).raise_for_status()
        logging.info(f"Project {project_id} status updated to '{status}'")
    except Exception as e:
        logging.warning(f"Failed to update project status to '{status}': {e}")

def handle_task_failure(context):
    """
    Extracts details from Airflow context to call the existing set_project_status.
    """
    dag_run = context.get("dag_run")
    if not dag_run:
        return

    conf = dag_run.conf or {}
    project_id = conf.get("project_id")
    
    # Reconstruct headers from conf, just like in your other tasks
    headers = {
        "Content-Type": "application/json", 
        "Accept": "application/json",
        "WORKSPACE_UUID": conf.get("workspace_uuid", ""),
        "x-ltai-user-email": conf.get("x-ltai-user-email", "")
    }

    if project_id:
        # REUSES YOUR EXISTING FUNCTION
        set_project_status(project_id, "failed", headers)

def update_project_doc_type(project_id, doc_type, headers):
    """Helper to update project document_type via API"""
    try:
        url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
        payload = {"document_type_code": doc_type}
        requests.patch(url, json=payload, headers=headers, timeout=10).raise_for_status()
        logging.info(f"Project {project_id} document_type updated to '{doc_type}'")
    except Exception as e:
        logging.warning(f"Failed to update document_type to '{doc_type}': {e}")

# =============================================================================
# Task Functions
# =============================================================================

def check_fast_path(**context):
    """
    Check if user provided document_type directly.
    If yes, skip AI classification. If no, proceed with full flow.
    """
    conf = context["dag_run"].conf or {}
    doc_type = conf.get("document_type", "AUTO").strip().upper()
    project_id = conf.get("project_id")

    # Extract headers for API call
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "WORKSPACE_UUID": conf.get("workspace_uuid", ""),
        "x-ltai-user-email": conf.get("x-ltai-user-email", "")
    }

    if doc_type != "AUTO" and doc_type in DOCUMENT_TYPE_TO_DAG:
        logging.info(f"Fast-path activated! User provided document_type = {doc_type}")
        # Update DB with the manually provided type
        if project_id:
            update_project_doc_type(project_id, doc_type, headers)
        # Push the document type so map_to_processing_dag can use it
        context["ti"].xcom_push(key="document_type", value=doc_type)
        # Skip to trigger step directly
        return "trigger_processing_dag"
    else:
        logging.info("No valid document_type in conf → proceeding with AI classification")
        return "fetch_pdf_from_api"


def fetch_pdf_and_extract_text(**context):
    """
    Fetch PDF from API using project_id and extract text.
    Combined task for efficiency.
    """
    conf = context["dag_run"].conf or {}
    project_id = conf.get("project_id")
    workspace_uuid = conf.get("workspace_uuid", "")
    user_email = conf.get("x-ltai-user-email", "")
    
    if not project_id:
        raise ValueError("project_id is required in dag_run.conf")
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if workspace_uuid:
        headers["WORKSPACE_UUID"] = workspace_uuid
    if user_email:
        headers["x-ltai-user-email"] = user_email

    # Set status to 'generating' immediately
    set_project_status(project_id, "generating", headers)

    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/rfpfile"
    logging.info(f"Downloading PDF for project_id={project_id} from {url}")

    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if workspace_uuid:
            headers["WORKSPACE_UUID"] = workspace_uuid
        
        if user_email:
            headers["x-ltai-user-email"] = user_email
        else:
            logging.warning("x-ltai-user-email not provided in DAG params.")
        response = requests.get(url, headers=headers, timeout=90)
        response.raise_for_status()

        pdf_bytes = response.content
        if len(pdf_bytes) == 0:
            raise ValueError("Downloaded PDF is empty")

        logging.info(f"Downloaded PDF: {len(pdf_bytes):,} bytes")

        # Extract text from PDF
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
        else:
            logging.info(f"Successfully extracted {len(text):,} characters")

        # Push extracted text to XCom
        context["ti"].xcom_push(key="extracted_text", value=text)
        return text

    except Exception as e:
        logging.error(f"Failed during download or text extraction: {e}")
        raise


def classify_document_with_ai(**context):
    """
    Use AI to classify the document type based on extracted text.
    """
    extracted_text = context["ti"].xcom_pull(
        task_ids="fetch_pdf_from_api", 
        key="extracted_text"
    )
    
    workspace_uuid = context["dag_run"].conf.get("workspace_uuid", "")
    x_ltai_user_email = context["dag_run"].conf.get("x-ltai-user-email", "")
    headers = {"WORKSPACE_UUID": workspace_uuid , "x-ltai-user-email":x_ltai_user_email} 
    
    if not extracted_text or len(extracted_text.strip()) < 50:
        raise ValueError("Insufficient text extracted for classification")
    
    project_id = context["dag_run"].conf.get("project_id") # Ensure project_id is retrieved

    # Truncate if too long (Ollama has context limits)
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
        detected_type = get_ai_response(prompt, headers=headers).strip().upper()
        logging.info(f"AI classified document as: {detected_type}")

        # Validate and correct AI response
        if detected_type not in DOCUMENT_TYPE_TO_DAG:
            # Try to recover by matching known patterns
            for key in DOCUMENT_TYPE_TO_DAG.keys():
                if key.replace("_", " ").lower() in detected_type.lower():
                    detected_type = key
                    logging.info(f"Corrected ambiguous AI output to: {detected_type}")
                    break
            else:
                raise ValueError(f"Invalid document type returned by AI: {detected_type}")
        
        # Update the project in the database with the detected type
        if project_id:
            update_project_doc_type(project_id, detected_type, headers)

        context["ti"].xcom_push(key="document_type", value=detected_type)
        return detected_type

    except Exception as e:
        logging.error(f"Document classification failed: {e}")
        raise


def get_target_dag_id(**context):
    """
    Map document type to target processing DAG.
    Works for both fast-path and AI classification flows.
    """
    # Try to get from fast-path first, then from classification
    doc_type = context["ti"].xcom_pull(
        task_ids="fast_path_or_full_flow", 
        key="document_type"
    )
    
    if not doc_type:
        doc_type = context["ti"].xcom_pull(
            task_ids="classify_document_with_ai", 
            key="document_type"
        )
    
    if not doc_type:
        raise ValueError("No document_type found in XCom")
    
    target_dag = DOCUMENT_TYPE_TO_DAG.get(doc_type)
    
    if not target_dag:
        raise ValueError(f"Unknown document type: {doc_type}")
    
    logging.info(f"Mapped {doc_type} → {target_dag}")
    
    # Push target DAG ID for trigger task
    context["ti"].xcom_push(key="target_dag_id", value=target_dag)
    return target_dag

def update_project_selector_run_id(**context):
    """
    Update the project record in the database with the selector_dag_run_id
    upon successful completion.
    """
    conf = context["dag_run"].conf or {}
    project_id = conf.get("project_id")
    workspace_uuid = conf.get("workspace_uuid", "")
    user_email = conf.get("x-ltai-user-email", "")
    dag_run_id = context["dag_run"].run_id
    
    if not project_id:
        logging.warning("No project_id found in configuration. Skipping API update.")
        return

    update_url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
    payload = {
        "selector_dag_run_id": dag_run_id
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if workspace_uuid:
        headers["WORKSPACE_UUID"] = workspace_uuid
    else:
        logging.warning("WORKSPACE_UUID not provided in DAG params.")
    if user_email:
        headers["x-ltai-user-email"] = user_email
    else:
        logging.warning("x-ltai-user-email not provided in DAG params.")

    try:
        response = requests.patch(url=update_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info(f"Successfully updated project {project_id} with selector_dag_run_id: {dag_run_id}")

    except Exception as e:
        logging.error(f"Failed to update project {project_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
             logging.error(f"API Response: {e.response.text}")
        raise

# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="rfp_document_selector_dag",
    default_args=default_args,
    description="Fetches RFP PDF via API, classifies it using AI, then triggers correct processing DAG",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "rfp", "ai-classifier", "document-routing"],
    max_active_runs=10,
    render_template_as_native_obj=True,
    on_failure_callback=handle_task_failure,
    params={
        "x-ltai-user-email": Param(
            type="string",
            title="User Email",
            description="Email of the user initiating the DAG run",
        ),
        "project_id": Param(
            type="integer",
            minimum=1,
            title="Project ID",
            description="The numeric ID of the RFP project (e.g., 123)",
        ),
        "workspace_uuid": Param(
            type="string",
            title="Workspace UUID",
            description="The UUID of the workspace",
        ),
        "document_type": Param(
            default="AUTO",
            type="string",
            enum=["AUTO"] + list(DOCUMENT_TYPE_TO_DAG.keys()),
            title="Document Type (optional)",
            description="Leave as AUTO for AI classification, or force a specific type",
        ),
    },
) as dag:

    # Check if fast-path or full flow
    fast_path_branch = BranchPythonOperator(
        task_id="fast_path_or_full_flow",
        python_callable=check_fast_path
    )
    
    # Fetch PDF and extract text
    fetch_pdf = PythonOperator(
        task_id="fetch_pdf_from_api",
        python_callable=fetch_pdf_and_extract_text
    )

    # Classify document using AI
    classify_doc = PythonOperator(
        task_id="classify_document_with_ai",
        python_callable=classify_document_with_ai
    )

    # Trigger the appropriate processing DAG
    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing_dag",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='fast_path_or_full_flow', key='document_type') | "
                        "default(ti.xcom_pull(task_ids='classify_document_with_ai', key='document_type'), true) | "
                        "map_doc_type_to_dag }}",
        wait_for_completion=True,
        reset_dag_run=True,
        conf="{{ dag_run.conf }}",
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Add custom Jinja filter for mapping
    def map_doc_type_to_dag(doc_type):
        return DOCUMENT_TYPE_TO_DAG.get(doc_type, "")
    
    dag.user_defined_filters = {"map_doc_type_to_dag": map_doc_type_to_dag}

    # Log completion
    finalize = PythonOperator(
        task_id="log_completion",
        python_callable=update_project_selector_run_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define task flow
    fast_path_branch >> [fetch_pdf, trigger_processing]
    fetch_pdf >> classify_doc >> trigger_processing
    trigger_processing >> finalize