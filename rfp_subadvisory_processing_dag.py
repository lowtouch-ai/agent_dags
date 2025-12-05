from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable, Param
import logging
import json
import requests
from io import BytesIO
from pypdf import PdfReader
from ollama import Client
import re

# =============================================================================
# Configuration
# =============================================================================
OLLAMA_HOST = Variable.get("ltai.v1.rfp.OLLAMA_HOST", default_var="http://agentomatic:8000")
RFP_API_BASE = Variable.get("ltai.v1.rfp.RFP_API_BASE", default_var="http://agentconnector:8000")
MODEL_FOR_EXTRACTION = "rfp/autogeneration:0.3af"

default_args = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": 60,
}

# =============================================================================
# Helper Functions
# =============================================================================
def get_ai_response(prompt, headers=None):
    """Call Ollama API and return response text"""
    try:
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")
        
        client = Client(host=OLLAMA_HOST, headers=headers)
        response = client.chat(
            model=MODEL_FOR_EXTRACTION,
            messages=[{"role": "user", "content": prompt}],
            stream=False
        )
        content = response['message']['content'].strip()
        if not content:
            raise ValueError("Empty response from AI")
        return content
    except Exception as e:
        logging.error(f"AI call failed: {e}")
        raise

def extract_json_from_response(response_text):
    """Extract JSON from AI response, handling code blocks"""
    match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response_text)
    if match:
        response_text = match.group(1).strip()
    return json.loads(response_text)

# =============================================================================
# Task 1: Fetch PDF and Extract Text
# =============================================================================
def fetch_pdf_from_api(**context):
    """Download PDF from API and extract text content"""
    conf = context["dag_run"].conf or {}
    project_id = conf.get("project_id")
    workspace_uuid = conf.get("workspace_uuid", "")
    user_email = conf.get("x-ltai-user-email", "")
    
    if not project_id:
        raise ValueError("project_id is required in dag_run.conf")

    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/rfpfile"
    logging.info(f"Downloading PDF for project_id={project_id}")

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

        logging.info(f"Successfully extracted {len(text):,} characters")
        context["ti"].xcom_push(key="extracted_text", value=text)
        return text

    except Exception as e:
        logging.error(f"Failed during download or text extraction: {e}")
        raise

# =============================================================================
# Task 2: Extract Questions with AI
# =============================================================================
def extract_questions_with_ai(**context):
    """Use AI to extract all questions from RFP text and create them in database"""
    extracted_text = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api", key="extracted_text")
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    
    if len(extracted_text.strip()) < 50:
        raise ValueError("Insufficient text for question extraction")

    prompt = f"""
You are an expert at extracting questions from Sub-Advisory RFPs.

Analyze the RFP document and extract ALL questions. Questions are typically numbered (e.g., 1., 2.1, A.3).

Return ONLY a valid JSON dictionary in this exact format:
{{"1": "Full question text here", "2": "Another question", "3.1": "Subquestion text"}}

Include question numbers as keys (strings). Do not add explanations, metadata, or extra text.

Document preview: {extracted_text}
"""

    raw_json = get_ai_response(prompt, headers=headers)
    
    try:
        questions_dict = extract_json_from_response(raw_json)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse AI response as JSON: {raw_json[:500]}")
        raise
    
    logging.info(f"Extracted {len(questions_dict)} questions via AI")
    
    # Create questions in database and store IDs
    questions_with_id = {}
    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"

    for idx, (q_num, q_text) in enumerate(questions_dict.items(), start=1):
        if not isinstance(q_text, str) or not q_text.strip():
            continue

        payload = {
            "questiontext": q_text.strip(),
            "questionorder": idx
        }

        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            if workspace_uuid:
                headers["WORKSPACE_UUID"] = workspace_uuid
            
            if x_ltai_user_email:
                headers["x-ltai-user-email"] = x_ltai_user_email
            else:
                logging.warning("x-ltai-user-email not provided in DAG params.")
            resp = requests.post(url, json=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            question_id = data.get("questionid")
            
            if not question_id:
                logging.warning(f"No ID returned for question {q_num}")
                continue

            questions_with_id[q_num] = {
                "text": q_text.strip(),
                "id": question_id
            }
            logging.info(f"Created question {q_num} → ID {question_id}")

        except Exception as e:
            logging.error(f"Failed to create question {q_num}: {e}")

    if not questions_with_id:
        raise ValueError("No questions were successfully created in backend")

    context["ti"].xcom_push(key="questions_dict", value=questions_dict)
    context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
    logging.info(f"Created {len(questions_with_id)} questions in backend")
    return questions_with_id

# =============================================================================
# Task 3: Validate & Fix Missing Questions
# =============================================================================
def validate_and_fix_questions(**context):
    """Validate extracted questions and add any missing ones"""
    questions_dict = context["ti"].xcom_pull(task_ids="extract_questions_with_ai", key="questions_dict")
    questions_with_id = context["ti"].xcom_pull(task_ids="extract_questions_with_ai", key="questions_with_id")
    current_keys = sorted(questions_dict.keys(), key=lambda x: (x.split('.')[0], int(x.split('.')[-1]) if '.' in x else 0))
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    
    prompt_validate = f"""
Review these extracted questions against the RFP document. List ONLY the question numbers that appear MISSING (e.g., "1.2, 4, 5.1-5.3").

Extracted keys: {json.dumps(current_keys)}

Document preview: {questions_dict}

IMPORTANT: Respond with ONLY "COMPLETE" if all questions are extracted, or ONLY a comma-separated list of missing numbers. Do not include any explanations, analysis, lists, or markdown. No additional text.

Response format: Comma-separated missing numbers OR "COMPLETE"
"""

    missing_str = get_ai_response(prompt_validate, headers=headers).strip()
    logging.info(f"Raw validation response: '{missing_str}'")
    
    if "COMPLETE" in missing_str.upper():
        logging.info("All questions validated - no missing questions found")
        context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
        return questions_with_id

    # Parse missing question numbers
    missing_raw = [m.strip() for m in missing_str.split(',') if m.strip()]
    question_pattern = re.compile(r'^\d+(\.\d+)?(-?\d+(\.\d+)?)?$')
    missing_list = []
    
    for item in missing_raw:
        cleaned = re.sub(r'^MISSING:\s*', '', item.strip(), flags=re.IGNORECASE)
        if question_pattern.match(cleaned):
            missing_list.append(cleaned)
        else:
            logging.warning(f"Skipping invalid missing item: '{item}' -> cleaned '{cleaned}'")
    
    logging.info(f"Missing questions identified: {missing_list}")

    if not missing_list:
        context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
        return questions_with_id

    # Extract missing questions
    prompt_missing = f"""
Extract ONLY these missing questions from the document. Do not touch existing ones.

Missing numbers: {', '.join(missing_list)}

Return ONLY a valid JSON object (dict) with these keys and their question text. No other text, explanations, or markdown.

Example: {{"3.3": "What is the deadline?", "5.2": "Describe the process."}}

Document preview: {questions_dict}
"""

    missing_dict_str = get_ai_response(prompt_missing, headers=headers).strip()
    logging.info(f"Raw missing extraction response: '{missing_dict_str}'")
    
    try:
        missing_dict = extract_json_from_response(missing_dict_str)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON from missing extraction: {e}. Raw response: '{missing_dict_str}'")
        raise
    
    # Create missing questions in database
    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"

    for idx, (q_num, q_text) in enumerate(missing_dict.items(), start=len(questions_dict) + 1):
        if not isinstance(q_text, str) or not q_text.strip():
            continue

        payload = {
            "questiontext": q_text.strip(),
            "questionorder": idx
        }

        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            if workspace_uuid:
                headers["WORKSPACE_UUID"] = workspace_uuid
            
            if x_ltai_user_email:
                headers["x-ltai-user-email"] = x_ltai_user_email
            else:
                logging.warning("x-ltai-user-email not provided in DAG params.")
            resp = requests.post(url, json=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            question_id = data.get("questionid")
            
            if not question_id:
                logging.warning(f"No ID returned for missing question {q_num}")
                continue

            questions_with_id[q_num] = {
                "text": q_text.strip(),
                "id": question_id
            }
            logging.info(f"Added missing question {q_num} → ID {question_id}")

        except Exception as e:
            logging.error(f"Failed to create missing question {q_num}: {e}")
    
    context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
    logging.info(f"Total questions after validation: {len(questions_with_id)}")
    return questions_with_id

# =============================================================================
# Task 4: Generate Answers with AI
# =============================================================================
def generate_answers_with_ai(**context):
    """Generate answers for all questions using AI"""
    questions_with_id = context["ti"].xcom_pull(task_ids="validate_and_fix_questions", key="questions_with_id")
    conf = context["dag_run"].conf
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    
    answers_dict = {}
    
    for q_num, question_data in questions_with_id.items():
        question_text = question_data["text"]
        question_id = question_data["id"]
        
        prompt_answer = f"""
You are an expert answering Sub-Advisory RFP questions for our firm.

Question {q_num}: {question_text}

Provide a complete, professional answer. Use bullet points if needed. Be concise yet thorough.

IMPORTANT: Respond with ONLY a valid JSON object in this exact format:
{{"answer": "your detailed answer here", "is_sensitive": true}}
"""
        
        try:
            response_str = get_ai_response(prompt_answer, headers=headers).strip()
            response_data = extract_json_from_response(response_str)
            
            answer = response_data.get("answer", "")
            is_sensitive = response_data.get("is_sensitive", False)
            
            if not answer:
                logging.warning(f"Empty answer received for Q{q_num}, skipping")
                continue
            
            answers_dict[question_id] = {
                "answer": answer,
                "is_sensitive": is_sensitive,
                "question_num": q_num
            }
            logging.info(f"Generated answer for Q{q_num} (ID: {question_id})")
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response for Q{q_num}: {e}. Raw response: '{response_str}'")
        except Exception as e:
            logging.error(f"Failed to generate answer for Q{q_num}: {e}")
    
    context["ti"].xcom_push(key="answers_dict", value=answers_dict)
    logging.info(f"Generated {len(answers_dict)}/{len(questions_with_id)} answers")
    return answers_dict

# =============================================================================
# Task 5: Update Answers to Database
# =============================================================================
def update_answers_to_database(**context):
    """Update all generated answers to the database via API"""
    answers_dict = context["ti"].xcom_pull(task_ids="generate_answers_with_ai", key="answers_dict")
    conf = context["dag_run"].conf
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    if not answers_dict:
        logging.warning("No answers to update")
        return {"total": 0, "updated": 0}
    
    updated_count = 0
    
    for question_id, answer_data in answers_dict.items():
        answer = answer_data["answer"]
        is_sensitive = answer_data["is_sensitive"]
        question_num = answer_data["question_num"]
        
        url = f"{RFP_API_BASE}/rfp/questions/{question_id}"
        payload = {
            "answertext": answer,
            "is_sensitive": is_sensitive
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json", "WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
        
        try:
            response = requests.patch(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            updated_count += 1
            logging.info(f"Updated answer for Q{question_num} (ID: {question_id})")
        except Exception as e:
            logging.error(f"Failed to update answer for Q{question_num} (ID: {question_id}): {e}")
    
    logging.info(f"Updated {updated_count}/{len(answers_dict)} answers to database")
    return {"total": len(answers_dict), "updated": updated_count}
# =============================================================================
# Task 6: Update Run ID & Log Completion
# =============================================================================
def update_run_id_and_log(**context):
    """Update processing_dag_run_id via API and log final completion status"""
    conf = context["dag_run"].conf or {}
    project_id = conf.get("project_id")
    workspace_uuid = conf.get("workspace_uuid", "")
    user_email = conf.get("x-ltai-user-email", "")
    dag_run_id = context["dag_run"].run_id

    if not project_id:
        logging.warning("No project_id found in configuration. Skipping API update.")
        return
    
    url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
    payload = {"processing_dag_run_id": dag_run_id}
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
        response = requests.patch(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info(f"Successfully updated project {project_id} with processing_dag_run_id: {dag_run_id}")
    except Exception as e:
        logging.error(f"Failed to update project {project_id} run_id: {e}")

    questions_with_id = context["ti"].xcom_pull(task_ids="validate_and_fix_questions", key="questions_with_id")
    questions_count = len(questions_with_id) if questions_with_id else 0
    
    logging.info(f"RFP Processing DAG completed for project_id={project_id} | Questions processed: {questions_count}")

# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id="rfp_subadvisory_processing_dag",
    default_args=default_args,
    description="Processes Sub-Advisory RFPs: Extracts questions, generates answers, updates via API",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "rfp", "public-pension", "processing"],
    max_active_runs=5,
    params={
        "project_id": Param(
            type="integer",
            minimum=1,
            title="Project ID",
            description="RFP project ID from selector",
        ),
        "workspace_uuid": Param(
            type="string",
            title="Workspace UUID",
            description="The UUID of the workspace",
        ),
        "x-ltai-user-email": Param(
            type="string",
            title="User Email",
            description="Email of the user initiating the DAG run",
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    fetch_pdf = PythonOperator(
        task_id="fetch_pdf_from_api",
        python_callable=fetch_pdf_from_api,
        provide_context=True,
    )

    extract_questions = PythonOperator(
        task_id="extract_questions_with_ai",
        python_callable=extract_questions_with_ai,
        provide_context=True,
    )

    validate_fix_questions = PythonOperator(
        task_id="validate_and_fix_questions",
        python_callable=validate_and_fix_questions,
        provide_context=True,
    )

    generate_answers = PythonOperator(
        task_id="generate_answers_with_ai",
        python_callable=generate_answers_with_ai,
        provide_context=True,
    )

    update_answers = PythonOperator(
        task_id="update_answers_to_database",
        python_callable=update_answers_to_database,
        provide_context=True,
    )

    finalize = PythonOperator(
        task_id="log_completion",
        python_callable=update_run_id_and_log,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task dependencies
    fetch_pdf >> extract_questions >> validate_fix_questions >> generate_answers >> update_answers >> finalize