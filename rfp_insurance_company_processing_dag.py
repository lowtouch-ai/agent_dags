from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable, Param
import logging
import json
import requests
import tempfile
import os
import pymupdf4llm
from ollama import Client
import re
import time

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

def extract_local_context(
    full_text: str,
    anchor: str,
    window: int = 1200
) -> str:
    """
    Extract a bounded context window around the anchor text.
    """
    idx = full_text.find(anchor)
    if idx == -1:
        return ""

    start = max(0, idx - window)
    end = min(len(full_text), idx + len(anchor) + window)
    return full_text[start:end]

def derive_answer_instructions(question_text: str, section: str, local_context: str, headers: dict) -> str:
    """
    Derive explicit answer-writing instructions for a question based on its text and section.
    """
    prompt = f"""
You are an expert RFP response advisor.

Determine the exact instructions the respondent must follow when answering the question below.

Question text:
{question_text}

Section:
{section}

Nearby RFP context (authoritative):
{local_context if local_context else "No additional context available."}

Infer the expected answer format, structure, and constraints.

The instruction MUST:
- Be a single sentence
- Be no more than 25 words
- Describe HOW to answer, not WHAT the answer is
- Specify format requirements (e.g. table, list, Yes/No, attachment) if implied

Return ONLY the instruction sentence.
Do NOT explain your reasoning.
"""
    
    return get_ai_response(prompt, headers=headers).strip()

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
        
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if workspace_uuid:
        headers["WORKSPACE_UUID"] = workspace_uuid
    if user_email:
        headers["x-ltai-user-email"] = user_email

    # Ensure status is 'generating'
    set_project_status(project_id, "generating", headers)

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
        text = ""
        # Create a temporary file to work with pymupdf4llm
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            tmp_file.write(pdf_bytes)
            tmp_path = tmp_file.name

        try:
            # Convert PDF to Markdown to preserve structure (tables, headers)
            text = pymupdf4llm.to_markdown(tmp_path, write_images=False)
            logging.info(f"Converted PDF to Markdown via pymupdf4llm. Length: {len(text):,}")
        except Exception as e:
            logging.error(f"pymupdf4llm conversion failed: {e}")
            raise
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

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
You are a **Senior RFP Structuring Analyst** specializing in extracting vendor response requirements from complex government and enterprise RFP documents.
Your task is to **identify, normalize, and extract EVERY vendor response requirement (“question”) from the provided RFP document**, regardless of how it is phrased or formatted.
* * *
## 1. WHAT COUNTS AS A QUESTION (NON-NEGOTIABLE)
A **question** is **ANY requirement that expects the vendor to provide information, confirmation, data, documentation, or a declaration**, even if:
   _It is_ _not written as a question_*
   _It appears as a_ _form field, table, checklist, checkbox, or declaration_*
   _It is phrased as an_ _instruction or statement_*
   _It is_ _implicitly requesting information_* (e.g., blank fields, column headers, labels)
If a vendor would reasonably be expected to respond to it, **it IS a question**.

### IMPORTANT EXCLUSION RULE

Do NOT extract the following as questions:
- General procedural rules, policies, or conditions of participation
- Instructions that explain HOW the RFP process operates (e.g. submission mechanics, evaluation process, probity rules)
- Obligations that do NOT require the vendor to provide a response, field entry, document, confirmation, or declaration
- If a requirement cannot be answered with text, a document, a completed field, or a Yes/No response, it MUST NOT be extracted.

Only extract items where the Respondent is required to:
- Actively provide information
- Complete a form, table, or schedule
- Submit a document or declaration
- Explicitly confirm or acknowledge something as part of their Proposal response

* * *
## 2. QUESTION FORMATS YOU MUST RECOGNISE
You MUST actively look for and extract questions posed in **all of the following formats**:
### A. Narrative / Instructional Requests
Examples:
*   “Please describe…”
*   “Provide details of…”
*   “Demonstrate your ability to…”
### B. Yes / No Questions (Binary)
Examples:
*   “Do you have…?”
*   “Has your organisation…?”
### C. Yes / No with Conditional Detail
Examples:
*   “If Yes, please provide details…”
*   “If selected, will your organisation be in a position to…”
:arrow_right: These MUST be extracted as **two questions**:
1.  The Yes/No confirmation
2.  The conditional explanation
### D. Form Fields / Fill-in-the-Blank Items
Examples:
*   “Name of Respondent”
*   “ACN / ABN”
*   “Registered office address”
These are **implicit questions** and MUST be converted into explicit requests:
> “Provide the Name of Respondent.”
### E. Tables Requiring Vendor Input
If a table has:
*   Column headers
*   Empty cells
*   “Respondent to populate”
:arrow_right: Treat the **entire table as ONE question**, unless the rows clearly represent unrelated requests.
### F. Checklists / Tick-Box Confirmations
Examples:
*   “Please confirm the following documents are attached”
*   Lists of items with checkboxes
### G. Declarations / Certifications / Acknowledgements
Examples:
*   “The Respondent acknowledges and agrees that…”
*   Undertakings, signatures, certifications
These are **mandatory confirmation questions**, even if phrased declaratively.
* * *
## 3. SECTION IDENTIFICATION RULES (UPDATED – LOGICAL HEADERS ONLY)
Each extracted question MUST be assigned a **single logical section string** that reflects the **actual requirement heading**, not the page or schedule container.
### Section Resolution Priority (STRICT ORDER)
1.  **Nearest titled subsection with both number and name**
    Examples:
    *   `1.2 Respondent’s details`
    *   `1.3 Disclosure of Conflicts of Interest`
    *   `14.18 Legal complaints and inquiries`
2.  **Nearest titled numbered section**
    Examples:
    *   `Section 1 – Respondent’s details`
    *   `Section 3 – Required attachments`
3.  **Named schedule ONLY if no titled section exists below it**
    Examples:
    *   `Returnable Schedule 14`
    *   `Returnable Schedule 7 – Price`
4.  If none apply → `"General Requirements"`
### Critical Rules:
+ If a numbered subsection (e.g. "1.2 Respondent’s details") exists, you MUST use that subsection and MUST NOT use the enclosing Schedule or Form title.
   Prefer descriptive section headings over schedule or page titles
   Do NOT use page-level headers like:
     ✗ Returnable Schedule 14 if 14.1 Information Security Questionnaire exists

   _Use the_ _exact wording_* of the section as written
   _All questions under the same heading MUST use_ _identical section strings_*
*   Do NOT invent or summarize section names
* * *
## 4. HOW TO WRITE THE QUESTION TEXT (CRITICAL)
The `"text"` field must be written so that:
> **If this question were given alone to an LLM, the LLM could answer it correctly without seeing the original document.**
### Normalization Rules:
*   Convert labels, blanks, and table headers into explicit instructions
*   Merge bullet points, sub-fields, and table columns into one coherent request
*   Preserve legal, compliance, and contractual intent
*   Remove layout artifacts such as “Response”, “Information requested”, or column labels like “Respondent response”
### Examples:
**Form fields**
```
Name of Respondent
ACN/ABN
Registered office address
```
:arrow_right:
```
Provide the following Respondent details: Name of Respondent, ACN/ABN, and Registered office address.
```
**Table**
```
Name | Role | Position | Start Date
[Respondent to populate]
```
:arrow_right:
```
Provide details of the proposed resources by completing a table including Name, Role, Position, and Available Start Date.
```
**Declaration**
```
The Respondent acknowledges and agrees that it will comply with privacy legislation.
```
:arrow_right:
```
Confirm and acknowledge that the Respondent will comply with all applicable privacy legislation, including the Privacy and Personal Information Protection Act and the Health Records and Information Privacy Act.
```
* * *
## 5. QUESTION NUMBERING RULES (UPDATED – CLEAN KEYS)
   _Use_ _only the document’s numeric or alphanumeric identifiers_*
    *   :white_check_mark: `1.1`
    *   :white_check_mark: `1.3.a`
    *   :white_check_mark: `14.18.b`
*   :x: Do NOT prefix keys with `"Section"` or other text
*   If a question is unnumbered but clearly under a numbered section, infer logically:
    *   Under `Section 2 – Proposal details` → `2.1`, `2.2`
*   For Yes/No + conditional pairs, use `.a`, `.b`
Each key in the JSON MUST be unique.
* * *
## 6. FINAL OUTPUT FORMAT (UNCHANGED)
Return **ONLY valid JSON** in the following format:
```json
{{
  "{{Question number}}": {{
    "section": "{{Exact logical section header this question belongs to}}",
    "text": "{{A fully self-contained question written as a single, answerable instruction}}"
  }}
}}
```
### Mandatory Constraints:
*   :x: Do NOT include any additional fields
*   :x: Do NOT include explanations outside JSON
*   :x: Do NOT group unrelated requests
   _:white_check_mark: Prefer_ _over-extraction_* to missing questions
*   :white_check_mark: Treat compliance, confirmations, and declarations as first-class questions ONLY when the Respondent is explicitly required to provide a confirmation, declaration, certification, or completed response as part of the Proposal or a Returnable Schedule.

* * *
## 7. FINAL GUIDANCE
Assume this RFP is **legally binding and evaluation-critical**.
If a vendor could reasonably be scored, disqualified, or contractually bound based on a response, **it MUST be extracted as a question**.

## EXECUTION MODE (CRITICAL)

This is a **pure extraction and transformation task**, not an analysis or explanation task.

You MUST:
- Perform the extraction silently
- NOT describe steps, phases, reasoning, or intermediate analysis
- NOT summarize the document
- NOT explain what you are doing

Your response MUST consist of the final JSON output only.

Document preview(Pre-extracted RFP content (treat as authoritative source)): {extracted_text}
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

    for idx, (q_num, q_data) in enumerate(questions_dict.items(), start=1):
        if isinstance(q_data, str):
            q_text = q_data
            q_section = "General"
        else:
            q_text = q_data.get("text", "")
            q_section = q_data.get("section", "General")

        if not q_text.strip():
            continue

        local_context = extract_local_context(extracted_text, q_num)

        if not local_context:
            local_context = extract_local_context(
                extracted_text,
                q_text[:60]  # stable semantic anchor
            )

        answer_instructions = derive_answer_instructions(
            q_text.strip(),
            q_section.strip(),
            local_context,
            headers
        )
        payload = {
            "questiontext": q_text.strip(),
            "section": q_section.strip(),
            "questionorder": idx,
            "answer_instructions": answer_instructions
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
                "section": q_section.strip(),
                "answer_instructions": answer_instructions,
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
    extracted_text = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api", key="extracted_text")
    def sort_key_generator(key):
        # 1. Clean the key of trailing dots or whitespace
        clean_key = key.strip().rstrip('.')
        
        # 2. Split by dot OR underscore (handling both '1.1' and 'Section_1')
        parts = re.split(r'[._]', clean_key)
        
        # 3. Primary Sort: Try to parse the *last* part as a number (e.g. '1' in 'Section_1')
        #    If that fails, try the first part. Fallback to 999999.
        try:
            primary = int(parts[-1]) 
        except ValueError:
            try:
                primary = int(parts[0])
            except ValueError:
                primary = 999999
        
        return (primary, clean_key)

    current_keys = sorted(questions_dict.keys(), key=sort_key_generator)
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
Extract ONLY these missing questions from the document. Do not touch existing ones. Determine the section they belong to.

Missing numbers: {', '.join(missing_list)}

Return ONLY a valid JSON object (dict) with these keys and their question text. No other text, explanations, or markdown.

Example: {{"3.3": {{"text": "What is the deadline?", "section": "Timeline"}}, "5.2": {{"text": "Describe the process.", "section": "Operations"}}}}

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

    for idx, (q_num, q_data) in enumerate(missing_dict.items(), start=len(questions_dict) + 1):
        if isinstance(q_data, str):
            q_text = q_data
            q_section = "General"
        else:
            q_text = q_data.get("text", "")
            q_section = q_data.get("section", "General")

        if not q_text.strip():
            continue

        local_context = extract_local_context(extracted_text, q_num)

        if not local_context:
            local_context = extract_local_context(
                extracted_text,
                q_text[:60]
            )

        answer_instructions = derive_answer_instructions(
            q_text.strip(),
            q_section.strip(),
            local_context,
            headers
        )

        payload = {
            "questiontext": q_text.strip(),
            "section": q_section.strip(),
            "questionorder": idx,
            "answer_instructions": answer_instructions
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
                "section": q_section.strip(),
                "answer_instructions": answer_instructions,
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
        answer_instructions = question_data.get(
            "answer_instructions",
            "Provide a clear, complete response following standard RFP submission conventions."
        )
        question_id = question_data["id"]
        
        prompt_answer = f"""
You are an expert answering Insurance Company RFP questions for our firm.

Question {q_num}: {question_text}

Answer instructions (MANDATORY):
{answer_instructions}

Follow the instructions strictly.

Provide a complete, professional answer. Use bullet points if needed. Be concise yet thorough.

At the end, list any sources from the RFP document you referenced and your confidence level.

IMPORTANT: Respond with ONLY a valid JSON object in this exact format:
{{
  "answer": "your detailed answer text here (do not include sources or confidence in this field)",
  "sources_referenced": ["<insert actual section/page found in text>", "<insert another actual source if applicable>"],
  "confidence": "High" or "Medium" or "Low",
  "is_sensitive": true or false
}}

Do not add any extra text, markdown, or explanations outside the JSON.
"""
        
        # Initialize variables before the retry loop
        answer = None
        sources = []
        confidence = None
        is_sensitive = False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response_str = get_ai_response(prompt_answer, headers=headers).strip()
                response_data = extract_json_from_response(response_str)
                
                raw_answer = response_data.get("answer", "")
                if isinstance(raw_answer, list):
                    answer = "\n".join(str(item) for item in raw_answer).strip()
                else:
                    answer = str(raw_answer).strip()
                sources = response_data.get("sources_referenced", [])
                confidence = response_data.get("confidence")
                is_sensitive = response_data.get("is_sensitive", False)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    logging.warning(f"Attempt {attempt + 1} failed for Q{q_num}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"All {max_retries} attempts failed for Q{q_num}: {e}")

        # Check if we got a valid answer after retries
        if not answer:
            logging.warning(f"Empty answer or failed generation for Q{q_num}, skipping.")
            continue
        
        answers_dict[question_id] = {
            "answer": answer,
            "sources_referenced": sources if isinstance(sources, list) else [],
            "confidence": confidence if confidence else None,
            "is_sensitive": is_sensitive,
            "question_num": q_num,
            "answer_instructions": answer_instructions
        }
        logging.info(f"Generated answer for Q{q_num} (ID: {question_id}) with sources and confidence")
    
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
            "sources_referenced": answer_data.get("sources_referenced", []),
            "confidence": answer_data.get("confidence"),
            "is_sensitive": is_sensitive,
            "answer_instructions": answer_data.get("answer_instructions")
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
    payload = {"processing_dag_run_id": dag_run_id, "status": "review"}
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
    dag_id="rfp_insurance_company_processing_dag",
    default_args=default_args,
    description="Processes Insurance Company RFPs: Extracts questions, generates answers, updates via API",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "rfp", "public-pension", "processing"],
    max_active_runs=5,
    on_failure_callback=handle_task_failure,
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