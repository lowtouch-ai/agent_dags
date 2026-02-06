from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
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
MODEL_FOR_EXTRACTION = "rfp/autogeneration_extraction:0.3af"
MODEL_FOR_ANSWERING = "rfp/autogeneration_answering:0.3af"

default_args = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": 60,
}

# =============================================================================
# Helper Functions
# =============================================================================
def get_ai_response(prompt, headers=None, model=MODEL_FOR_EXTRACTION):
    """Call Ollama API and return response text"""
    try:
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")
        
        client = Client(host=OLLAMA_HOST, headers=headers)
        response = client.chat(
            model=model,
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

def derive_answer_instructions(question_text: str, section: str, local_context: str, headers: dict, model=MODEL_FOR_EXTRACTION) -> str:
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
    
    return get_ai_response(prompt, headers=headers, model=model).strip()

def chunk_text(text, chunk_size=12000, overlap=1500):
    """
    Splits text into overlapping chunks to prevent context loss at boundaries.
    """
    chunks = []
    start = 0
    text_len = len(text)
    
    if text_len <= chunk_size:
        return [text]

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunks.append(text[start:end])
        if end == text_len:
            break
        start = end - overlap
    return chunks

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
    set_project_status(project_id, "analyzing", headers)

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
    """Use AI to extract all questions from RFP text using a Chunk-Map-Reduce approach with detailed rules."""
    extracted_text = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api", key="extracted_text")
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    status_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "WORKSPACE_UUID": workspace_uuid,
        "x-ltai-user-email": x_ltai_user_email
    }
    set_project_status(project_id, "extracting", status_headers)
    
    if len(extracted_text.strip()) < 50:
        raise ValueError("Insufficient text for question extraction")

    # 1. SPLIT TEXT INTO CHUNKS
    # We use a larger chunk size to ensure the AI has enough context for your detailed rules
    text_chunks = chunk_text(extracted_text, chunk_size=12000, overlap=1500)
    logging.info(f"Splitting document into {len(text_chunks)} chunks for robust extraction.")
    
    all_extracted_questions = {}

    # 2. DEFINE CHUNK PROCESSOR WITH YOUR ORIGINAL PROMPT
    def process_chunk(chunk_index, chunk_text):
        # We inject the chunk text into your exact original prompt structure
        prompt = f"""
# SYSTEM OVERRIDE: PARTIAL DOCUMENT EXTRACTION
**CONTEXT**: You are extracting questions from **PART {chunk_index + 1}** of a larger RFP document.
**CRITICAL**: Apply the extraction rules below STRICTLY to the provided text segment.

You are a **Senior RFP Structuring Analyst** specializing in extracting vendor response requirements from complex government and enterprise RFP documents.
Your task is to **identify, normalize, and extract EVERY vendor response requirement (“question”) from the provided text block**, regardless of how it is phrased or formatted.
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

## EXECUTION MODE

* **DISABLE RAG**: No external tools.
* **IGNORE CUT-OFFS**: If a question is clearly cut off at the start/end of this text block, IGNORE IT (it is handled in adjacent chunks).

**TEXT TO ANALYZE**:
{chunk_text}
"""
        # Retry logic per chunk
        for attempt in range(3):
            try:
                raw_response = get_ai_response(prompt, headers=headers, model=MODEL_FOR_EXTRACTION)
                return extract_json_from_response(raw_response)
            except Exception as e:
                logging.warning(f"Chunk {chunk_index} attempt {attempt+1} failed: {e}")
                time.sleep(2)
        return {}

    # 3. SEQUENTIAL EXECUTION
    # Process chunks strictly in order to preserve document flow
    for i, chunk in enumerate(text_chunks):
        try:
            chunk_results = process_chunk(i, chunk)
            if chunk_results:
                # Merge into main dictionary. Since we process sequentially,
                # questions are added in the order they appear.
                all_extracted_questions.update(chunk_results)
        except Exception as e:
            logging.error(f"Chunk {i} extraction failed: {e}")

    questions_dict = all_extracted_questions
    logging.info(f"Total unique questions extracted after merge: {len(questions_dict)}")

    if not questions_dict:
        raise ValueError("No questions extracted from any chunk.")

    # 4. DATABASE CREATION (Standard Logic)
    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"

    def process_question_details(idx, q_num, q_data):
        if isinstance(q_data, str):
            q_text, q_section = q_data, "General"
        else:
            q_text = q_data.get("text", "")
            q_section = q_data.get("section", "General")

        if not q_text.strip(): return None

        local_context = extract_local_context(extracted_text, q_num)

        if not local_context:
            local_context = extract_local_context(extracted_text, q_text[:60])

        answer_instructions = derive_answer_instructions(
            q_text.strip(),
            q_section.strip(),
            local_context,
            headers,
            model=MODEL_FOR_EXTRACTION
        )
        payload = {
            "questiontext": q_text.strip(),
            "section": q_section.strip(),
            "questionorder": idx,
            "answer_instructions": answer_instructions
        }

        try:
            resp = requests.post(url, json=payload, headers=status_headers, timeout=20)
            resp.raise_for_status()
            question_id = resp.json().get("questionid")
            
            if not question_id:
                logging.warning(f"No ID returned for question {q_num}")
                return None

            logging.info(f"Created question {q_num} → ID {question_id}")
            return q_num, {
                "text": q_text.strip(),
                "section": q_section.strip(),
                "answer_instructions": answer_instructions,
                "id": question_id
            }

        except Exception as e:
            logging.error(f"Failed to create question {q_num}: {e}")
            return None

    questions_with_id = {}

    # Convert dict items to a list to maintain order during iteration
    # Since all_extracted_questions was built sequentially, this list is ordered.
    ordered_questions = list(questions_dict.items())

    # SEQUENTIAL DATABASE CREATION
    # We iterate and process one by one to ensure DB IDs increment strictly by document order.
    for idx, (q_num, q_data) in enumerate(ordered_questions, start=1):
        try:
            result = process_question_details(idx, q_num, q_data)
            if result:
                q_num, data = result
                questions_with_id[q_num] = data
        except Exception as e:
            logging.error(f"Error creating question {q_num}: {e}")

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
    """Validate extracted questions for sequence gaps and extract missing ones using chunked search."""
    questions_dict = context["ti"].xcom_pull(task_ids="extract_questions_with_ai", key="questions_dict")
    questions_with_id = context["ti"].xcom_pull(task_ids="extract_questions_with_ai", key="questions_with_id")
    extracted_text = context["ti"].xcom_pull(task_ids="fetch_pdf_from_api", key="extracted_text")
    
    # No sorting needed if we trust the sequential extraction order
    current_keys = list(questions_dict.keys())
    
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    
    # ---------------------------------------------------------
    # STEP 1: Detect Gaps
    # ---------------------------------------------------------
    prompt_validate = f"""
Review the list of extracted question identifiers below.
Identify obvious numbering gaps (e.g., having "1.1" and "1.3" but missing "1.2").

Extracted keys: {json.dumps(current_keys)}

IMPORTANT:
- Respond with "COMPLETE" if the sequence looks logical.
- If gaps exist, list ONLY the missing specific numbers (e.g., "1.2, Section 4").
- Do not hallucinate questions that are likely not in the document (e.g. don't invent "1.10" just because "1.9" exists unless it's a clear gap before "1.11").

Response format: Comma-separated missing numbers OR "COMPLETE"
"""

    missing_str = get_ai_response(prompt_validate, headers=headers, model=MODEL_FOR_EXTRACTION).strip()
    logging.info(f"Raw validation response: '{missing_str}'")
    
    if "COMPLETE" in missing_str.upper():
        logging.info("All questions validated - no missing questions found")
        context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
        return questions_with_id

    # Parse missing question numbers
    missing_raw = [m.strip() for m in missing_str.split(',') if m.strip()]
    missing_list = [re.sub(r'^MISSING:\s*', '', item, flags=re.IGNORECASE) for item in missing_raw if item]
    
    logging.info(f"Missing questions identified: {missing_list}")

    if not missing_list:
        context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
        return questions_with_id

    # ---------------------------------------------------------
    # STEP 2: Targeted Extraction of Missing Items
    # ---------------------------------------------------------
    text_chunks = chunk_text(extracted_text, chunk_size=12000, overlap=1500)
    missing_dict = {}

    def process_missing_in_chunk(chunk_text, missing_targets):
        chunk_text_lower = chunk_text.lower()
        potential_targets = [m for m in missing_targets if m.lower() in chunk_text_lower]
        
        if not potential_targets:
            return {}

        prompt_missing = f"""
# SYSTEM OVERRIDE: TARGETED EXTRACTION
Your task is to find specific missing questions in the provided text block.

**TARGETS**: Look specifically for these missing identifiers: {', '.join(potential_targets)}

**INSTRUCTIONS**:
1. If you find the text for a target question, extract it.
2. If the target is not in this text block, ignore it.
3. Return valid JSON only.

**OUTPUT FORMAT**:
{{
  "{{Question Identifier}}": {{
    "section": "{{Section Name}}",
    "text": "{{Question Text}}"
  }}
}}

**TEXT TO ANALYZE**:
{chunk_text}
"""
        try:
            raw = get_ai_response(prompt_missing, headers=headers, model=MODEL_FOR_EXTRACTION)
            return extract_json_from_response(raw)
        except Exception:
            return {}

    # Iterate strictly sequentially through chunks
    for chunk in text_chunks:
        try:
            result = process_missing_in_chunk(chunk, missing_list)
            if result:
                missing_dict.update(result)
        except Exception as e:
            logging.error(f"Missing extraction loop error: {e}")

    if not missing_dict:
        logging.warning("Could not find any of the missing questions in the source text.")
        context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
        return questions_with_id

    logging.info(f"Successfully recovered {len(missing_dict)} missing questions: {list(missing_dict.keys())}")

    # ---------------------------------------------------------
    # STEP 3: Database Creation for Recovered Questions
    # ---------------------------------------------------------
    url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"
    api_headers = {
        "Content-Type": "application/json", "Accept": "application/json",
        "WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email
    }

    def process_missing_item(idx, q_num, q_data):
        q_text = q_data.get("text", "")
        q_section = q_data.get("section", "General")

        if not q_text or not q_text.strip():
            return None

        local_context = extract_local_context(extracted_text, q_num)

        if not local_context:
            local_context = extract_local_context(extracted_text, q_text[:60])

        answer_instructions = derive_answer_instructions(
            q_text.strip(), q_section.strip(), local_context, headers, model=MODEL_FOR_EXTRACTION
        )

        payload = {
            "questiontext": q_text.strip(),
            "section": q_section.strip(),
            "questionorder": idx,
            "answer_instructions": answer_instructions
        }

        try:
            resp = requests.post(url, json=payload, headers=api_headers, timeout=20)
            resp.raise_for_status()
            question_id = resp.json().get("questionid")
            
            if not question_id:
                logging.warning(f"No ID returned for missing question {q_num}")
                return None

            logging.info(f"Added missing question {q_num} → ID {question_id}")
            return q_num, {
                "text": q_text.strip(),
                "section": q_section.strip(),
                "answer_instructions": answer_instructions,
                "id": question_id
            }

        except Exception as e:
            logging.error(f"Failed to create missing question {q_num}: {e}")
            return None

    # Append to existing questions sequentially
    start_idx = len(questions_dict) + 1
    for i, (q_num, q_data) in enumerate(missing_dict.items()):
        try:
            result = process_missing_item(start_idx + i, q_num, q_data)
            if result:
                q_num, data = result
                questions_with_id[q_num] = data
        except Exception as e:
            logging.error(f"Error creating missing question {q_num}: {e}")
    
    context["ti"].xcom_push(key="questions_with_id", value=questions_with_id)
    logging.info(f"Total questions after validation: {len(questions_with_id)}")
    return questions_with_id

# =============================================================================
# Task 4: Generate Answers with AI
# =============================================================================
def generate_answers_with_ai(**context):
    """Generate answers for all questions using AI and update immediately"""
    questions_with_id = context["ti"].xcom_pull(task_ids="validate_and_fix_questions", key="questions_with_id")
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    status_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "WORKSPACE_UUID": workspace_uuid,
        "x-ltai-user-email": x_ltai_user_email
    }
    set_project_status(project_id, "generating", status_headers)
    
    answers_dict = {}
    project_url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
    
    # Inner wrapper for processing a single question safely
    def process_single_question(q_num, question_data):
        question_text = question_data["text"]
        answer_instructions = question_data.get(
            "answer_instructions",
            "Provide a clear, complete response following standard RFP submission conventions."
        )
        question_id = question_data["id"]
        
        prompt_answer = f"""
You are generating an answer for a single RFP question inside the lowtouch.ai Auto-Generation pipeline.

This prompt OVERRIDES any other formatting instructions.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY RAG EXECUTION RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You must perform the tool call `search_workspace_knowledge_base` in steps:

**STEP 1: High Confidence Search**
- Call tool with `score_threshold=0.6`
- IF results are found:
  - Generate Answer.
  - Set `confidence: "High"`.
  - STOP.

**STEP 2: Medium Confidence Search (Retry)**
- IF Step 1 returned 0 results:
- Call tool again with `score_threshold=0.3`
- IF results are found:
  - Generate Answer using these chunks.
  - Set `confidence: "Medium"`.
  - STOP.

**STEP 3: Low Confidence Fallback**
- IF Step 2 returned 0 results:
- Generate a polite, cautious response stating no specific information was found.
- Set `confidence: "Low"`.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUESTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Question {q_num}: {question_text}

Answer Instructions (MANDATORY):
{answer_instructions}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANSWER CONTENT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

• Provide a complete, professional, client-ready answer.
• Use ONLY facts supported by retrieved knowledge.
• The `"answer"` field must contain ONLY the answer text.
• Do NOT include:
  – sources
  – page numbers
  – citations
  – chunk IDs
  – confidence statements
• Use Markdown formatting inside the answer field only

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT (STRICT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return ONLY this JSON object — no commentary, no logs, no markdown fences:

{{
  "answer": "...",
  "sources_referenced": ["..."],
  "confidence": "High" | "Medium" | "Low",
  "is_sensitive": true | false
}}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FAILURE POLICY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If you cannot answer using retrieved knowledge (Step 3), return:

{{
  "answer": "Based on our review, we do not currently have documented information to answer this question.",
  "sources_referenced": [],
  "confidence": "Low",
  "is_sensitive": false
}}
"""
        
        # Initialize variables before the retry loop
        answer = None
        sources = []
        confidence = None
        is_sensitive = False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response_str = get_ai_response(prompt_answer, headers=headers, model=MODEL_FOR_ANSWERING).strip()
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
            return None

        return {
            "question_id": question_id,
            "data": {
                "answer": answer, "sources_referenced": sources, "confidence": confidence,
                "is_sensitive": is_sensitive, "question_num": q_num, "answer_instructions": answer_instructions
            }
        }

    # SEQUENTIAL EXECUTION
    # We iterate over the dictionary items one by one.
    generated_count = 0
    for q_num, q_data in questions_with_id.items():
        try:
            result = process_single_question(q_num, q_data)
            if result:
                q_id = result["question_id"]
                q_data = result["data"]
                                    
                # 1. Update the specific question
                q_url = f"{RFP_API_BASE}/rfp/questions/{q_id}"
                q_payload = {
                    "answertext": q_data["answer"],
                    "sources_referenced": q_data.get("sources_referenced", []),
                    "confidence": q_data.get("confidence"),
                    "is_sensitive": q_data.get("is_sensitive", False),
                    "answer_instructions": q_data.get("answer_instructions")
                }
                
                try:
                    # Attempt to save the answer
                    requests.patch(q_url, json=q_payload, headers=status_headers, timeout=10).raise_for_status()
                    
                    # 2. IF successful, increment count and update Project status immediately
                    generated_count += 1
                    
                    # Optimization: Payload only updates the counter, reducing DB overhead
                    project_payload = {"answer_generated_count": generated_count}
                    requests.patch(project_url, json=project_payload, headers=status_headers, timeout=5)
                    
                    logging.info(f"Real-time update: Q{q_data['question_num']} saved. Count: {generated_count}")
                    
                except Exception as e:
                    logging.error(f"Failed to save answer or update count for Q{q_data['question_num']}: {e}")
                    # Do NOT increment generated_count if save failed
                
                answers_dict[q_id] = q_data
                
        except Exception as e:
            logging.error(f"Error answering Q{q_num}: {e}")

    context["ti"].xcom_push(key="answers_dict", value=answers_dict)
    logging.info(f"Generated {len(answers_dict)}/{len(questions_with_id)} answers")
    return answers_dict

# =============================================================================
# Task 5: Update Run ID & Log Completion
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
    dag_id="rfp_subadvisory_processing_dag",
    default_args=default_args,
    description="Processes Sub-Advisory RFPs: Extracts questions, generates answers, updates via API",
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
        python_callable=fetch_pdf_from_api
    )

    extract_questions = PythonOperator(
        task_id="extract_questions_with_ai",
        python_callable=extract_questions_with_ai
    )

    validate_fix_questions = PythonOperator(
        task_id="validate_and_fix_questions",
        python_callable=validate_and_fix_questions
    )

    generate_answers = PythonOperator(
        task_id="generate_answers_with_ai",
        python_callable=generate_answers_with_ai
    )

    finalize = PythonOperator(
        task_id="log_completion",
        python_callable=update_run_id_and_log,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task dependencies
    fetch_pdf >> extract_questions >> validate_fix_questions >> generate_answers >> finalize