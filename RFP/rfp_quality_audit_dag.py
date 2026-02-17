"""
RFP Quality Audit DAG

Background quality assurance and remediation process that validates and fixes
question-answer pairs after RFP processing.

IMPORTANT: This is a BACKGROUND task that does NOT affect project status.
  - Project remains in 'review' state regardless of audit outcome
  - Audit failures are logged but don't set project to 'failed'
  - Silently fixes quality issues without user intervention

Quality Checks:
  1. Duplicate question detection (fuzzy matching)
  2. Answer completeness verification
  3. Answer formatting validation
  4. Reference validity checks
  5. Confidence score distribution analysis

Auto-Remediation (when enabled):
  - Removes duplicate questions (keeps highest quality version)
  - Regenerates missing or incomplete answers
  - Regenerates low-confidence answers
  - Reformats malformed or poorly structured answers

Triggered by: Processing DAGs after completion (non-blocking trigger)
Configuration: Set ltai.v1.rfp.AUTO_REMEDIATION_ENABLED=true (default) to enable fixes
Results: Saved to project.quality_audit_summary as JSON metadata
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.models import Variable, Param
import logging
import json
import requests
import re
import time
from difflib import SequenceMatcher
from collections import defaultdict
from ollama import Client

# =============================================================================
# Configuration
# =============================================================================
OLLAMA_HOST = Variable.get("ltai.v1.rfp.OLLAMA_HOST", default_var="http://agentomatic:8000")
RFP_API_BASE = Variable.get("ltai.v1.rfp.RFP_API_BASE", default_var="http://agentconnector:8000")
QUALITY_AUDIT_MODEL = "rfp/autogeneration_extraction:0.3af"

# Quality thresholds
MIN_ANSWER_LENGTH = 10  # Minimum character count for valid answer
MAX_ANSWER_LENGTH = 50000  # Maximum reasonable answer length
DUPLICATE_SIMILARITY_THRESHOLD = 0.85  # 85% similarity = duplicate
LOW_CONFIDENCE_THRESHOLD = 0.3  # Percentage threshold for low confidence answers

# Source validation
INVALID_SOURCE_PLACEHOLDERS = [
    "none", "n/a", "na", "null", "nil", "-", "—", "unknown",
    "not provided", "not available", "tbd", "to be determined"
]
VALID_LOW_CONFIDENCE_SOURCE = "General LLM reasoning"  # Valid source for low confidence answers

# Remediation configuration
AUTO_REMEDIATION_ENABLED = Variable.get("ltai.v1.rfp.AUTO_REMEDIATION_ENABLED", default_var="true").lower() == "true"
MAX_REGENERATION_ATTEMPTS = 2  # Maximum retries for answer regeneration

# AI-enhanced quality checks
AI_COMPLETENESS_CHECK_ENABLED = Variable.get("ltai.v1.rfp.AI_COMPLETENESS_CHECK_ENABLED", default_var="true").lower() == "true"
AI_FORMATTING_CHECK_ENABLED = Variable.get("ltai.v1.rfp.AI_FORMATTING_CHECK_ENABLED", default_var="true").lower() == "true"
MIN_LENGTH_FOR_AI_CHECKS = 50  # Only run AI checks on answers with at least this many characters

DEFAULT_ARGS = {
    "owner": "lowtouch.ai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 60,
}

# =============================================================================
# Helper Functions
# =============================================================================
def get_ai_response(prompt, headers=None, model=QUALITY_AUDIT_MODEL):
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

def calculate_similarity(text1, text2):
    """Calculate similarity ratio between two strings (0.0 to 1.0)"""
    return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()

def normalize_question_text(text):
    """Normalize question text for comparison"""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    # Convert to lowercase
    text = text.lower()
    # Remove common prefixes
    text = re.sub(r'^(provide|describe|explain|outline|detail|list|confirm|certify)\s+', '', text)
    return text

def check_semantic_completeness_with_ai(question_text, answer_text, headers):
    """
    Use AI to check if answer semantically addresses the question.

    Returns tuple: (has_issue: bool, issue_type: str, severity: str, explanation: str)
    """
    try:
        prompt = f"""You are validating an RFP question-answer pair for semantic completeness.

Question: {question_text}

Answer: {answer_text}

Evaluate ONLY these aspects:
1. Does the answer directly address what the question asks?
2. Is the answer semantically complete (not cut off mid-thought)?
3. Does it provide substantive information (not placeholder text like "will provide later", "to be determined", "contact us")?
4. Is it on-topic and relevant to the question?

Return ONLY this JSON object (no commentary, no markdown fences):
{{
  "is_complete": true,
  "issue_type": null,
  "severity": null,
  "explanation": null
}}

OR if there IS an issue:
{{
  "is_complete": false,
  "issue_type": "off_topic" | "incomplete_thought" | "placeholder_text" | "circular_answer" | "does_not_address_question",
  "severity": "high" | "medium",
  "explanation": "brief reason (max 50 words)"
}}
"""

        response_text = get_ai_response(prompt, headers=headers, model=QUALITY_AUDIT_MODEL)

        # Parse JSON response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            logging.warning("AI completeness check returned non-JSON response")
            return (False, None, None, None)

        result = json.loads(json_match.group())

        if result.get("is_complete", True):
            return (False, None, None, None)
        else:
            return (
                True,
                result.get("issue_type", "semantic_incompleteness"),
                result.get("severity", "medium"),
                result.get("explanation", "AI flagged semantic issue")
            )

    except Exception as e:
        logging.warning(f"AI completeness check failed: {e}")
        return (False, None, None, None)

def check_formatting_with_ai(question_text, answer_text, headers):
    """
    Use AI to check if answer has structural/formatting issues.

    Only called for long answers (>500 chars) that lack paragraph structure.

    Returns tuple: (has_issue: bool, issue_type: str, severity: str, explanation: str)
    """
    try:
        prompt = f"""You are validating an RFP answer for FORMATTING and STRUCTURE only (NOT content quality).

Question: {question_text}

Answer:
{answer_text}

Evaluate ONLY formatting/structure:
1. Is it a wall of text that needs paragraph breaks?
2. Should it use bullet points, numbered lists, or tables but doesn't?
3. Is markdown formatting broken (e.g., incomplete tables, mismatched headers)?
4. Is it unreadable due to poor structure?

Return ONLY this JSON object (no commentary, no markdown fences):
{{
  "has_formatting_issues": false,
  "issue_type": null,
  "severity": null,
  "explanation": null
}}

OR if there IS a formatting issue:
{{
  "has_formatting_issues": true,
  "issue_type": "wall_of_text" | "missing_lists" | "missing_tables" | "broken_markdown" | "poor_structure",
  "severity": "medium" | "low",
  "explanation": "brief reason (max 50 words)"
}}
"""

        response_text = get_ai_response(prompt, headers=headers, model=QUALITY_AUDIT_MODEL)

        # Parse JSON response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            logging.warning("AI formatting check returned non-JSON response")
            return (False, None, None, None)

        result = json.loads(json_match.group())

        if not result.get("has_formatting_issues", False):
            return (False, None, None, None)
        else:
            return (
                True,
                result.get("issue_type", "formatting_issue"),
                result.get("severity", "medium"),
                result.get("explanation", "AI flagged formatting issue")
            )

    except Exception as e:
        logging.warning(f"AI formatting check failed: {e}")
        return (False, None, None, None)

def set_project_status(project_id, status, headers):
    """
    Helper to update project status via API.

    NOTE: This function should NOT be called by the quality audit DAG.
    The quality audit is a background process and should not modify project status.
    Only processing DAGs should change project status (analyzing, extracting, generating, review, failed).
    """
    try:
        url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
        payload = {"status": status}
        requests.patch(url, json=payload, headers=headers, timeout=10).raise_for_status()
        logging.info(f"Project {project_id} status updated to '{status}'")
    except Exception as e:
        logging.warning(f"Failed to update project status to '{status}': {e}")

def handle_task_failure(context):
    """
    Log failure but DO NOT modify project status.

    This is a background quality audit DAG that runs independently
    of the main RFP processing pipeline. Failures here should not
    affect the project status, which remains in 'review' state.
    """
    dag_run = context.get("dag_run")
    if not dag_run:
        return

    conf = dag_run.conf or {}
    project_id = conf.get("project_id")

    logging.error(
        f"Quality audit task failed for project {project_id}, "
        f"but project status will NOT be modified (remains in 'review')"
    )
    # Intentionally NOT calling set_project_status - this is a background audit

def _get_conf_and_headers(context):
    """Extract common config and headers from Airflow context."""
    conf = context["dag_run"].conf
    project_id = conf["project_id"]
    workspace_uuid = conf['workspace_uuid']
    x_ltai_user_email = conf['x-ltai-user-email']
    headers = {"WORKSPACE_UUID": workspace_uuid, "x-ltai-user-email": x_ltai_user_email}
    api_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "WORKSPACE_UUID": workspace_uuid,
        "x-ltai-user-email": x_ltai_user_email
    }
    return conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers

# =============================================================================
# Task 1: Fetch Project Data
# =============================================================================
def fetch_project_data(**context):
    """Fetch project and all question-answer pairs from API"""
    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)

    logging.info(f"Starting quality audit for project {project_id}")

    # Fetch project details
    project_url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
    try:
        project_response = requests.get(project_url, headers=api_headers, timeout=30)
        project_response.raise_for_status()
        project_data = project_response.json()
        logging.info(f"Fetched project data: {project_data.get('status')}")
    except Exception as e:
        logging.error(f"Failed to fetch project: {e}")
        raise

    # Fetch all questions for the project (with pagination)
    questions_url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"
    all_questions = []
    current_page = 1

    try:
        while True:
            # Request current page
            page_url = f"{questions_url}?page={current_page}"
            questions_response = requests.get(page_url, headers=api_headers, timeout=30)
            questions_response.raise_for_status()
            response_data = questions_response.json()

            # Extract questions from "data" key
            if isinstance(response_data, dict):
                page_questions = response_data.get("data", [])
                pagination = response_data.get("pagination", {})
            else:
                # Fallback for non-paginated response (list)
                page_questions = response_data
                pagination = {}

            all_questions.extend(page_questions)
            logging.info(f"Fetched page {current_page}: {len(page_questions)} questions")

            # Check if there are more pages
            next_page = pagination.get("next_page")
            if next_page:
                current_page = next_page
            else:
                break

        logging.info(f"Fetched total of {len(all_questions)} question-answer pairs across {current_page} page(s)")
    except Exception as e:
        logging.error(f"Failed to fetch questions: {e}")
        raise

    # Store in XCom for downstream tasks
    context["ti"].xcom_push(key="project_data", value=project_data)
    context["ti"].xcom_push(key="questions_data", value=all_questions)

    return {
        "project_id": project_id,
        "total_questions": len(all_questions),
        "project_status": project_data.get("status")
    }

# =============================================================================
# Task 2: Check for Duplicate Questions
# =============================================================================
def check_duplicate_questions(**context):
    """Identify duplicate or highly similar questions"""
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")

    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for duplicate check")
        context["ti"].xcom_push(key="duplicates", value=[])
        return {"duplicate_count": 0, "has_issues": False}

    duplicates = []
    questions_list = list(questions_data)

    # Compare each question with every other question
    for i in range(len(questions_list)):
        for j in range(i + 1, len(questions_list)):
            q1 = questions_list[i]
            q2 = questions_list[j]

            text1 = normalize_question_text(q1.get("questiontext", ""))
            text2 = normalize_question_text(q2.get("questiontext", ""))

            similarity = calculate_similarity(text1, text2)

            if similarity >= DUPLICATE_SIMILARITY_THRESHOLD:
                duplicates.append({
                    "question_id_1": q1.get("questionid"),
                    "question_key_1": q1.get("questionorder"),
                    "question_id_2": q2.get("questionid"),
                    "question_key_2": q2.get("questionorder"),
                    "similarity": round(similarity, 3),
                    "text_1": q1.get("questiontext", "")[:100],
                    "text_2": q2.get("questiontext", "")[:100]
                })

    logging.info(f"Found {len(duplicates)} duplicate question pairs")

    if duplicates:
        for dup in duplicates[:5]:  # Log first 5
            logging.warning(
                f"Duplicate detected: Q{dup['question_key_1']} <-> Q{dup['question_key_2']} "
                f"(similarity: {dup['similarity']})"
            )

    context["ti"].xcom_push(key="duplicates", value=duplicates)
    return {
        "duplicate_count": len(duplicates),
        "has_issues": len(duplicates) > 0
    }

# =============================================================================
# Task 3: Check Answer Completeness
# =============================================================================
def check_answer_completeness(**context):
    """
    Verify all questions have valid answers.

    Two-stage check:
      1. Fast pattern-based checks (empty, too short, error keywords)
      2. AI semantic checks (off-topic, placeholder text, incomplete thoughts)
    """
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")
    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)

    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for completeness check")
        context["ti"].xcom_push(key="completeness_issues", value=[])
        return {"issue_count": 0, "has_issues": False, "ai_checks_run": 0}

    issues = []
    ai_checks_run = 0

    for question in questions_data:
        q_id = question.get("questionid")
        q_key = question.get("questionorder")
        q_text = question.get("questiontext", "")
        answer = question.get("answertext", "").strip()

        # STAGE 1: Fast pattern-based checks
        basic_issue_found = False

        # Check for missing answers
        if not answer:
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "missing_answer",
                "severity": "high",
                "description": "Question has no answer"
            })
            basic_issue_found = True
        # Check for too short answers
        elif len(answer) < MIN_ANSWER_LENGTH:
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "incomplete_answer",
                "severity": "medium",
                "description": f"Answer too short ({len(answer)} chars)"
            })
            basic_issue_found = True
        # Check for error messages in answers
        elif any(error_phrase in answer.lower() for error_phrase in [
            "error", "failed", "unable to", "could not", "no information",
            "not found", "unavailable"
        ]):
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "error_in_answer",
                "severity": "medium",
                "description": "Answer contains error indicators"
            })
            basic_issue_found = True

        # STAGE 2: AI semantic check (only if basic checks passed and answer is long enough)
        if not basic_issue_found and AI_COMPLETENESS_CHECK_ENABLED and len(answer) >= MIN_LENGTH_FOR_AI_CHECKS:
            ai_checks_run += 1
            has_issue, issue_type, severity, explanation = check_semantic_completeness_with_ai(
                q_text, answer, headers
            )

            if has_issue:
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": issue_type,
                    "severity": severity,
                    "description": f"AI semantic check: {explanation}"
                })
                logging.info(
                    f"AI flagged Q{q_key} (ID={q_id}): {issue_type} - {explanation}"
                )

    logging.info(
        f"Answer completeness check: {len(issues)} issues found "
        f"(AI semantic checks run: {ai_checks_run})"
    )

    context["ti"].xcom_push(key="completeness_issues", value=issues)
    return {
        "issue_count": len(issues),
        "has_issues": len(issues) > 0,
        "ai_checks_run": ai_checks_run
    }

# =============================================================================
# Task 4: Check Answer Formatting
# =============================================================================
def check_answer_formatting(**context):
    """
    Validate answer formatting and structure.

    Two-stage check:
      1. Fast pattern checks (excessive length, malformed JSON, repeated text)
      2. AI structural checks (wall of text, missing lists/tables, broken markdown)
    """
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")
    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)

    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for formatting check")
        context["ti"].xcom_push(key="formatting_issues", value=[])
        return {"issue_count": 0, "has_issues": False, "ai_checks_run": 0}

    issues = []
    ai_checks_run = 0

    for question in questions_data:
        q_id = question.get("questionid")
        q_key = question.get("questionorder")
        q_text = question.get("questiontext", "")
        answer = question.get("answertext", "").strip()

        if not answer:
            continue  # Already caught by completeness check

        # STAGE 1: Fast pattern-based checks
        pattern_issue_found = False

        # Check for excessive length
        if len(answer) > MAX_ANSWER_LENGTH:
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "excessive_length",
                "severity": "low",
                "description": f"Answer extremely long ({len(answer)} chars)"
            })
            pattern_issue_found = True

        # Check for malformed JSON (if answer contains JSON artifacts)
        if answer.startswith("{") and not answer.endswith("}"):
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "malformed_json",
                "severity": "high",
                "description": "Answer appears to be incomplete JSON"
            })
            pattern_issue_found = True

        # Check for repeated text (copy-paste errors)
        words = answer.split()
        if len(words) > 20:
            # Check if first 10 words appear again later
            first_chunk = " ".join(words[:10])
            rest = " ".join(words[10:])
            if first_chunk in rest:
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": "repeated_content",
                    "severity": "medium",
                    "description": "Answer contains repeated text sections"
                })
                pattern_issue_found = True

        # STAGE 2: AI structural check (only for long answers lacking structure)
        # Criteria: >500 chars AND no paragraph breaks (no double newlines)
        if (not pattern_issue_found and
            AI_FORMATTING_CHECK_ENABLED and
            len(answer) > 500 and
            '\n\n' not in answer and
            '\n' not in answer[:200]):  # No newlines in first 200 chars = likely wall of text

            ai_checks_run += 1
            has_issue, issue_type, severity, explanation = check_formatting_with_ai(
                q_text, answer, headers
            )

            if has_issue:
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": issue_type,
                    "severity": severity,
                    "description": f"AI structural check: {explanation}"
                })
                logging.info(
                    f"AI flagged Q{q_key} (ID={q_id}) formatting: {issue_type} - {explanation}"
                )

    logging.info(
        f"Answer formatting check: {len(issues)} issues found "
        f"(AI structural checks run: {ai_checks_run})"
    )

    context["ti"].xcom_push(key="formatting_issues", value=issues)
    return {
        "issue_count": len(issues),
        "has_issues": len(issues) > 0,
        "ai_checks_run": ai_checks_run
    }

# =============================================================================
# Task 5: Check Reference Validity
# =============================================================================
def check_reference_validity(**context):
    """
    Validate sources_referenced field.

    Valid sources:
      - List with actual source references (file names, sections, page numbers)
      - ["General LLM reasoning"] for low confidence answers

    Invalid sources:
      - Missing/empty list
      - Placeholder values: "None", "N/A", "null", "-", etc.
      - Empty strings in list
    """
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")

    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for reference check")
        context["ti"].xcom_push(key="reference_issues", value=[])
        context["ti"].xcom_push(key="reference_stats", value={
            "total_answers": 0,
            "with_valid_sources": 0,
            "without_sources": 0,
            "invalid_sources": 0,
            "low_confidence_sources": 0
        })
        return {"issue_count": 0, "has_issues": False, "stats": {}}

    issues = []
    stats = {
        "total_answers": 0,
        "with_valid_sources": 0,
        "without_sources": 0,
        "invalid_sources": 0,
        "low_confidence_sources": 0
    }

    for question in questions_data:
        q_id = question.get("questionid")
        q_key = question.get("questionorder")
        answer = question.get("answertext", "").strip()
        sources = question.get("sources_referenced", [])
        confidence = question.get("confidence", "").lower()

        if not answer:
            continue

        stats["total_answers"] += 1

        # Check if sources field exists and is valid
        if not sources or not isinstance(sources, list) or len(sources) == 0:
            stats["without_sources"] += 1
            issues.append({
                "question_id": q_id,
                "question_key": q_key,
                "issue_type": "missing_sources",
                "severity": "low",
                "description": "No sources referenced"
            })
            continue

        # Check for "General LLM reasoning" (valid for low confidence only)
        if len(sources) == 1 and sources[0] == VALID_LOW_CONFIDENCE_SOURCE:
            if confidence == "low":
                stats["low_confidence_sources"] += 1
                # Valid case - low confidence with General LLM reasoning
                continue
            else:
                stats["invalid_sources"] += 1
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": "incorrect_llm_reasoning_source",
                    "severity": "medium",
                    "description": f"'General LLM reasoning' source used for {confidence} confidence answer (should only be used for low confidence)"
                })
                continue

        # Check for invalid placeholder values
        has_invalid_placeholder = False
        for src in sources:
            if not src or not src.strip():
                stats["invalid_sources"] += 1
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": "empty_source_entry",
                    "severity": "medium",
                    "description": "Sources list contains empty entries"
                })
                has_invalid_placeholder = True
                break

            # Check for placeholder strings
            src_normalized = src.strip().lower()
            if src_normalized in INVALID_SOURCE_PLACEHOLDERS:
                stats["invalid_sources"] += 1
                issues.append({
                    "question_id": q_id,
                    "question_key": q_key,
                    "issue_type": "placeholder_source_value",
                    "severity": "medium",
                    "description": f"Source contains placeholder value: '{src}'"
                })
                has_invalid_placeholder = True
                break

        if not has_invalid_placeholder:
            stats["with_valid_sources"] += 1

    logging.info(
        f"Reference validity check: {stats['with_valid_sources']}/{stats['total_answers']} "
        f"answers have valid sources, {stats['low_confidence_sources']} have 'General LLM reasoning', "
        f"{len(issues)} issues found"
    )

    context["ti"].xcom_push(key="reference_issues", value=issues)
    context["ti"].xcom_push(key="reference_stats", value=stats)
    return {
        "issue_count": len(issues),
        "has_issues": len(issues) > 0,
        "stats": stats
    }

# =============================================================================
# Task 6: Analyze Confidence Distribution
# =============================================================================
def analyze_confidence_distribution(**context):
    """Analyze confidence score distribution"""
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")

    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for confidence analysis")
        return {
            "distribution": {},
            "low_confidence_count": 0,
            "low_confidence_percentage": 0,
            "has_concern": False
        }

    confidence_counts = defaultdict(int)
    low_confidence_questions = []

    for question in questions_data:
        q_id = question.get("questionid")
        q_key = question.get("questionorder")
        answer = question.get("answertext", "").strip()
        confidence = question.get("confidence", "").strip()

        if not answer:
            continue

        # Normalize confidence value
        conf_normalized = confidence.lower() if confidence else "unknown"
        confidence_counts[conf_normalized] += 1

        # Track low confidence answers
        if conf_normalized == "low":
            low_confidence_questions.append({
                "question_id": q_id,
                "question_key": q_key,
                "confidence": confidence
            })

    total_answered = sum(confidence_counts.values())
    low_conf_percentage = (confidence_counts.get("low", 0) / total_answered * 100) if total_answered > 0 else 0

    has_concern = low_conf_percentage > (LOW_CONFIDENCE_THRESHOLD * 100)

    logging.info(
        f"Confidence distribution: High={confidence_counts.get('high', 0)}, "
        f"Medium={confidence_counts.get('medium', 0)}, Low={confidence_counts.get('low', 0)}, "
        f"Unknown={confidence_counts.get('unknown', 0)}"
    )

    if has_concern:
        logging.warning(
            f"{low_conf_percentage:.1f}% of answers have low confidence "
            f"(threshold: {LOW_CONFIDENCE_THRESHOLD * 100}%)"
        )

    context["ti"].xcom_push(key="confidence_stats", value=dict(confidence_counts))
    context["ti"].xcom_push(key="low_confidence_questions", value=low_confidence_questions)

    return {
        "distribution": dict(confidence_counts),
        "low_confidence_count": len(low_confidence_questions),
        "low_confidence_percentage": round(low_conf_percentage, 2),
        "has_concern": has_concern
    }

# =============================================================================
# Task 7: Aggregate Results
# =============================================================================
def aggregate_quality_results(**context):
    """Aggregate all quality check results"""

    # Pull all check results with defensive checks
    duplicate_result = context["ti"].xcom_pull(task_ids="check_duplicate_questions") or {}
    completeness_result = context["ti"].xcom_pull(task_ids="check_answer_completeness") or {}
    formatting_result = context["ti"].xcom_pull(task_ids="check_answer_formatting") or {}
    reference_result = context["ti"].xcom_pull(task_ids="check_reference_validity") or {}
    confidence_result = context["ti"].xcom_pull(task_ids="analyze_confidence_distribution") or {}

    # Ensure all results are dicts
    if not isinstance(confidence_result, dict):
        confidence_result = {}
    if not isinstance(duplicate_result, dict):
        duplicate_result = {}
    if not isinstance(completeness_result, dict):
        completeness_result = {}
    if not isinstance(formatting_result, dict):
        formatting_result = {}
    if not isinstance(reference_result, dict):
        reference_result = {}

    # Pull detailed issues
    duplicates = context["ti"].xcom_pull(task_ids="check_duplicate_questions", key="duplicates") or []
    completeness_issues = context["ti"].xcom_pull(task_ids="check_answer_completeness", key="completeness_issues") or []
    formatting_issues = context["ti"].xcom_pull(task_ids="check_answer_formatting", key="formatting_issues") or []
    reference_issues = context["ti"].xcom_pull(task_ids="check_reference_validity", key="reference_issues") or []

    # Ensure all are lists
    if not isinstance(duplicates, list):
        duplicates = []
    if not isinstance(completeness_issues, list):
        completeness_issues = []
    if not isinstance(formatting_issues, list):
        formatting_issues = []
    if not isinstance(reference_issues, list):
        reference_issues = []

    # Count by severity
    severity_counts = defaultdict(int)
    all_issues = completeness_issues + formatting_issues + reference_issues

    for issue in all_issues:
        if isinstance(issue, dict):
            severity_counts[issue.get("severity", "unknown")] += 1

    # Determine overall quality status
    total_issues = len(all_issues) + len(duplicates)
    has_critical_issues = severity_counts.get("high", 0) > 0

    quality_report = {
        "total_issues": total_issues,
        "duplicate_questions": len(duplicates),
        "completeness_issues": len(completeness_issues),
        "formatting_issues": len(formatting_issues),
        "reference_issues": len(reference_issues),
        "severity_breakdown": dict(severity_counts),
        "confidence_distribution": confidence_result.get("distribution", {}) if isinstance(confidence_result, dict) else {},
        "low_confidence_percentage": confidence_result.get("low_confidence_percentage", 0) if isinstance(confidence_result, dict) else 0,
        "has_critical_issues": has_critical_issues,
        "overall_status": "failed" if has_critical_issues else "passed"
    }

    logging.info("=" * 80)
    logging.info("QUALITY AUDIT REPORT")
    logging.info("=" * 80)
    logging.info(f"Total Issues Found: {total_issues}")
    logging.info(f"  - Duplicate Questions: {len(duplicates)}")
    logging.info(f"  - Completeness Issues: {len(completeness_issues)}")
    logging.info(f"  - Formatting Issues: {len(formatting_issues)}")
    logging.info(f"  - Reference Issues: {len(reference_issues)}")
    logging.info(f"Severity Breakdown: {dict(severity_counts)}")
    logging.info(f"Confidence Distribution: {quality_report['confidence_distribution']}")
    logging.info(f"Overall Status: {quality_report['overall_status'].upper()}")
    logging.info("=" * 80)

    context["ti"].xcom_push(key="quality_report", value=quality_report)
    return quality_report

# =============================================================================
# Task 8: Remediate Duplicate Questions
# =============================================================================
def remediate_duplicate_questions(**context):
    """Remove duplicate questions, keeping the one with better quality"""
    if not AUTO_REMEDIATION_ENABLED:
        logging.info("Auto-remediation is disabled. Skipping duplicate removal.")
        return {"removed_count": 0, "skipped": True}

    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)
    duplicates = context["ti"].xcom_pull(task_ids="check_duplicate_questions", key="duplicates") or []
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")

    # Ensure duplicates is a list
    if not isinstance(duplicates, list):
        duplicates = []

    if not duplicates:
        logging.info("No duplicates found. Skipping remediation.")
        context["ti"].xcom_push(key="removed_duplicate_ids", value=[])
        return {"removed_count": 0}

    # Validate questions_data
    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for duplicate remediation")
        context["ti"].xcom_push(key="removed_duplicate_ids", value=[])
        return {"removed_count": 0}

    # Create lookup for question data
    questions_by_id = {q["questionid"]: q for q in questions_data if isinstance(q, dict) and "questionid" in q}

    removed_ids = set()
    removed_count = 0

    for dup in duplicates:
        q1_id = dup["question_id_1"]
        q2_id = dup["question_id_2"]

        # Skip if either already removed
        if q1_id in removed_ids or q2_id in removed_ids:
            continue

        q1_data = questions_by_id.get(q1_id)
        q2_data = questions_by_id.get(q2_id)

        if not q1_data or not q2_data:
            continue

        # Determine which question to keep based on quality
        q1_score = _calculate_question_quality_score(q1_data)
        q2_score = _calculate_question_quality_score(q2_data)

        # Keep the higher quality question, remove the other
        keep_id, remove_id = (q1_id, q2_id) if q1_score >= q2_score else (q2_id, q1_id)

        # Delete the duplicate via API
        delete_url = f"{RFP_API_BASE}/rfp/questions/{remove_id}"
        try:
            response = requests.delete(delete_url, headers=api_headers, timeout=10)
            response.raise_for_status()
            removed_ids.add(remove_id)
            removed_count += 1
            logging.info(
                f"Removed duplicate question {remove_id} (kept {keep_id}) - "
                f"similarity: {dup['similarity']}"
            )
        except Exception as e:
            logging.error(f"Failed to delete duplicate question {remove_id}: {e}")

    logging.info(f"Duplicate remediation complete: Removed {removed_count} duplicate questions")

    # Update project question counts if duplicates were removed
    if removed_count > 0:
        try:
            # Fetch updated question list to get accurate count
            questions_url = f"{RFP_API_BASE}/rfp/projects/{project_id}/questions"
            response = requests.get(questions_url, headers=api_headers, timeout=30)
            response.raise_for_status()
            response_data = response.json()

            # Extract total count from pagination or data length
            if isinstance(response_data, dict) and "pagination" in response_data:
                remaining_count = response_data["pagination"].get("total_records", 0)
            elif isinstance(response_data, dict) and "data" in response_data:
                remaining_count = len(response_data["data"])
            else:
                remaining_count = len(response_data) if isinstance(response_data, list) else 0

            # Update project counts
            project_url = f"{RFP_API_BASE}/rfp/projects/{project_id}"
            update_payload = {
                "question_count": remaining_count,
                "answer_generated_count": remaining_count  # After deletion, counts should match
            }
            update_response = requests.patch(project_url, json=update_payload, headers=api_headers, timeout=10)
            update_response.raise_for_status()

            logging.info(
                f"Updated project {project_id} counts: question_count={remaining_count}, "
                f"answer_generated_count={remaining_count} (removed {removed_count} duplicates)"
            )
        except Exception as e:
            logging.warning(f"Failed to update project question counts after duplicate removal: {e}")
            # Don't raise - this is a non-critical update

    context["ti"].xcom_push(key="removed_duplicate_ids", value=list(removed_ids))
    return {
        "removed_count": removed_count,
        "removed_ids": list(removed_ids)
    }

def _calculate_question_quality_score(question_data):
    """Calculate quality score for a question to determine which duplicate to keep"""
    score = 0

    # Higher confidence is better
    confidence = (question_data.get("confidence") or "").lower()
    if confidence == "high":
        score += 30
    elif confidence == "medium":
        score += 20
    elif confidence == "low":
        score += 10

    # Longer answers are generally better (up to a point)
    answer = question_data.get("answertext", "").strip()
    answer_len = len(answer)
    if answer_len > MIN_ANSWER_LENGTH:
        score += min(answer_len // 100, 20)  # Cap at 20 points

    # Having sources is better
    sources = question_data.get("sources_referenced", [])
    if sources and isinstance(sources, list) and len(sources) > 0:
        score += 15

    # Not sensitive is slightly preferred (easier to use)
    if not question_data.get("is_sensitive", False):
        score += 5

    return score

# =============================================================================
# Task 9: Remediate Low Quality Answers
# =============================================================================
def remediate_low_quality_answers(**context):
    """Regenerate or reformat answers that don't meet quality standards"""
    if not AUTO_REMEDIATION_ENABLED:
        logging.info("Auto-remediation is disabled. Skipping answer remediation.")
        return {"regenerated_count": 0, "reformatted_count": 0, "skipped": True}

    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)

    # Collect all issues that need remediation with defensive checks
    completeness_issues = context["ti"].xcom_pull(task_ids="check_answer_completeness", key="completeness_issues") or []
    formatting_issues = context["ti"].xcom_pull(task_ids="check_answer_formatting", key="formatting_issues") or []
    low_confidence_questions = context["ti"].xcom_pull(task_ids="analyze_confidence_distribution", key="low_confidence_questions") or []
    questions_data = context["ti"].xcom_pull(task_ids="fetch_project_data", key="questions_data")
    removed_duplicate_ids = context["ti"].xcom_pull(task_ids="remediate_duplicate_questions", key="removed_duplicate_ids") or []

    # Ensure all are lists
    if not isinstance(completeness_issues, list):
        completeness_issues = []
    if not isinstance(formatting_issues, list):
        formatting_issues = []
    if not isinstance(low_confidence_questions, list):
        low_confidence_questions = []
    if not isinstance(removed_duplicate_ids, list):
        removed_duplicate_ids = []

    # Validate questions_data
    if not questions_data or not isinstance(questions_data, list):
        logging.warning("No questions data available for remediation")
        return {"regenerated_count": 0, "reformatted_count": 0, "skipped": False}

    # Create lookup for question data
    questions_by_id = {q["questionid"]: q for q in questions_data if isinstance(q, dict) and "questionid" in q}

    # Identify questions that need regeneration (high severity)
    needs_regeneration = set()
    for issue in completeness_issues:
        if issue["severity"] == "high":  # Missing or very incomplete answers
            needs_regeneration.add(issue["question_id"])

    # Add low confidence questions to regeneration list
    for lc_q in low_confidence_questions:
        needs_regeneration.add(lc_q["question_id"])

    # Identify questions that need reformatting (medium severity formatting issues)
    needs_reformatting = set()
    for issue in formatting_issues:
        if issue["severity"] in ["high", "medium"] and issue["question_id"] not in needs_regeneration:
            needs_reformatting.add(issue["question_id"])

    # Remove any questions that were deleted as duplicates
    skipped_regen = needs_regeneration & set(removed_duplicate_ids)
    skipped_reformat = needs_reformatting & set(removed_duplicate_ids)
    needs_regeneration = needs_regeneration - set(removed_duplicate_ids)
    needs_reformatting = needs_reformatting - set(removed_duplicate_ids)

    if skipped_regen or skipped_reformat:
        logging.info(
            f"Skipping {len(skipped_regen)} regeneration and {len(skipped_reformat)} reformatting tasks "
            f"for questions deleted as duplicates: {sorted(skipped_regen | skipped_reformat)}"
        )

    logging.info(
        f"Answer remediation plan: {len(needs_regeneration)} to regenerate, "
        f"{len(needs_reformatting)} to reformat"
    )

    regenerated_count = 0
    reformatted_count = 0

    # Regenerate answers for high-severity issues
    for q_id in needs_regeneration:
        q_data = questions_by_id.get(q_id)
        if not q_data:
            continue

        success = _regenerate_answer(q_id, q_data, headers, api_headers)
        if success:
            regenerated_count += 1

    # Reformat answers for medium-severity issues
    for q_id in needs_reformatting:
        q_data = questions_by_id.get(q_id)
        if not q_data:
            continue

        success = _reformat_answer(q_id, q_data, headers, api_headers)
        if success:
            reformatted_count += 1

    logging.info(
        f"Answer remediation complete: {regenerated_count} regenerated, "
        f"{reformatted_count} reformatted"
    )

    return {
        "regenerated_count": regenerated_count,
        "reformatted_count": reformatted_count
    }

def _regenerate_answer(question_id, question_data, headers, api_headers):
    """
    Regenerate a single answer using the AI model.

    For low confidence answers, ensures sources_referenced is set to
    ["General LLM reasoning"] if the AI returns empty or invalid sources.
    """
    question_text = question_data.get("questiontext", "")
    answer_instructions = question_data.get("answer_instructions",
                                            "Provide a clear, complete response following standard RFP submission conventions.")

    # Use same prompt as original generation
    prompt = f"""
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

Question: {question_text}

Answer Instructions (MANDATORY):
{answer_instructions}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANSWER CONTENT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Provide a complete, professional, client-ready answer.
- Use ONLY facts supported by retrieved knowledge.
- The `"answer"` field must contain ONLY the answer text.
- Do NOT include:
  - sources
  - page numbers
  - citations
  - chunk IDs
  - confidence statements
- Use Markdown formatting inside the answer field only

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT (STRICT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return ONLY this JSON object - no commentary, no logs, no markdown fences:

{{
  "answer": "...",
  "sources_referenced": ["..."],
  "confidence": "High" | "Medium" | "Low",
  "is_sensitive": true | false
}}
"""

    for attempt in range(MAX_REGENERATION_ATTEMPTS):
        try:
            from ollama import Client
            client = Client(host=OLLAMA_HOST, headers=headers)
            response = client.chat(
                model="rfp/autogeneration_answering:0.3af",
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )

            raw_response = response['message']['content'].strip()

            # Parse JSON response
            import re
            json_match = re.search(r'\{[\s\S]*\}', raw_response)
            if json_match:
                response_data = json.loads(json_match.group())

                answer = response_data.get("answer", "").strip()
                if not answer or len(answer) < MIN_ANSWER_LENGTH:
                    raise ValueError("Generated answer too short")

                # Extract confidence and sources
                confidence = response_data.get("confidence", "Medium")
                sources = response_data.get("sources_referenced", [])

                # IMPORTANT: For low confidence answers, ensure sources is set to "General LLM reasoning"
                # if no valid sources were found by the AI
                if confidence.lower() == "low":
                    # Check if sources is empty or contains only empty/whitespace strings
                    if not sources or len(sources) == 0 or all(not s or not s.strip() for s in sources):
                        sources = [VALID_LOW_CONFIDENCE_SOURCE]  # ["General LLM reasoning"]
                        logging.info(
                            f"Set sources to 'General LLM reasoning' for low confidence answer (question {question_id})"
                        )

                # Update via API
                update_url = f"{RFP_API_BASE}/rfp/questions/{question_id}"
                payload = {
                    "answertext": answer,
                    "sources_referenced": sources,
                    "confidence": confidence,
                    "is_sensitive": response_data.get("is_sensitive", False)
                }

                update_response = requests.patch(update_url, json=payload, headers=api_headers, timeout=15)
                update_response.raise_for_status()

                logging.info(f"Successfully regenerated answer for question {question_id}")
                return True

        except Exception as e:
            logging.warning(f"Regeneration attempt {attempt + 1} failed for question {question_id}: {e}")
            if attempt < MAX_REGENERATION_ATTEMPTS - 1:
                import time
                time.sleep(3)

    logging.error(f"Failed to regenerate answer for question {question_id} after {MAX_REGENERATION_ATTEMPTS} attempts")
    return False

def _reformat_answer(question_id, question_data, headers, api_headers):
    """Reformat an answer to fix structural issues while preserving content"""
    original_answer = question_data.get("answertext", "").strip()

    if not original_answer:
        return False

    # Use AI to reformat the answer
    prompt = f"""
You are a professional document formatter specializing in RFP responses.

Your task is to reformat the answer below to fix any structural or formatting issues while preserving ALL original content and meaning.

**Common issues to fix:**
- Malformed JSON or incomplete formatting
- Repeated text blocks (deduplicate)
- Poor paragraph structure
- Missing or excessive whitespace
- Broken markdown syntax

**Rules:**
1. Preserve ALL factual content - do not remove or change any information
2. Fix structural issues only
3. Use proper markdown formatting
4. Ensure professional presentation
5. Return ONLY the reformatted answer text (no JSON, no commentary)

Original Answer:
{original_answer}

Reformatted Answer:
"""

    try:
        reformatted = get_ai_response(prompt, headers=headers, model=QUALITY_AUDIT_MODEL)

        if not reformatted or len(reformatted) < MIN_ANSWER_LENGTH:
            logging.warning(f"Reformatted answer too short for question {question_id}")
            return False

        # Update via API
        update_url = f"{RFP_API_BASE}/rfp/questions/{question_id}"
        payload = {"answertext": reformatted}

        response = requests.patch(update_url, json=payload, headers=api_headers, timeout=10)
        response.raise_for_status()

        logging.info(f"Successfully reformatted answer for question {question_id}")
        return True

    except Exception as e:
        logging.error(f"Failed to reformat answer for question {question_id}: {e}")
        return False

# =============================================================================
# Task 10: Update Project with Audit Results
# =============================================================================
def update_project_with_audit(**context):
    """
    Update project with audit results and remediation metadata.

    IMPORTANT: This function ONLY updates audit metadata fields:
      - quality_audit_run_id
      - quality_audit_status (passed/failed)
      - quality_audit_summary (JSON with detailed metrics)

    It does NOT modify the project's main 'status' field, which remains 'review'.
    """
    conf, project_id, workspace_uuid, x_ltai_user_email, headers, api_headers = _get_conf_and_headers(context)
    quality_report = context["ti"].xcom_pull(task_ids="aggregate_quality_results", key="quality_report")
    dag_run_id = context["dag_run"].run_id

    # Pull remediation results with defensive checks
    duplicate_remediation = context["ti"].xcom_pull(task_ids="remediate_duplicate_questions") or {}
    answer_remediation = context["ti"].xcom_pull(task_ids="remediate_low_quality_answers") or {}

    # Ensure they are dicts
    if not isinstance(duplicate_remediation, dict):
        duplicate_remediation = {}
    if not isinstance(answer_remediation, dict):
        answer_remediation = {}
    if not isinstance(quality_report, dict):
        logging.error("Quality report is not a dict, using defaults")
        quality_report = {
            "overall_status": "failed",
            "total_issues": 0,
            "duplicate_questions": 0,
            "completeness_issues": 0,
            "formatting_issues": 0,
            "reference_issues": 0,
            "severity_breakdown": {},
            "confidence_distribution": {},
            "low_confidence_percentage": 0
        }

    # Calculate final status after remediation
    removed_duplicates = duplicate_remediation.get("removed_count", 0)
    regenerated_answers = answer_remediation.get("regenerated_count", 0)
    reformatted_answers = answer_remediation.get("reformatted_count", 0)

    total_remediations = removed_duplicates + regenerated_answers + reformatted_answers
    remediation_enabled = not duplicate_remediation.get("skipped", False)

    # Prepare payload
    payload = {
        "quality_audit_run_id": dag_run_id,
        "quality_audit_status": quality_report.get("overall_status", "unknown"),
        "quality_audit_summary": json.dumps({
            "original_issues": {
                "total_issues": quality_report.get("total_issues", 0),
                "duplicate_questions": quality_report.get("duplicate_questions", 0),
                "completeness_issues": quality_report.get("completeness_issues", 0),
                "formatting_issues": quality_report.get("formatting_issues", 0),
                "reference_issues": quality_report.get("reference_issues", 0),
                "severity_breakdown": quality_report.get("severity_breakdown", {}),
                "confidence_distribution": quality_report.get("confidence_distribution", {}),
                "low_confidence_percentage": quality_report.get("low_confidence_percentage", 0),
            },
            "remediation": {
                "enabled": remediation_enabled,
                "removed_duplicates": removed_duplicates,
                "regenerated_answers": regenerated_answers,
                "reformatted_answers": reformatted_answers,
                "total_remediations": total_remediations
            },
            "audit_timestamp": datetime.utcnow().isoformat()
        })
    }

    url = f"{RFP_API_BASE}/rfp/projects/{project_id}"

    try:
        response = requests.patch(url, json=payload, headers=api_headers, timeout=30)
        response.raise_for_status()
        logging.info(
            f"Successfully updated project {project_id} with audit results. "
            f"Remediations: {total_remediations} ({removed_duplicates} duplicates removed, "
            f"{regenerated_answers} answers regenerated, {reformatted_answers} reformatted)"
        )
    except Exception as e:
        logging.error(f"Failed to update project with audit results: {e}")
        # Don't raise - audit failure shouldn't block the project

    return {
        "project_id": project_id,
        "audit_status": quality_report["overall_status"],
        "total_issues": quality_report["total_issues"],
        "total_remediations": total_remediations
    }

# =============================================================================
# DAG Definition
# =============================================================================
dag = DAG(
    dag_id="rfp_quality_audit_dag",
    default_args=DEFAULT_ARGS,
    description="Background quality audit for RFP question-answer pairs",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lowtouch", "rfp", "quality-assurance", "audit"],
    max_active_runs=5,
    on_failure_callback=handle_task_failure,
    params={
        "project_id": Param(
            type="integer",
            minimum=1,
            title="Project ID",
            description="RFP project ID to audit",
        ),
        "workspace_uuid": Param(
            type="string",
            title="Workspace UUID",
            description="The UUID of the workspace",
        ),
        "x-ltai-user-email": Param(
            type="string",
            title="User Email",
            description="Email of the user initiating the audit",
        ),
    },
    render_template_as_native_obj=True,
)

with dag:
    # Task 1: Fetch project and Q&A data
    fetch_data = PythonOperator(
        task_id="fetch_project_data",
        python_callable=fetch_project_data
    )

    # Task 2-6: Parallel quality checks
    check_duplicates = PythonOperator(
        task_id="check_duplicate_questions",
        python_callable=check_duplicate_questions
    )

    check_completeness = PythonOperator(
        task_id="check_answer_completeness",
        python_callable=check_answer_completeness
    )

    check_formatting = PythonOperator(
        task_id="check_answer_formatting",
        python_callable=check_answer_formatting
    )

    check_references = PythonOperator(
        task_id="check_reference_validity",
        python_callable=check_reference_validity
    )

    analyze_confidence = PythonOperator(
        task_id="analyze_confidence_distribution",
        python_callable=analyze_confidence_distribution
    )

    # Task 7: Aggregate results
    aggregate_results = PythonOperator(
        task_id="aggregate_quality_results",
        python_callable=aggregate_quality_results,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task 8: Remediate duplicates
    remediate_duplicates = PythonOperator(
        task_id="remediate_duplicate_questions",
        python_callable=remediate_duplicate_questions,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task 9: Remediate low quality answers
    remediate_answers = PythonOperator(
        task_id="remediate_low_quality_answers",
        python_callable=remediate_low_quality_answers,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task 10: Update project
    update_project = PythonOperator(
        task_id="update_project_with_audit",
        python_callable=update_project_with_audit,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task dependencies
    fetch_data >> [check_duplicates, check_completeness, check_formatting, check_references, analyze_confidence]
    [check_duplicates, check_completeness, check_formatting, check_references, analyze_confidence] >> aggregate_results

    # IMPORTANT: Remediation tasks must run SEQUENTIALLY (not parallel)
    # Reason: remediate_answers needs to know which questions were deleted by remediate_duplicates
    aggregate_results >> remediate_duplicates >> remediate_answers >> update_project
