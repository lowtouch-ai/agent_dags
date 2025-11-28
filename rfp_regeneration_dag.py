# dags/rfp_regeneration_dag.py
from datetime import datetime,timedelta
from typing import Dict, Any
import re
import yaml
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from ollama import Client
# -------------------------------
# Configuration
# -------------------------------
OLLAMA_HOST = Variable.get("ltai.v1.rfp.OLLAMA_HOST", default_var="http://agentomatic:8000/")
REGENERATION_AGENT = Variable.get("ltai.v1.rfp.REGENERATION_AGENT", default_var="rfp/regeneration:0.3af")
MAX_IMPROVEMENT_ATTEMPTS = 3
QUALITY_THRESHOLD = 8

# -------------------------------
# Core AI Call Function
# -------------------------------
def get_ai_response(prompt: str, conversation_history: list = None, headers: dict = None) -> str:
    try:
        logging.debug(f"Query received: {prompt}")

        if not prompt or not isinstance(prompt, str):
            raise ValueError("Invalid prompt provided.")

        client = Client(host=OLLAMA_HOST, headers=headers)
        messages = conversation_history[:] if conversation_history else []
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model=REGENERATION_AGENT,
            messages=messages,
            stream=False,
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        if 'message' not in response or 'content' not in response['message']:
            raise ValueError("Invalid response format from AI.")

        content = response['message']['content'].strip()
        if not content:
            raise ValueError("AI returned empty content.")

        return content

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        raise AirflowException(f"AI call failed: {str(e)}")


def extract_yaml_from_response(text: str) -> str:
    """
    Extract a YAML block from the model response.

    Handles:
    - ```yaml ... ``` or ``` ... ``` fenced blocks
    - Plain text starting from 'regenerated:' (no fences)
    """
    if not text:
        return ""

    # 1) If there's a fenced code block, prefer that
    code_block_match = re.search(
        r"```(?:yaml|yml)?\s*([\s\S]*?)```",
        text,
        flags=re.IGNORECASE,
    )
    if code_block_match:
        return code_block_match.group(1).strip()

    # 2) Otherwise, try to start from the 'regenerated:' line
    regenerated_match = re.search(
        r"(?mi)^regenerated\s*:\s*.*$",
        text,
    )
    if regenerated_match:
        candidate = text[regenerated_match.start():]
        return candidate.strip()

    # 3) Fallback: just strip the whole thing
    return text.strip()


# -------------------------------
# Combined Task: Generate with Quality Loop
# -------------------------------
def generate_with_quality_loop(**context) -> Dict[str, Any]:
    """Generate and iteratively improve the regenerated answer until quality threshold is met."""
    ti = context["ti"]
    params = context["params"]

    question = params["question"]
    current_answer = params["current_answer"]
    portion = params.get("portion_to_regenerate", "")
    user_instruction = params["user_instruction"]
    workspace_uuid = params["workspace_uuid"]
    headers = {'WORKSPACE_UUID': workspace_uuid}

    # Initial generation
    initial_prompt = f"""You are an expert RFP response writer.

        Original Question:
        {question}

        Current Answer:
        {current_answer}

        {"Portion to update: " + portion if portion else ""}

        User Instruction: {user_instruction}

        Please regenerate the *entire* answer. If a portion is specified, revise and enhance that portion but still return the fully rewritten answer with the improvements incorporated throughout.

        Return ONLY the fully regenerated answer — no explanations, no markdown, no YAML.
        
    """

    regenerated_text = get_ai_response(initial_prompt, headers=headers)
    conversation_history = [
        {"role": "user", "content": initial_prompt},
        {"role": "assistant", "content": regenerated_text}
    ]

    # Quality improvement loop
    attempt = 0
    rating = 0
    rating_justification = ""

    while attempt < MAX_IMPROVEMENT_ATTEMPTS:
        attempt += 1
        logging.info(f"Quality check attempt {attempt}/{MAX_IMPROVEMENT_ATTEMPTS}")

        # Validate and rate
        validation_prompt = f"""Rate the following regenerated RFP answer on a scale of 1–10 based on:
- Clarity & professionalism
- Accuracy to the original question
- Adherence to user instruction
- Conciseness and impact

Regenerated answer:
\"\"\"{regenerated_text}\"\"\"

Respond with a short justification and then a final rating like: RATING: 9/10"""

        rating_text = get_ai_response(validation_prompt, conversation_history=conversation_history,headers=headers)
        
        # Extract numeric rating
        try:
            rating_line = [line for line in rating_text.lower().splitlines() if "rating:" in line][-1]
            rating = int(rating_line.split("/")[0].split(":")[-1].strip())
        except Exception as e:
            logging.warning(f"Could not parse rating: {e}")
            rating = 0

        rating_justification = rating_text
        logging.info(f"Attempt {attempt}: Rating = {rating}/10")

        # Check if quality threshold met
        if rating >= QUALITY_THRESHOLD:
            logging.info(f"Quality threshold met ({rating}>={QUALITY_THRESHOLD})")
            break

        # If not last attempt, try to improve
        if attempt < MAX_IMPROVEMENT_ATTEMPTS:
            logging.info("Quality below threshold, attempting improvement...")
            
            improvement_prompt = f"""The previous regeneration was rated {rating}/10. Here is the feedback:

{rating_justification}

Original task:
Question: {question}
Current Answer: {current_answer}
{"Portion: " + portion if portion else ""}
User Instruction: {user_instruction}

Please regenerate again with significant improvements based on the feedback above.
Return ONLY the clean regenerated text."""

            regenerated_text = get_ai_response(improvement_prompt, headers=headers)
            
            # Update conversation history
            conversation_history.append({"role": "user", "content": validation_prompt})
            conversation_history.append({"role": "assistant", "content": rating_text})
            conversation_history.append({"role": "user", "content": improvement_prompt})
            conversation_history.append({"role": "assistant", "content": regenerated_text})

    # Store results in XCom
    ti.xcom_push(key="final_regenerated_text", value=regenerated_text)
    ti.xcom_push(key="final_rating", value=rating)
    ti.xcom_push(key="rating_justification", value=rating_justification)
    ti.xcom_push(key="improvement_attempts", value=attempt)

    return {
        "text": regenerated_text,
        "rating": rating,
        "attempts": attempt
    }


# -------------------------------
# Task: Format Final Response as YAML
# -------------------------------
def format_final_response(**context) -> Dict[str, Any]:
    ti = context["ti"]
    params = context["params"]
    workspace_uuid = params["workspace_uuid"]
    headers = {'WORKSPACE_UUID': workspace_uuid}
    final_text: str = ti.xcom_pull(key="final_regenerated_text", task_ids="generate_and_improve")
    rating = ti.xcom_pull(key="final_rating", task_ids="generate_and_improve")
    attempts = ti.xcom_pull(key="improvement_attempts", task_ids="generate_and_improve")

    format_prompt = f"""Take the following regenerated RFP answer and return it in strict YAML format:

Regenerated text:
\"\"\"{final_text}\"\"\"

Also determine if the content appears to contain any sensitive information (PII, pricing, client names, internal strategy, etc.).

Return exactly this YAML structure and nothing else:

regenerated: |
  [exact regenerated text, preserved formatting]
is_sensitive: true/false

No additional commentary."""

    raw_yaml = get_ai_response(format_prompt,headers=headers)
    logging.info(f"Raw YAML from model:\n{raw_yaml}")
    # Fallback parsing in case model deviates
    cleaned_yaml_text = extract_yaml_from_response(raw_yaml)
    logging.info(f"Cleaned YAML candidate:\n{cleaned_yaml_text}")
    try:
        parsed = yaml.safe_load(cleaned_yaml_text)
        if not isinstance(parsed, dict) or "regenerated" not in parsed:
            raise ValueError("Invalid YAML structure")
    except Exception as e:
        logging.warning(f"Model failed to return clean YAML: {e}. Forcing correction.")
        # Hard enforcement fallback
        cleaned_text = final_text.strip()
        contains_sensitive = any(kw in cleaned_text.lower() for kw in 
            ["price", "$", "confidential", "pii", "ssn", "client name", "proprietary"])
        parsed = {
            "regenerated": cleaned_text,
            "is_sensitive": contains_sensitive
        }
        raw_yaml = yaml.dump(parsed, sort_keys=False)

    ti.xcom_push(key="final_yaml_output", value=raw_yaml)
    ti.xcom_push(key="final_parsed", value=parsed)

    logging.info(f"Final regeneration complete. Rating: {rating}/10, Attempts: {attempts}")
    return parsed


# -------------------------------
# DAG Definition
# -------------------------------
with DAG(
    dag_id="rfp_regeneration_workflow",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args = {
    "owner": "ai-team",
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
    },
    tags=["ai", "rfp", "regeneration", "lowtouch"],
    doc_md="""
    # RFP Regeneration Workflow
    
    This DAG regenerates RFP answers using AI with built-in quality control.
    
    **Flow:**
    1. Generate initial answer
    2. Validate and rate (loop up to 3 times if quality < 8/10)
    3. Format as YAML with sensitivity detection
    
    **Parameters:**
    - question: The RFP question
    - current_answer: The existing answer to improve
    - portion_to_regenerate: (Optional) Specific portion to focus on
    - user_instruction: Specific guidance for regeneration
    - workspace_uuid: Workspace identifier
    """,
    params={
        "question": "",
        "current_answer": "",
        "portion_to_regenerate": "",
        "user_instruction": "",
        "workspace_uuid": ""
    },
) as dag:

    generate_and_improve = PythonOperator(
        task_id="generate_and_improve",
        python_callable=generate_with_quality_loop,
        provide_context=True,
    )

    finalize = PythonOperator(
        task_id="format_final_response",
        python_callable=format_final_response,
        provide_context=True,
    )

    # Simple linear flow
    generate_and_improve >> finalize