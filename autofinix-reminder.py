from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from ollama import Client
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# API Endpoint and Credentials
AUTOLOAN_API_URL = Variable.get("AUTOLOAN_API_URL")
AGENTOMATIC_API_URL = Variable.get("AGENTOMATIC_API_URL")
TEST_PHONE_NUMBER = Variable.get("TEST_PHONE_NUMBER")

# Ensure API URL is set
if not AUTOLOAN_API_URL:
    raise ValueError("Autoloan API URL is missing. Set it in Airflow Variables.")

# Define DAG
with DAG(
    "autoloan_reminder",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered or via scheduler
    catchup=False,
) as dag:

    def fetch_due_loans(**kwargs):
        """Fetches loans that are due from the Autoloan API and retrieves the associated phone number."""
        try:
            response = requests.get(f"{AUTOLOAN_API_URL}/loan/overdue")
            if response.status_code == 200:
                loans = response.json()
                logger.info(f"Fetched {len(loans)} due loans.")

                if loans:
                    customer_id = loans[0]["customer_id"]
                    customer_response = requests.get(f"{AUTOLOAN_API_URL}/customer/{customer_id}")

                    if customer_response.status_code == 200:
                        customer_data = customer_response.json()
                        loans[0]["phone"] = TEST_PHONE_NUMBER  # Using test phone number
                    else:
                        raise Exception(f"Failed to fetch customer details for ID {customer_id}")

                kwargs['ti'].xcom_push(key='due_loans', value=[loans[0]])  # Ensure it's a list
            else:
                logger.error(f"Failed to fetch due loans from API:{response.text}")
                raise Exception("Failed to fetch due loans from API")
        except Exception as e:
            logger.error(f"Failed to fetch due loans: {e}")

    def generate_message_using_agent(loan):
        """Generates voice message content for each loan."""
        try:
            client = Client(
                host=AGENTOMATIC_API_URL,
                headers={'x-ltai-client': 'autofinix-loan-reminder'}
            )

            response = client.chat(
                model='autofinix:0.3',
                messages=[{"role": "user", "content": f'Generate a voice message for the loan due reminder for the loan:{loan}'}],
                stream=False
            )

            agent_response = response['message']['content']
            logging.info(f"Agent Response: {agent_response}")
            return agent_response
        except Exception as e:
            logging.error(f"Failed to generate message using agent: {e}")
            return f"Hello, this is a reminder that your loan is due. Please contact us for more information."
    def generate_voice_message(**kwargs):
        """Generates voice message content for each loan."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')

        messages = [
            {
                "phone_number": loan["phone"],
                "message": generate_message_using_agent(loan)
            }
            for loan in loans
        ]

        ti.xcom_push(key='voice_messages', value=messages)

    def update_reminder_status(**kwargs):
        """Marks the reminder as scheduled in the Autoloan API."""
        ti = kwargs['ti']
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')

        for loan in loans:
            # Uncomment below to send request to API
            # response = requests.post(
            #     f"{AUTOLOAN_API_URL}/update-reminder-status", json={"loan_id": loan['id'], "status": "scheduled"}
            # )
            # if response.status_code == 200:
            #     logger.info(f"Updated reminder status for Loan ID: {loan['id']}")
            # else:
            #     logger.error(f"Failed to update reminder status for Loan ID: {loan['id']}")
            logger.info(f"Updated reminder status for Loan ID: {loan['id']}")
            return 'status'

    def update_call_status(**kwargs):
        """Updates the call status in the Autoloan API."""
        ti = kwargs['ti']
        call_status = ti.xcom_pull(task_ids='wait_for_call_completion', key='call_status')
        loans = ti.xcom_pull(task_ids='fetch_due_loans', key='due_loans')

        # for loan, status in zip(loans, call_status):
        #     update_response = requests.post(
        #         f"{AUTOLOAN_API_URL}/update-call-status",
        #         json={"loan_id": loan['id'], "status": status},
        #     )
        #     if update_response.status_code == 200:
        #         logger.info(f"Updated call status for Loan ID: {loan['id']} to {status}")
        #     else:
        #         logger.error(f"Failed to update call status for Loan ID: {loan['id']}")
        logger.info(f"Updated call status for Loan ID: {loan['id']} to {status}")
        return 'status'

    # Tasks
    fetch_due_loans_task = PythonOperator(
        task_id="fetch_due_loans",
        python_callable=fetch_due_loans,
        provide_context=True,
    )

    generate_voice_message_task = PythonOperator(
        task_id="generate_voice_message",
        python_callable=generate_voice_message,
        provide_context=True,
    )

    trigger_send_voice_message = TriggerDagRunOperator(
        task_id="trigger_send_voice_message",
        trigger_dag_id="twilio_voice_call_direct",
        conf={"params": "{{ ti.xcom_pull(task_ids='generate_voice_message', key='voice_messages') }}"},
        wait_for_completion=False,
    )

    update_reminder_status_task = PythonOperator(
        task_id="update_reminder_status",
        python_callable=update_reminder_status,
        provide_context=True,
    )

    wait_for_call_completion = ExternalTaskSensor(
        task_id="wait_for_call_completion",
        external_dag_id="twilio_voice_call_direct",
        external_task_id="fetch_and_save_recording",
        mode="poke",
        timeout=600,
        poke_interval=10,
    )

    update_call_status_task = PythonOperator(
        task_id="update_call_status",
        python_callable=update_call_status,
        provide_context=True,
    )

    # Task Dependencies
    fetch_due_loans_task >> generate_voice_message_task
    generate_voice_message_task >> trigger_send_voice_message >> update_reminder_status_task
    update_reminder_status_task >> wait_for_call_completion >> update_call_status_task
