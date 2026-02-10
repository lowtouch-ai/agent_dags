"""RFP Processing DAG for Broker Dealer Platform Questionnaires."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_broker_dealer_platform_processing_dag",
    description="Processes Broker Dealer Platform Questionnaires: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "broker-dealer-platform", "processing"],
)
