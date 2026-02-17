"""RFP Processing DAG for Gatekeeper Review Questionnaires."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_gatekeeper_review_processing_dag",
    description="Processes Gatekeeper Review Questionnaires: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "gatekeeper-review", "processing"],
)
