"""RFP Processing DAG for Endowment and Foundation documents."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_endowment_foundation_processing_dag",
    description="Processes Endowment and Foundation RFPs: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "endowment-foundation", "processing"],
)
