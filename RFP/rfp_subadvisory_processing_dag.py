"""RFP Processing DAG for Sub-Advisory documents."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_subadvisory_processing_dag",
    description="Processes Sub-Advisory RFPs: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "subadvisory", "processing"],
)
