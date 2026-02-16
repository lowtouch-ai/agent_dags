"""RFP Processing DAG for Corporate Pension documents."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_corporate_pension_processing_dag",
    description="Processes Corporate Pension RFPs: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "corporate-pension", "processing"],
)
