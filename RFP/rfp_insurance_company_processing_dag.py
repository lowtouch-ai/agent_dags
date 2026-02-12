"""RFP Processing DAG for Insurance Company documents."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_insurance_company_processing_dag",
    description="Processes Insurance Company RFPs: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "insurance-company", "processing"],
)
