"""RFP Processing DAG for Wrap/SMA Program documents."""

from airflow import DAG  # noqa: F401 — required for Airflow DAG discovery
from agent_dags.RFP.rfp_processing_base import create_rfp_processing_dag

dag = create_rfp_processing_dag(
    dag_id="rfp_wrap_sma_processing_dag",
    description="Processes Wrap or SMA Program RFPs: Extracts questions, generates answers, updates via API",
    tags=["lowtouch", "rfp", "wrap-sma", "processing"],
)
