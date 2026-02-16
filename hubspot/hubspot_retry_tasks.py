from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun, TaskInstance, XCom, Variable
from airflow.utils.state import State
from airflow.api.common.trigger_dag import trigger_dag
from airflow import settings

from datetime import datetime, timedelta
import logging
import pytz
import json

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(minutes=5),
    "retries": 2,
}


@task
def check_and_retry_failed_tasks(**context):
    """
    Check for ALL failed DAG runs and trigger retries.
    UPDATED: Only retry 3 times with 20-minute intervals between attempts
    """
    session = settings.Session()

    try:
        monitored_dags = [
            "hubspot_monitor_mailbox",
            "hubspot_search_entities", 
            "hubspot_create_objects",
            "hubspot_task_completion_handler"
        ]
        
        # Get shared retry tracker
        retry_tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
        
        logger.info("="*60)
        logger.info("RETRY TRACKER STATUS")
        logger.info("="*60)
        logger.info(f"Total tracked failures: {len(retry_tracker)}")
        
        if retry_tracker:
            logger.info("Tracked keys:")
            for key in retry_tracker.keys():
                logger.info(f"  - {key}")
        else:
            logger.warning("❌ RETRY TRACKER IS EMPTY!")
        logger.info("="*60)
        
        retry_summary = {
            "check_timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
            "dags_checked": {},
            "total_retries_triggered": 0,
            "total_skipped": 0,
            "total_tracked_failures": len(retry_tracker),
            "retry_tracker_keys": list(retry_tracker.keys()),
            "max_retries_exceeded": 0  # NEW: Track permanently failed
        }

        for target_dag_id in monitored_dags:
            logger.info(f"\n=== Checking DAG: {target_dag_id} ===")
            
            # Find ALL failed dag runs
            failed_runs = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == target_dag_id,
                    DagRun.state == State.FAILED
                )
                .order_by(DagRun.execution_date.desc())
                .limit(50)
                .all()
            )

            if not failed_runs:
                logger.info(f"No failed runs found for {target_dag_id}")
                retry_summary["dags_checked"][target_dag_id] = {
                    "failed_runs": 0,
                    "retries": 0,
                    "skipped": 0,
                    "max_retries_exceeded": 0
                }
                continue

            logger.info(f"Found {len(failed_runs)} failed runs for {target_dag_id}")

            retry_count = 0
            skip_count = 0
            tracked_count = 0
            untracked_count = 0
            max_retries_count = 0

            for dag_run in failed_runs:
                run_id = dag_run.run_id
                tracker_key = f"{target_dag_id}:{run_id}"
                
                logger.info(f"\nProcessing run: {run_id}")
                logger.info(f"  Tracker key: {tracker_key}")
                logger.info(f"  Run state: {dag_run.state}")
                logger.info(f"  Execution date: {dag_run.execution_date}")
                
                entry = retry_tracker.get(tracker_key)
                
                if entry is None:
                    untracked_count += 1
                    logger.warning(f"  ⚠️ NOT IN TRACKER - This failure was never tracked!")
                    logger.warning(f"     This might be an old failure or tracking failed")
                    logger.warning(f"     Conf: {dag_run.conf}")
                    continue
                
                logger.info(f"  ✓ Found in tracker")
                logger.info(f"     Status: {entry.get('status')}")
                logger.info(f"     Thread ID: {entry.get('thread_id')}")
                logger.info(f"     Failed Task: {entry.get('failed_task_id')}")
                logger.info(f"     Retry Count: {entry.get('retry_count', 0)}/3")
                logger.info(f"     Has email_data: {bool(entry.get('email_data'))}")
                
                tracked_count += 1
                
                # NEW: Check if max retries reached
                if entry.get("max_retries_reached", False) or entry.get("retry_count", 0) >= 3:
                    logger.warning(f"  ⚠️ MAX RETRIES EXCEEDED - No more retry attempts")
                    max_retries_count += 1
                    
                    # Update status to permanent failure
                    if entry.get("status") != "max_retries_exceeded":
                        entry["status"] = "max_retries_exceeded"
                        entry["max_retries_reached"] = True
                        retry_tracker[tracker_key] = entry
                        Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
                    continue
                
                if entry.get("status") == "success":
                    logger.info(f"  ✓ Already successfully retried - skipping")
                    continue

                # NEW: Check 20-minute cooling period
                last_trigger_time = entry.get("last_trigger_time")
                if last_trigger_time:
                    try:
                        last_trigger_dt = datetime.fromisoformat(last_trigger_time.replace('Z', '+00:00'))
                        time_since_last_trigger = datetime.now(pytz.utc) - last_trigger_dt
                        
                        # 20-minute cooldown
                        if time_since_last_trigger < timedelta(minutes=20):
                            remaining_minutes = 20 - (time_since_last_trigger.total_seconds() / 60)
                            logger.info(f"  ⏳ COOLING DOWN - {remaining_minutes:.1f} minutes remaining until next retry")
                            skip_count += 1
                            continue
                        else:
                            logger.info(f"  ✓ Cooling period passed ({time_since_last_trigger.total_seconds() / 60:.1f} minutes)")
                    except Exception as e:
                        logger.warning(f"  Could not parse last_trigger_time: {e}")

                # Check for existing retry attempts
                existing_retries = (
                    session.query(DagRun)
                    .filter(
                        DagRun.dag_id == target_dag_id,
                        DagRun.run_id.like(f"retry_{run_id}%")
                    )
                    .order_by(DagRun.execution_date.desc())
                    .all()
                )

                if existing_retries:
                    latest_retry = existing_retries[0]
                    logger.info(f"  Found {len(existing_retries)} previous retry attempts")
                    logger.info(f"    Latest: {latest_retry.run_id} - State: {latest_retry.state}")
                    
                    if latest_retry.state in [State.RUNNING, State.QUEUED]:
                        logger.info(f"  ⏳ SKIP: Retry in progress")
                        skip_count += 1
                        continue
                    
                    if latest_retry.state == State.SUCCESS:
                        if tracker_key in retry_tracker:
                            retry_tracker[tracker_key]["status"] = "success"
                            retry_tracker[tracker_key]["resolved_at"] = datetime.now(pytz.utc).isoformat()
                            Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
                        logger.info(f"  ✓ Latest retry succeeded - marked as resolved in tracker")
                        continue
                    
                    logger.info(f"  ⚠️ Previous retry failed - checking if we can retry again")

                # NEW: Calculate current retry attempt number
                current_retry_count = entry.get("retry_count", 0)
                next_retry_count = current_retry_count + 1
                
                if next_retry_count > 3:
                    logger.warning(f"  ❌ Cannot retry - would exceed max attempts (current: {current_retry_count}, max: 3)")
                    entry["max_retries_reached"] = True
                    entry["status"] = "max_retries_exceeded"
                    retry_tracker[tracker_key] = entry
                    Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
                    max_retries_count += 1
                    continue

                # Build retry conf
                original_conf = dag_run.conf or {}
                
                retry_conf = {
                    "retry_attempt": True,
                    "original_run_id": run_id,
                    "original_dag_id": target_dag_id,
                    "original_task_id": entry.get("failed_task_id", "unknown"),
                    "thread_id": entry.get("thread_id"),
                    "fallback_email_already_sent": entry.get("fallback_sent", False),
                    "retry_timestamp": datetime.now(pytz.utc).isoformat(),
                    "retry_attempt_number": next_retry_count  # NEW: Track which attempt this is
                }
                
                # Add email_data from tracker
                if "email_data" in entry:
                    retry_conf["email_data"] = entry["email_data"]
                    logger.info(f"  ✓ Retrieved email_data from tracker")
                elif "email_data" in original_conf:
                    retry_conf["email_data"] = original_conf["email_data"]
                    logger.info(f"  ✓ Retrieved email_data from original conf")
                else:
                    logger.warning(f"  ⚠️ No email_data available - retry may fail")
                
                # Also include other original conf data
                for key, value in original_conf.items():
                    if key not in retry_conf:
                        retry_conf[key] = value
                
                # Generate unique run_id
                timestamp = datetime.now(pytz.utc).strftime("%Y%m%d_%H%M%S")
                retry_run_id = f"retry_{run_id}_{timestamp}"

                try:
                    trigger_dag(
                        dag_id=target_dag_id,
                        run_id=retry_run_id,
                        conf=retry_conf,
                        execution_date=datetime.now(pytz.utc)
                    )
                    logger.info(f"  ✓ TRIGGERED RETRY {next_retry_count}/3: {retry_run_id}")
                    retry_count += 1
                    retry_summary["total_retries_triggered"] += 1

                    # Update tracker with new retry count and timestamp
                    entry["status"] = "retrying"
                    entry["last_retry_run_id"] = retry_run_id
                    entry["last_trigger_time"] = datetime.now(pytz.utc).isoformat()
                    entry["retry_count"] = next_retry_count  # NEW: Increment count
                    retry_tracker[tracker_key] = entry
                    Variable.set("hubspot_retry_tracker", json.dumps(retry_tracker))
                    
                except Exception as trigger_error:
                    logger.error(f"  ❌ Failed to trigger retry: {trigger_error}")

            retry_summary["dags_checked"][target_dag_id] = {
                "failed_runs_scanned": len(failed_runs),
                "tracked_failures": tracked_count,
                "untracked_failures": untracked_count,
                "retries_triggered": retry_count,
                "skipped_pending": skip_count,
                "max_retries_exceeded": max_retries_count  # NEW
            }
            retry_summary["max_retries_exceeded"] += max_retries_count

        logger.info("\n" + "="*60)
        logger.info("RETRY CHECK SUMMARY")
        logger.info("="*60)
        logger.info(f"Total failures in tracker: {len(retry_tracker)}")
        logger.info(f"Total retries triggered: {retry_summary['total_retries_triggered']}")
        logger.info(f"Total skipped (cooling/pending): {retry_summary['total_skipped']}")
        logger.info(f"Total max retries exceeded: {retry_summary['max_retries_exceeded']}")
        logger.info("="*60)

        return retry_summary

    except Exception as e:
        logger.error(f"Critical error in retry check: {e}", exc_info=True)
        return {"error": str(e)}
    
    finally:
        session.close()


@task
def cleanup_successful_retries(**context):
    # (keeping your original cleanup task unchanged - it's still useful for XCom + fallback cleanup)
    session = settings.Session()
    
    try:
        monitored_dags = [
            "hubspot_monitor_mailbox",
            "hubspot_search_entities", 
            "hubspot_create_objects",
            "hubspot_task_completion_handler"
        ]
        
        cleanup_summary = {
            "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
            "retry_trackers_cleaned": 0,
            "fallback_trackers_cleaned": 0,
            "threads_resolved": []
        }
        
        lookback_time = datetime.now(pytz.utc) - timedelta(hours=24)
        
        for dag_id in monitored_dags:
            successful_retries = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id.like('retry_%'),
                    DagRun.state == State.SUCCESS,
                    DagRun.execution_date >= lookback_time
                )
                .all()
            )
            
            if not successful_retries:
                continue
            
            logger.info(f"Found {len(successful_retries)} successful retries for {dag_id}")
            
            for retry_run in successful_retries:
                retry_run_id = retry_run.run_id
                retry_conf = retry_run.conf or {}
                
                original_run_id = retry_conf.get("original_run_id")
                original_dag_id = retry_conf.get("original_dag_id", dag_id)
                thread_id = retry_conf.get("thread_id")
                
                if not original_run_id:
                    continue
                
                logger.info(f"✓ Retry successful: {retry_run_id} → cleaning up")
                
                # Clean retry_triggered XCom (legacy)
                try:
                    deleted_count = (
                        session.query(XCom)
                        .filter(
                            XCom.dag_id == original_dag_id,
                            XCom.run_id == original_run_id,
                            XCom.key == "retry_triggered"
                        )
                        .delete()
                    )
                    if deleted_count > 0:
                        session.commit()
                        cleanup_summary["retry_trackers_cleaned"] += deleted_count
                except Exception as e:
                    logger.warning(f"Failed to clean retry_triggered XCom: {e}")
                    session.rollback()
                
                # Clean fallback tracker if thread_id exists
                if thread_id:
                    try:
                        from sqlalchemy import desc
                        fallback_xcom = (
                            session.query(XCom)
                            .join(DagRun, XCom.dag_run_id == DagRun.id)
                            .filter(
                                XCom.dag_id == original_dag_id,
                                XCom.key == "fallback_sent_threads"
                            )
                            .order_by(desc(DagRun.execution_date))
                            .limit(1)
                            .first()
                        )
                        
                        if fallback_xcom and fallback_xcom.value:
                            tracker = fallback_xcom.value
                            if isinstance(tracker, dict) and thread_id in tracker:
                                del tracker[thread_id]
                                fallback_xcom.value = tracker
                                session.commit()
                                cleanup_summary["fallback_trackers_cleaned"] += 1
                                cleanup_summary["threads_resolved"].append(thread_id)
                    except Exception as e:
                        logger.warning(f"Failed to clean fallback tracker: {e}")
                        session.rollback()
        
        logger.info(f"=== CLEANUP SUMMARY ===")
        logger.info(f"Retry trackers cleaned (XCom): {cleanup_summary['retry_trackers_cleaned']}")
        logger.info(f"Fallback trackers cleaned: {cleanup_summary['fallback_trackers_cleaned']}")
        logger.info(f"Threads resolved: {len(cleanup_summary['threads_resolved'])}")
        
        return cleanup_summary
        
    except Exception as e:
        logger.error(f"Critical error in cleanup: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        session.close()


@task
def log_retry_statistics(**context):
    ti = context['ti']
    retry_results = ti.xcom_pull(task_ids='check_and_retry_failed_tasks')
    cleanup_results = ti.xcom_pull(task_ids='cleanup_successful_retries')
    
    if not retry_results:
        logger.warning("No retry results available")
        return
    
    tracker = Variable.get("hubspot_retry_tracker", default_var={}, deserialize_json=True)
    
    stats = {
        "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
        "total_retries_triggered_this_run": retry_results.get("total_retries_triggered", 0),
        "total_skipped_pending": retry_results.get("total_skipped", 0),
        "currently_tracked_failures": len(tracker),
        "dags_checked": retry_results.get("dags_checked", {}),
        "cleanup": cleanup_results or {}
    }
    
    logger.info(f"RETRY_STATS: {json.dumps(stats, indent=2)}")
    
    if stats["currently_tracked_failures"] > 30:
        logger.warning(f"ALERT: {stats['currently_tracked_failures']} failures still awaiting successful retry")
    
    return stats



with DAG(
    dag_id="hubspot_retry_failed_tasks",
    default_args=default_args,
    description="Auto-retry failed HubSpot DAGs (max 3 attempts, 20-min intervals)",
    schedule=timedelta(minutes=5),  # Check every 5 minutes (will enforce 20-min cooldown internally)
    catchup=False,
    tags=["hubspot", "monitoring", "retry", "critical"],
    max_active_runs=1,
    max_active_tasks=3,
) as dag:

    monitor_and_retry = check_and_retry_failed_tasks()
    cleanup = cleanup_successful_retries()
    stats = log_retry_statistics()

    monitor_and_retry >> cleanup >> stats