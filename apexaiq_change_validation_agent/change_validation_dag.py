"""
Change Validation & Compliance Agent (MVP)
Validates infrastructure device changes against approved ManageEngine
ServiceDesk Plus change controls. Detects unauthorized or mismatched
changes and classifies them for compliance review.

Trigger: Manual only — pass device_changes array via dag_run.conf.

Sample conf:
{
  "device_changes": [
    {
      "device_id": "router-01",
      "asset_id": "300000000000001",
      "change_type": "configuration",
      "change_details": "Modified ACL rules on GigabitEthernet0/1",
      "previous_state": "permit ip 10.0.0.0/8 any",
      "current_state": "permit ip 10.0.0.0/8 any; deny ip 192.168.1.0/24 any",
      "change_timestamp": "2025-12-15T14:30:00Z"
    }
  ]
}

Airflow Variables required:
  - ltai.change_validation.me.base_url       (ManageEngine SDP Cloud base URL)
  - APEXAIQ_ZOHO_CLIENT_ID                   (Zoho OAuth2 client ID)
  - APEXAIQ_ZOHO_CLIENT_SECRET               (Zoho OAuth2 client secret)
"""

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ME_BASE_URL = Variable.get("ltai.change_validation.me.base_url", default="")
ZOHO_CLIENT_ID = Variable.get("APEXAIQ_ZOHO_CLIENT_ID", default="")
ZOHO_CLIENT_SECRET = Variable.get("APEXAIQ_ZOHO_CLIENT_SECRET", default="")

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 12, 16),
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


# ---------------------------------------------------------------------------
# Task 0 — Fetch Zoho OAuth2 access token
# ---------------------------------------------------------------------------
ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"
ZOHO_SCOPES = (
    "SDPOnDemand.requests.ALL,"
    "SDPOnDemand.requests.READ,"
    "SDPOnDemand.assets.READ,"
    "SDPOnDemand.changes.READ,"
    "SDPOnDemand.requests.CREATE"
)


def fetch_zoho_token(**kwargs):
    """Obtain a Zoho OAuth2 access token using client_credentials grant
    and push it to XCom for downstream tasks."""

    if not ZOHO_CLIENT_ID or not ZOHO_CLIENT_SECRET:
        raise ValueError(
            "Zoho credentials missing — set Airflow Variables "
            "APEXAIQ_ZOHO_CLIENT_ID and "
            "APEXAIQ_ZOHO_CLIENT_SECRET"
        )

    params = {
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": ZOHO_SCOPES,
    }

    logging.info("Requesting Zoho OAuth2 token: POST %s", ZOHO_TOKEN_URL)
    resp = requests.post(ZOHO_TOKEN_URL, params=params, timeout=30)
    resp.raise_for_status()
    token_data = resp.json()

    access_token = token_data.get("access_token")
    if not access_token:
        error = token_data.get("error", "unknown")
        raise ValueError(f"Zoho token response missing access_token: {error}")

    expires_in = token_data.get("expires_in", "N/A")
    logging.info("Zoho token acquired (expires_in=%s seconds)", expires_in)

    ti = kwargs["ti"]
    ti.xcom_push(key="access_token", value=access_token)
    return access_token


# ---------------------------------------------------------------------------
# Task 1 — Extract asset IDs from device change input
# ---------------------------------------------------------------------------
def extract_asset_ids(**kwargs):
    """Parse dag_run.conf, validate each device change entry, and push
    deduplicated asset_ids plus the validated change list to XCom."""

    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf or {}

    device_changes = conf.get("device_changes", [])
    if not device_changes:
        raise ValueError("No device_changes provided in DAG conf")

    asset_ids = []
    validated_changes = []

    for change in device_changes:
        asset_id = change.get("asset_id")
        if not asset_id:
            logging.warning(
                "Skipping change entry without asset_id — device_id: %s",
                change.get("device_id", "unknown"),
            )
            continue
        asset_ids.append(str(asset_id))
        validated_changes.append(change)

    if not asset_ids:
        raise ValueError("No valid asset_ids found in device_changes")

    unique_asset_ids = list(set(asset_ids))
    logging.info("Extracted %d unique asset IDs: %s", len(unique_asset_ids), unique_asset_ids)

    ti = kwargs["ti"]
    ti.xcom_push(key="asset_ids", value=unique_asset_ids)
    ti.xcom_push(key="device_changes", value=validated_changes)
    return unique_asset_ids


# ---------------------------------------------------------------------------
# Task 2 — Fetch change requests from ManageEngine ServiceDesk Plus Cloud
# ---------------------------------------------------------------------------
def fetch_change_requests(**kwargs):
    """For each asset_id, query ManageEngine SDP Cloud v3 Changes API and
    collect all associated change requests.  Results keyed by asset_id via XCom."""

    ti = kwargs["ti"]
    asset_ids = ti.xcom_pull(task_ids="extract_asset_ids", key="asset_ids")
    if not asset_ids:
        raise ValueError("No asset_ids received from extract_asset_ids")

    base_url = ME_BASE_URL.rstrip("/")
    if not base_url:
        raise ValueError(
            "ManageEngine base URL missing — set Airflow Variable "
            "ltai.change_validation.me.base_url"
        )

    access_token = ti.xcom_pull(task_ids="fetch_zoho_token", key="access_token")
    if not access_token:
        raise ValueError("No access_token received from fetch_zoho_token task")

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/vnd.manageengine.sdp.v3+json",
    }

    # Step 1 — Fetch all change IDs from the list endpoint
    list_input = {
        "list_info": {
            "row_count": 100,
            "sort_field": "created_time",
            "sort_order": "desc",
            "get_total_count": True,
        }
    }

    list_url = f"{base_url}/api/v3/changes"
    params = {"input_data": json.dumps(list_input)}

    logging.info("Requesting changes list: GET %s", list_url)
    logging.info("Request params: %s", json.dumps(params))

    try:
        resp = requests.get(list_url, headers=headers, params=params, timeout=30)
        logging.info("List changes response: status=%s", str(resp.status_code))
        resp.raise_for_status()
        data = resp.json()
        change_summaries = data.get("changes", [])
        total_count = data.get("list_info", {}).get("total_count", "N/A")
        logging.info(
            "Fetched %d change(s) from ManageEngine (total_count=%s)", len(change_summaries), str(total_count)
        )
        for cs in change_summaries:
            cs_display = cs.get("display_id", {}).get("display_value", "?")
            cs_title = cs.get("title", "N/A")
            logging.info("  Change %s — %s (id=%s)", cs_display, cs_title, str(cs.get("id")))
    except requests.exceptions.RequestException as exc:
        logging.error("Failed to list changes: %s", str(exc))
        change_summaries = []

    # Step 2 — For each change, fetch full details to get the assets array
    asset_id_set = set(asset_ids)
    logging.info("Looking for asset IDs: %s", str(list(asset_id_set)))
    all_changes: dict[str, list] = {aid: [] for aid in asset_ids}

    for idx, change_summary in enumerate(change_summaries, 1):
        change_id = change_summary.get("id")
        if not change_id:
            continue

        cs_display = change_summary.get("display_id", {}).get("display_value", str(change_id))
        detail_url = f"{base_url}/api/v3/changes/{change_id}"
        logging.info("[%d/%d] Fetching details for %s: GET %s", idx, len(change_summaries), cs_display, detail_url)

        try:
            detail_resp = requests.get(detail_url, headers=headers, timeout=30)
            logging.info("  Response status: %s", str(detail_resp.status_code))
            detail_resp.raise_for_status()
            detail_data = detail_resp.json()
            change_detail = detail_data.get("change", {})
        except requests.exceptions.RequestException as exc:
            logging.error("  Failed to fetch details for change %s: %s", cs_display, str(exc))
            continue

        # Resolve approval status via approval_levels API
        resolved_approval = _fetch_approval_status(base_url, change_id, headers)
        change_detail["_resolved_approval"] = resolved_approval
        logging.info("  Resolved approval for %s: %s", cs_display,
                     resolved_approval.get("status") if resolved_approval else "N/A")

        # Log key fields from the change detail
        logging.info("  Title: %s", change_detail.get("title", "N/A"))
        logging.info("  Status: %s", _safe_name(change_detail.get("status")))
        logging.info("  Approval: %s", _safe_name(change_detail.get("approval_status")))

        sched_start = change_detail.get("scheduled_start_time")
        sched_end = change_detail.get("scheduled_end_time")
        logging.info("  Scheduled: %s -> %s",
            sched_start.get("display_value", "N/A") if isinstance(sched_start, dict) else "N/A",
            sched_end.get("display_value", "N/A") if isinstance(sched_end, dict) else "N/A",
        )

        # Log linked assets
        linked_assets = change_detail.get("assets", []) or []
        asset_summary = json.dumps([{"id": a.get("id"), "name": a.get("name")} for a in linked_assets]) if linked_assets else "None"
        logging.info("  Linked assets (%d): %s", len(linked_assets), asset_summary)

        # Log linked CIs
        linked_cis = change_detail.get("configuration_items", []) or []
        if linked_cis:
            ci_summary = json.dumps([{"id": c.get("id"), "name": c.get("name")} for c in linked_cis])
            logging.info("  Linked CIs (%d): %s", len(linked_cis), ci_summary)

        # Check if any of the target asset IDs are linked to this change
        for asset in linked_assets:
            linked_id = str(asset.get("id", ""))
            if linked_id in asset_id_set:
                all_changes[linked_id].append(change_detail)
                logging.info("  MATCH: Change %s is linked to target asset %s", cs_display, linked_id)

    logging.info("=== Asset-to-Change matching summary ===")
    for asset_id in asset_ids:
        logging.info(
            "Asset %s — matched %d change request(s)", asset_id, len(all_changes[asset_id])
        )

    ti.xcom_push(key="change_requests", value=all_changes)
    return all_changes


# ---------------------------------------------------------------------------
# Task 3 — Correlate device changes with CRs and classify
# ---------------------------------------------------------------------------
def _check_time_window(change_ts, window_start, window_end):
    """Return True if *change_ts* falls within [window_start, window_end].
    Handles ISO-8601 strings and epoch-millisecond integers from ManageEngine."""
    from dateutil import parser as date_parser

    if not change_ts or not window_start or not window_end:
        return False

    try:
        def _parse(val):
            if isinstance(val, (int, float)):
                return pendulum.from_timestamp(val / 1000)
            return date_parser.parse(str(val))

        return _parse(window_start) <= _parse(change_ts) <= _parse(window_end)
    except Exception as exc:
        logging.warning("Timestamp parse error during window check: %s", exc)
        return False


def correlate_and_classify(**kwargs):
    """Match each device change against ManageEngine CRs and assign one of:
    Validated Change | Unapproved Change | Missing or Partial Approval | Data Inconclusive"""

    ti = kwargs["ti"]
    device_changes = ti.xcom_pull(task_ids="extract_asset_ids", key="device_changes")
    change_requests = ti.xcom_pull(task_ids="fetch_change_requests", key="change_requests")

    results = []

    for device_change in device_changes:
        asset_id = str(device_change.get("asset_id"))
        device_id = device_change.get("device_id", "unknown")
        change_ts = device_change.get("change_timestamp")
        change_details = device_change.get("change_details", "")

        related_crs = change_requests.get(asset_id, [])

        # --- no CRs at all → Unapproved ---
        if not related_crs:
            results.append(_build_result(
                device_change, "Unapproved Change",
                "No change requests found for this asset", None,
            ))
            continue

        # --- attempt to match an approved CR inside the time window ---
        matched_cr = None
        classification = "Unapproved Change"
        reason = "No matching approved change request found"

        for cr in related_crs:
            resolved = cr.get("_resolved_approval")
            if resolved and resolved.get("status") == "approved":
                is_approved = True
                approval_name = "approved (via approval levels)"
            elif resolved and resolved.get("status") == "rejected":
                is_approved = False
                approval_name = "rejected (via approval levels)"
            else:
                approval_name = _safe_name(cr.get("approval_status"))
                is_approved = approval_name.lower() in ("approved", "pre-approved")

            start_val = _safe_ts(cr.get("scheduled_start_time"))
            end_val = _safe_ts(cr.get("scheduled_end_time"))
            in_window = _check_time_window(change_ts, start_val, end_val)

            cr_label = cr.get("display_id") or cr.get("id", "unknown")

            # Build approval detail suffix for reason strings
            level_detail = ""
            if resolved and resolved.get("levels"):
                level_parts = []
                for lvl in resolved["levels"]:
                    lvl_num = lvl.get("level", "?")
                    lvl_status = lvl.get("status", "unknown")
                    level_parts.append(f"L{lvl_num}={lvl_status}")
                level_detail = f" [{', '.join(level_parts)}]"

            if is_approved and in_window:
                matched_cr = cr
                classification = "Validated Change"
                reason = f"Matches approved CR #{cr_label}{level_detail}"
                break

            if is_approved and not in_window:
                matched_cr = cr
                classification = "Missing or Partial Approval"
                reason = (
                    f"Approved CR #{cr_label} exists but change occurred "
                    f"outside the scheduled window{level_detail}"
                )

            if not is_approved and matched_cr is None:
                matched_cr = cr
                classification = "Missing or Partial Approval"
                reason = (
                    f"CR #{cr_label} found but approval status is "
                    f"'{approval_name}'{level_detail}"
                )

        if classification not in ("Validated Change", "Missing or Partial Approval"):
            if related_crs and matched_cr is None:
                classification = "Data Inconclusive"
                reason = "Change requests exist but could not be conclusively matched"

        results.append(_build_result(device_change, classification, reason, matched_cr))

    # --- summary ---
    summary = {
        "total_changes": len(results),
        "validated": sum(1 for r in results if r["classification"] == "Validated Change"),
        "unapproved": sum(1 for r in results if r["classification"] == "Unapproved Change"),
        "partial_approval": sum(
            1 for r in results if r["classification"] == "Missing or Partial Approval"
        ),
        "inconclusive": sum(1 for r in results if r["classification"] == "Data Inconclusive"),
    }

    logging.info("=== Validation Summary ===")
    logging.info(json.dumps(summary, indent=2))
    for r in results:
        logging.info(
            "  [%s] Device: %s | Asset: %s | Change: %s",
            r["classification"], r["device_id"], r["asset_id"],
            r["change_detected"][:80],
        )

    ti.xcom_push(key="validation_results", value=results)
    ti.xcom_push(key="validation_summary", value=summary)
    return {"summary": summary, "results": results}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_name(field):
    """Extract .name from a ManageEngine object-or-string field."""
    if isinstance(field, dict):
        return field.get("name", "")
    return str(field) if field else ""


def _safe_ts(field):
    """Extract .value (epoch ms) from a ManageEngine time field."""
    if isinstance(field, dict):
        return field.get("value")
    return field


def _build_result(device_change, classification, reason, matched_cr):
    return {
        "device_id": device_change.get("device_id", "unknown"),
        "asset_id": str(device_change.get("asset_id")),
        "change_detected": device_change.get("change_details", ""),
        "change_timestamp": device_change.get("change_timestamp"),
        "classification": classification,
        "reason": reason,
        "matching_change_id": (
            matched_cr.get("display_id") or matched_cr.get("id") if matched_cr else None
        ),
        "evidence": {
            "device_change": device_change,
            "matching_crs": [matched_cr] if matched_cr else [],
        },
    }


def _fetch_approval_status(base_url, change_id, headers):
    """Query the approval_levels and approvals sub-resources for a change
    to determine the real approval state instead of relying on the static
    approval_status field."""
    try:
        levels_url = f"{base_url}/api/v3/changes/{change_id}/approval_levels"
        logging.info("  Fetching approval levels: GET %s", levels_url)
        levels_resp = requests.get(levels_url, headers=headers, timeout=30)
        levels_resp.raise_for_status()
        levels_data = levels_resp.json()
        approval_levels = levels_data.get("approval_levels", [])

        if not approval_levels:
            logging.info("  No approval levels found for change %s", change_id)
            return {"status": "no_levels", "levels": []}

        all_levels = []
        any_rejected = False
        all_levels_approved = True

        for level in approval_levels:
            level_id = level.get("id")
            level_number = level.get("level", level.get("id"))
            level_status = _safe_name(level.get("status"))

            # Fetch individual approvals for this level
            approvals_url = (
                f"{base_url}/api/v3/changes/{change_id}"
                f"/approval_levels/{level_id}/approvals"
            )
            logging.info(
                "  Fetching approvals for level %s: GET %s",
                level_number, approvals_url,
            )
            approvals_resp = requests.get(
                approvals_url, headers=headers, timeout=30,
            )
            approvals_resp.raise_for_status()
            approvals_data = approvals_resp.json()
            individual_approvals = approvals_data.get("approvals", [])

            approval_details = []
            for appr in individual_approvals:
                appr_status = _safe_name(appr.get("status"))
                approver = _safe_name(appr.get("approver"))
                approval_details.append({
                    "approver": approver,
                    "status": appr_status,
                })
                if "rejected" in appr_status.lower():
                    any_rejected = True

            if level_status.lower() != "approved":
                all_levels_approved = False

            level_info = {
                "level": level_number,
                "status": level_status,
                "approvals": approval_details,
            }
            all_levels.append(level_info)
            logging.info(
                "  Level %s status=%s, approvals=%s",
                level_number, level_status, json.dumps(approval_details),
            )

        if any_rejected:
            resolved = "rejected"
        elif all_levels_approved:
            resolved = "approved"
        else:
            resolved = "pending"

        logging.info("  Resolved approval status: %s", resolved)
        return {"status": resolved, "levels": all_levels}

    except Exception as exc:
        logging.error(
            "  Failed to fetch approval levels for change %s: %s",
            change_id, exc,
        )
        return None


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="apexaiq_change_validation_agent",
    default_args=default_args,
    description="Validates device changes against ManageEngine change requests for compliance",
    schedule=None,
    catchup=False,
    tags=["compliance", "change-validation", "manageengine"],
) as dag:

    token_task = PythonOperator(
        task_id="fetch_zoho_token",
        python_callable=fetch_zoho_token,
    )

    extract_task = PythonOperator(
        task_id="extract_asset_ids",
        python_callable=extract_asset_ids,
    )

    fetch_task = PythonOperator(
        task_id="fetch_change_requests",
        python_callable=fetch_change_requests,
    )

    classify_task = PythonOperator(
        task_id="correlate_and_classify",
        python_callable=correlate_and_classify,
    )

    token_task >> extract_task >> fetch_task >> classify_task
