"""
Change Validation & Compliance Agent (MVP)
Validates an infrastructure device change against approved ManageEngine
ServiceDesk Plus change controls. Classifies the change as Validated,
Unapproved, Partial Approval, or Inconclusive and sends an email notification.

Trigger: Manual only — pass params via dag_run.conf.

Sample conf (minimal — asset_id only):
{
  "asset_id": "300000000000001"
}

Sample conf (full):
{
  "asset_id": "300000000000001",
  "device_name": "router-01",
  "change_type": "configuration",
  "change_details": "Modified ACL rules on GigabitEthernet0/1",
  "previous_state": "permit ip 10.0.0.0/8 any",
  "current_state": "permit ip 10.0.0.0/8 any; deny ip 192.168.1.0/24 any",
  "change_timestamp": "2025-12-15T14:30:00Z"
}

Params:
  - asset_id         (REQUIRED) ManageEngine asset ID to validate against
  - device_name      (optional) Human-readable device name, defaults to asset name from API
  - change_type      (optional) Type of change, e.g. "configuration"
  - change_details   (optional) Description of what changed
  - previous_state   (optional) State before the change
  - current_state    (optional) State after the change
  - change_timestamp (optional) ISO-8601 timestamp; if omitted, time-window validation is skipped

Airflow Variables required:
  - ltai.change_validation.me.base_url       (ManageEngine SDP Cloud base URL)
  - APEXAIQ_ZOHO_CLIENT_ID                   (Zoho OAuth2 client ID)
  - APEXAIQ_ZOHO_CLIENT_SECRET               (Zoho OAuth2 client secret)
  - SMTP_USER                                (SMTP username for sending emails)
  - SMTP_PASSWORD                            (SMTP password for sending emails)
  - APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL   (comma-separated recipient emails)
"""

from airflow.sdk import DAG, Param, Variable
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
import html as html_mod
import pendulum
import json
import logging
import re
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ME_BASE_URL = Variable.get("ltai.change_validation.me.base_url", default="")
ZOHO_CLIENT_ID = Variable.get("APEXAIQ_ZOHO_CLIENT_ID", default="")
ZOHO_CLIENT_SECRET = Variable.get("APEXAIQ_ZOHO_CLIENT_SECRET", default="")

# ---------------------------------------------------------------------------
# SMTP Email Configuration
# ---------------------------------------------------------------------------
SMTP_USER = Variable.get("SMTP_USER", default="")
SMTP_PASSWORD = Variable.get("SMTP_PASSWORD", default="")
SMTP_HOST = Variable.get("SMTP_HOST", default="mail.authsmtp.com")
SMTP_PORT = int(Variable.get("SMTP_PORT", default="2525"))
SMTP_SUFFIX = Variable.get("SMTP_FROM_SUFFIX", default="via lowtouch.ai <webmaster@ecloudcontrol.com>")
NOTIFY_EMAIL = Variable.get("APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL", default="")

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
def _fetch_asset_name(base_url, asset_id, headers):
    """Fetch the asset name from ManageEngine for a given asset_id.
    Returns the asset name string or None on failure."""
    detail_url = f"{base_url}/api/v3/assets/{asset_id}"
    logging.info("Fetching asset name for %s: GET %s", asset_id, detail_url)
    try:
        resp = requests.get(detail_url, headers=headers, timeout=30)
        resp.raise_for_status()
        asset_data = resp.json().get("asset", {})
        name = asset_data.get("name")
        logging.info("Asset %s name: %s", asset_id, name)
        return name
    except requests.exceptions.RequestException as exc:
        logging.warning("Failed to fetch asset name for %s: %s", asset_id, str(exc))
        return None


def extract_asset_ids(**kwargs):
    """Read flat params from dag_run.conf, validate, and push the asset_id
    plus the normalised change entry to XCom.

    ``asset_id`` is required.  All other fields are optional and receive
    sensible defaults when missing.  If ``device_name`` is not provided,
    the asset name is fetched from the ManageEngine API."""

    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf or {}

    asset_id = conf.get("asset_id")
    if not asset_id:
        raise ValueError("No asset_id provided in DAG conf")

    asset_id_str = str(asset_id)

    # Prepare API access for asset name lookups
    access_token = ti.xcom_pull(task_ids="fetch_zoho_token", key="access_token")
    base_url = ME_BASE_URL.rstrip("/") if ME_BASE_URL else ""
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/vnd.manageengine.sdp.v3+json",
    } if access_token and base_url else {}

    # Resolve device_name: use provided value, or fetch asset name from API
    device_name = conf.get("device_name") or ""
    if not device_name and headers and base_url:
        device_name = _fetch_asset_name(base_url, asset_id_str, headers) or ""

    change_ts = conf.get("change_timestamp") or None

    # Normalise into a single-entry list so downstream tasks work unchanged
    normalised = {
        "asset_id": asset_id_str,
        "device_name": device_name or "unknown",
        "change_type": conf.get("change_type") or "",
        "change_details": conf.get("change_details") or "",
        "previous_state": conf.get("previous_state") or "",
        "current_state": conf.get("current_state") or "",
        "change_timestamp": change_ts if change_ts else None,
    }

    unique_asset_ids = [asset_id_str]
    logging.info("Extracted asset ID: %s", asset_id_str)

    ti.xcom_push(key="asset_ids", value=unique_asset_ids)
    ti.xcom_push(key="device_changes", value=[normalised])
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
        # Store only the fields needed by correlate_and_classify
        slim_cr = {
            "id": change_detail.get("id"),
            "display_id": change_detail.get("display_id"),
            "title": change_detail.get("title", ""),
            "description": change_detail.get("description", ""),
            "approval_status": change_detail.get("approval_status"),
            "scheduled_start_time": change_detail.get("scheduled_start_time"),
            "scheduled_end_time": change_detail.get("scheduled_end_time"),
            "_resolved_approval": change_detail.get("_resolved_approval"),
        }

        for asset in linked_assets:
            linked_id = str(asset.get("id", ""))
            if linked_id in asset_id_set:
                all_changes[linked_id].append(slim_cr)
                logging.info("  MATCH: Change %s is linked to target asset %s", cs_display, linked_id)

    logging.info("=== Asset-to-Change matching summary ===")
    for asset_id in asset_ids:
        logging.info(
            "Asset %s — matched %d change request(s)", asset_id, len(all_changes[asset_id])
        )

    ti.xcom_push(key="change_requests", value=all_changes)

    # Return only the match counts — full data is in XCom
    return {aid: len(crs) for aid, crs in all_changes.items()}


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
        device_name = device_change.get("device_name", "unknown")
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
            # If no change_timestamp was provided, skip time-window
            # validation — approval status alone determines the result.
            if not change_ts:
                in_window = True
            else:
                in_window = _check_time_window(change_ts, start_val, end_val)

            cr_label = _safe_display_id(cr)

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
            r["classification"], r["device_name"], r["asset_id"],
            r["change_detected"][:80],
        )

    ti.xcom_push(key="validation_results", value=results)
    ti.xcom_push(key="validation_summary", value=summary)

    # Return only the summary for auto-logging; full results are in XCom
    return summary


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


def _safe_display_id(cr):
    """Extract a human-readable change ID string from a ManageEngine change record.
    display_id is often a dict like {"value": "9", "display_value": "CH-9"}."""
    raw = cr.get("display_id")
    if isinstance(raw, dict):
        return raw.get("display_value", raw.get("value"))
    return raw or str(cr.get("id", "unknown"))


def _strip_html(text):
    """Remove HTML tags and collapse whitespace from a string."""
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", str(text))
    return re.sub(r"\s+", " ", cleaned).strip()


def _build_result(device_change, classification, reason, matched_cr):
    # Use conf-provided change_details; fall back to the matched CR's
    # description or title so the email column is never blank.
    change_detected = device_change.get("change_details", "")
    if not change_detected and matched_cr:
        raw = matched_cr.get("description") or matched_cr.get("title") or ""
        change_detected = _strip_html(raw)

    cr_title = _strip_html(matched_cr.get("title", "")) if matched_cr else ""

    return {
        "device_name": device_change.get("device_name", "unknown"),
        "asset_id": str(device_change.get("asset_id")),
        "change_detected": change_detected,
        "change_timestamp": device_change.get("change_timestamp"),
        "classification": classification,
        "reason": reason,
        "matching_change_id": (
            _safe_display_id(matched_cr) if matched_cr else None
        ),
        "matching_change_title": cr_title,
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
# SMTP email helper
# ---------------------------------------------------------------------------
def _send_smtp_email(recipient, subject, body, cc_emails=None):
    """Send an SMTP email with an HTML body."""
    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)

        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"] = f"Change Validation Agent {SMTP_SUFFIX}"
        msg["To"] = recipient
        if cc_emails:
            msg["Cc"] = cc_emails

        msg.attach(MIMEText(body, "html"))

        recipients_list = [recipient]
        if cc_emails:
            cc_list = [e.strip() for e in cc_emails.split(",") if e.strip()]
            recipients_list.extend(cc_list)

        server.sendmail("webmaster@ecloudcontrol.com", recipients_list, msg.as_string())
        logging.info("Email sent successfully to %s", recipient)
        server.quit()
        return True
    except Exception as exc:
        logging.error("Failed to send email: %s", str(exc))
        return None


# ---------------------------------------------------------------------------
# Email template helpers
# ---------------------------------------------------------------------------
def _notification_css():
    """Shared CSS for change validation notification emails."""
    return """
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                         "Helvetica Neue", Arial, sans-serif;
            margin: 0; padding: 20px;
            background-color: #f4f7f6; color: #333;
        }
        .container {
            max-width: 850px; margin: 0 auto;
            background-color: #ffffff; border: 1px solid #e0e0e0;
            border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            overflow: hidden;
        }
        .header { padding: 24px 30px; color: #ffffff; }
        .header h1 { margin: 0; font-size: 22px; }
        .content { padding: 30px; }
        .section { margin-bottom: 28px; }
        .section h2 {
            font-size: 18px; margin: 0 0 12px 0; color: #004a99;
            border-bottom: 2px solid #f0f0f0; padding-bottom: 5px;
        }
        .detail-table {
            width: 100%; border-collapse: collapse; font-size: 13px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .detail-table th, .detail-table td {
            padding: 10px 12px; border: 1px solid #ddd; text-align: left;
        }
        .detail-table th { background-color: #f9f9f9; font-weight: 600; }
        .detail-table tr:nth-child(even) { background-color: #fdfdfd; }
        .footer {
            padding: 20px 30px; text-align: left; font-size: 14px;
            color: #fcfcfc; background-color: #1e275d;
        }
    </style>
    """


def _build_summary_cards(summary):
    """Build an email-safe summary table from the validation summary."""
    cards = [
        ("#28a745", summary.get("validated", 0), "Validated"),
        ("#ff8c00", summary.get("partial_approval", 0), "Partial Approval"),
        ("#dc3545", summary.get("unapproved", 0), "Unapproved"),
        ("#6c757d", summary.get("inconclusive", 0), "Inconclusive"),
    ]
    cells = ""
    for color, num, label in cards:
        cells += (
            f'<td style="text-align:center;padding:14px;border-radius:6px;'
            f'border:1px solid #e0e0e0;width:25%;">'
            f'<div style="font-size:28px;font-weight:700;color:{color};">{num}</div>'
            f'<div style="font-size:12px;color:#666;margin-top:4px;">{label}</div>'
            f'</td>'
        )
    return (
        f'<table style="width:100%;border-collapse:separate;border-spacing:8px 0;">'
        f'<tr>{cells}</tr></table>'
    )


def _build_change_detail_table(results):
    """Build an HTML detail table for a list of validation results.
    Always renders the full table structure — shows an empty-state row
    when there are no results so the table never collapses."""
    _esc = html_mod.escape

    rows = ""
    if not results:
        rows = """
            <tr>
                <td colspan="6" style="text-align:center;color:#999;padding:18px;">
                    No changes in this category.
                </td>
            </tr>"""
    else:
        for r in results:
            cr_id = r.get("matching_change_id")
            cr_title = r.get("matching_change_title", "")
            if cr_id and cr_title:
                cr_display = f"{_esc(str(cr_id))}: {_esc(cr_title)}"
            elif cr_id:
                cr_display = _esc(str(cr_id))
            else:
                cr_display = "None"

            change_details = _esc(r.get("change_detected") or "N/A")[:100]
            timestamp = _esc(str(r.get("change_timestamp") or "N/A"))

            rows += f"""
            <tr>
                <td>{_esc(r.get('device_name', 'N/A'))}</td>
                <td>{_esc(r.get('asset_id', 'N/A'))}</td>
                <td>{change_details}</td>
                <td>{timestamp}</td>
                <td>{_esc(r.get('reason', 'N/A'))}</td>
                <td>{cr_display}</td>
            </tr>"""

    return f"""
    <table class="detail-table">
        <thead>
            <tr>
                <th>Device Name</th>
                <th>Asset ID</th>
                <th>Change Details</th>
                <th>Timestamp</th>
                <th>Reason</th>
                <th>Matching CR</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>"""


def _compose_notification_html(header_bg, badge_bg, badge_text, title,
                                intro_text, summary, filtered_results):
    """Compose a full notification email HTML document."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    {_notification_css()}
</head>
<body>
    <div class="container">
        <div class="header" style="background-color:{header_bg};">
            <h1>{title}</h1>
            <span style="display:inline-block;padding:4px 12px;border-radius:4px;
                         font-size:13px;font-weight:600;margin-top:8px;color:#fff;
                         background-color:{badge_bg};">{badge_text}</span>
        </div>
        <div class="content">
            <p>{intro_text}</p>

            <div class="section">
                <h2>Validation Summary</h2>
                {_build_summary_cards(summary)}
            </div>

            <div class="section">
                <h2>Change Details</h2>
                {_build_change_detail_table(filtered_results)}
            </div>
        </div>
        <div class="footer">
            Best regards,<br>Change Validation Agent
            <center>
                <span style="font-size:14px;opacity:0.9;">
                    Powered by lowtouch<span style="color:#fb47de;">.ai</span>
                </span>
            </center>
        </div>
    </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Task 4a — Compose approved-changes email
# ---------------------------------------------------------------------------
def compose_approved_email(**kwargs):
    """Build HTML notification for validated (fully approved) changes."""
    ti = kwargs["ti"]
    logging.info("compose_approved_email: pulling XCom from correlate_and_classify")

    results = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_results")
    summary = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_summary")

    logging.info("compose_approved_email: results type=%s, summary type=%s",
                 type(results).__name__, type(summary).__name__)

    if not results:
        logging.warning("compose_approved_email: no validation_results from XCom")
        ti.xcom_push(key="approved_email_html", value="")
        return "No validation results available"

    if not summary:
        summary = {}

    approved = [r for r in results if r.get("classification") == "Validated Change"]
    if not approved:
        ti.xcom_push(key="approved_email_html", value="")
        return "No validated changes — skipping email"

    html = _compose_notification_html(
        header_bg="#1b5e20",
        badge_bg="#28a745",
        badge_text=f"{len(approved)} Validated",
        title="Change Validation Report — All Changes Approved",
        intro_text=(
            "The following infrastructure changes have been <strong>validated</strong> "
            "against approved change requests. Each change matches an approved CR "
            "within the scheduled maintenance window."
        ),
        summary=summary,
        filtered_results=approved,
    )
    ti.xcom_push(key="approved_email_html", value=html)
    logging.info("Composed approved email for %d change(s)", len(approved))
    return f"Composed approved email for {len(approved)} change(s)"


# ---------------------------------------------------------------------------
# Task 4b — Compose partial-approval email
# ---------------------------------------------------------------------------
def compose_partial_approval_email(**kwargs):
    """Build HTML notification for changes with missing or partial approval."""
    ti = kwargs["ti"]
    logging.info("compose_partial_approval_email: pulling XCom from correlate_and_classify")

    results = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_results")
    summary = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_summary")

    logging.info("compose_partial_approval_email: results type=%s, summary type=%s",
                 type(results).__name__, type(summary).__name__)

    if not results:
        logging.warning("compose_partial_approval_email: no validation_results from XCom")
        ti.xcom_push(key="partial_email_html", value="")
        return "No validation results available"

    if not summary:
        summary = {}

    partial = [r for r in results if r.get("classification") == "Missing or Partial Approval"]
    if not partial:
        ti.xcom_push(key="partial_email_html", value="")
        return "No partial-approval changes — skipping email"

    html = _compose_notification_html(
        header_bg="#e65100",
        badge_bg="#ff8c00",
        badge_text=f"{len(partial)} Partial Approval",
        title="Change Validation Alert — Partial Approval Detected",
        intro_text=(
            "The following infrastructure changes have <strong>incomplete or missing "
            "approvals</strong>. A matching change request was found but either the "
            "approval workflow is not fully completed or the change occurred outside "
            "the scheduled maintenance window. Please review and take action."
        ),
        summary=summary,
        filtered_results=partial,
    )
    ti.xcom_push(key="partial_email_html", value=html)
    logging.info("Composed partial-approval email for %d change(s)", len(partial))
    return f"Composed partial-approval email for {len(partial)} change(s)"


# ---------------------------------------------------------------------------
# Task 4c — Compose no-approval (unapproved) email
# ---------------------------------------------------------------------------
def compose_no_approval_email(**kwargs):
    """Build HTML notification for unapproved changes."""
    ti = kwargs["ti"]
    logging.info("compose_no_approval_email: pulling XCom from correlate_and_classify")

    results = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_results")
    summary = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_summary")

    logging.info("compose_no_approval_email: results type=%s, summary type=%s",
                 type(results).__name__, type(summary).__name__)

    if not results:
        logging.warning("compose_no_approval_email: no validation_results from XCom")
        ti.xcom_push(key="no_approval_email_html", value="")
        return "No validation results available"

    if not summary:
        summary = {}

    unapproved = [r for r in results if r.get("classification") == "Unapproved Change"]
    if not unapproved:
        ti.xcom_push(key="no_approval_email_html", value="")
        return "No unapproved changes — skipping email"

    html = _compose_notification_html(
        header_bg="#b71c1c",
        badge_bg="#dc3545",
        badge_text=f"{len(unapproved)} Unapproved",
        title="Change Validation Alert — Unapproved Changes Detected",
        intro_text=(
            "<strong>ATTENTION:</strong> The following infrastructure changes were "
            "detected <strong>without any matching approved change request</strong>. "
            "These changes may represent unauthorized modifications and require "
            "immediate investigation."
        ),
        summary=summary,
        filtered_results=unapproved,
    )
    ti.xcom_push(key="no_approval_email_html", value=html)
    logging.info("Composed no-approval email for %d change(s)", len(unapproved))
    return f"Composed no-approval email for {len(unapproved)} change(s)"


# ---------------------------------------------------------------------------
# Task 5 — Send notification emails
# ---------------------------------------------------------------------------
def send_notification_emails(**kwargs):
    """Send all composed notification emails to the configured recipient."""
    ti = kwargs["ti"]

    if not SMTP_USER or not SMTP_PASSWORD:
        raise ValueError(
            "SMTP credentials missing — set Airflow Variables "
            "SMTP_USER and SMTP_PASSWORD"
        )

    if not NOTIFY_EMAIL:
        raise ValueError(
            "Recipient email missing — set Airflow Variable "
            "APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL"
        )

    email_configs = [
        ("approved_email_html", "compose_approved_email",
         "Change Validation Report — All Changes Approved"),
        ("partial_email_html", "compose_partial_approval_email",
         "Change Validation Alert — Partial Approval Detected"),
        ("no_approval_email_html", "compose_no_approval_email",
         "Change Validation Alert — Unapproved Changes Detected"),
    ]

    # Parse recipients: first = TO, rest = CC
    email_list = [e.strip() for e in NOTIFY_EMAIL.split(",") if e.strip()]
    if not email_list:
        raise ValueError("No valid email addresses in APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL")

    to_email = email_list[0]
    cc_emails = ", ".join(email_list[1:]) if len(email_list) > 1 else None

    sent = []
    for xcom_key, task_id, subject in email_configs:
        html = ti.xcom_pull(task_ids=task_id, key=xcom_key)
        if not html:
            logging.info("No content for %s — skipping", xcom_key)
            continue

        result = _send_smtp_email(
            recipient=to_email,
            subject=subject,
            body=html,
            cc_emails=cc_emails,
        )
        if result:
            sent.append(subject)
            logging.info("Sent: %s", subject)
        else:
            logging.error("Failed to send: %s", subject)

    if not sent:
        logging.info("No notification emails needed for this run")
        return "No emails to send"

    return f"Sent {len(sent)} notification email(s)"


# ---------------------------------------------------------------------------
# Task 4 — Branch: pick only the compose task(s) that have results
# ---------------------------------------------------------------------------
def choose_email_tasks(**kwargs):
    """Return the task_id(s) of compose tasks whose classification count > 0.
    Tasks not returned are marked *skipped* by the BranchPythonOperator."""
    ti = kwargs["ti"]
    summary = ti.xcom_pull(task_ids="correlate_and_classify", key="validation_summary")

    if not summary:
        logging.warning("No validation summary in XCom — defaulting to compose_approved_email")
        return ["compose_approved_email"]

    tasks = []
    if summary.get("validated", 0) > 0:
        tasks.append("compose_approved_email")
    if summary.get("partial_approval", 0) > 0:
        tasks.append("compose_partial_approval_email")
    if summary.get("unapproved", 0) > 0:
        tasks.append("compose_no_approval_email")

    # Fallback when only inconclusive — run one task that will early-exit
    if not tasks:
        logging.info("No actionable classifications — defaulting to compose_approved_email")
        tasks = ["compose_approved_email"]

    logging.info("Branch decision: %s", tasks)
    return tasks


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="apexaiq_change_validation_agent",
    default_args=default_args,
    description=(
        "Validates an infrastructure device change against approved ManageEngine "
        "ServiceDesk Plus change controls. Classifies the change as Validated, "
        "Unapproved, Partial Approval, or Inconclusive and sends an email notification. "
        "Provide the asset_id to validate — all other fields are optional."
    ),
    schedule=None,
    catchup=False,
    tags=["compliance", "change-validation", "manageengine"],
    params={
        "asset_id": Param(
            type="string",
            description="ManageEngine ServiceDesk Plus Cloud numeric asset ID to validate (e.g. '300000000000001')",
        ),
        "device_name": Param(
            type=["string", "null"],
            default=None,
            description="Human-readable device name (e.g. 'router-01'). If omitted, auto-fetched from ManageEngine.",
        ),
        "change_type": Param(
            type=["string", "null"],
            default=None,
            description="Category of change performed (e.g. 'configuration', 'firmware_upgrade', 'hardware_replacement', 'access_change')",
        ),
        "change_details": Param(
            type=["string", "null"],
            default=None,
            description="Description of what was changed on the device (e.g. 'Modified ACL rules on GigabitEthernet0/1')",
        ),
        "previous_state": Param(
            type=["string", "null"],
            default=None,
            description="Device configuration or state before the change was applied",
        ),
        "current_state": Param(
            type=["string", "null"],
            default=None,
            description="Device configuration or state after the change was applied",
        ),
        "change_timestamp": Param(
            type=["string", "null"],
            default=None,
            description="ISO-8601 timestamp of when the change occurred (e.g. '2025-12-15T14:30:00Z'). If omitted, time-window validation is skipped.",
        ),
    },
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

    branch_task = BranchPythonOperator(
        task_id="choose_email_tasks",
        python_callable=choose_email_tasks,
    )

    approved_email_task = PythonOperator(
        task_id="compose_approved_email",
        python_callable=compose_approved_email,
    )

    partial_email_task = PythonOperator(
        task_id="compose_partial_approval_email",
        python_callable=compose_partial_approval_email,
    )

    no_approval_email_task = PythonOperator(
        task_id="compose_no_approval_email",
        python_callable=compose_no_approval_email,
    )

    send_email_task = PythonOperator(
        task_id="send_notification_emails",
        python_callable=send_notification_emails,
        trigger_rule="none_failed_min_one_success",
    )

    # Linear pipeline up to branching point
    token_task >> extract_task >> fetch_task >> classify_task >> branch_task

    # Fan-out: branch selects only the relevant compose task(s);
    # unselected tasks are skipped. All three feed into send.
    branch_task >> approved_email_task >> send_email_task
    branch_task >> partial_email_task >> send_email_task
    branch_task >> no_approval_email_task >> send_email_task
