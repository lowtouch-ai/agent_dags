# Change Validation Agent

## Overview

Validates infrastructure device changes against approved ManageEngine ServiceDesk Plus Cloud change controls. Detects unauthorized or mismatched changes and classifies them for compliance review.

## DAG: `apexaiq_change_validation_agent`

- **Trigger:** Manual only (no schedule) — triggered via `dag_run.conf`
- **Airflow version:** 3.1.3

## Trigger Config

The DAG expects flat params in `dag_run.conf`. Only `asset_id` is required — all other fields are optional.

Minimal example:
```json
{
  "asset_id": "256963000000366263"
}
```

Full example (all fields):
```json
{
  "asset_id": "256963000000366263",
  "device_name": "SDWAN-Branch-Router-01",
  "change_type": "configuration",
  "change_details": "Updated ACL rules on interface GigabitEthernet0/1",
  "previous_state": "permit ip 10.0.0.0/8 any",
  "current_state": "permit ip 10.0.0.0/8 any; deny ip 192.168.1.0/24 any",
  "change_timestamp": "2025-08-10T14:30:00Z"
}
```

### Params

Each param is defined using `Param()` in the DAG. Required params have `type="string"` (no default). Optional params have `type=["string", "null"]` with `default=None` — this allows the `fetch_dag_metadata` function to distinguish required vs optional by checking if `"null"` is in the schema type.

| Param | Required | Default | Description |
|---|---|---|---|
| `asset_id` | **Yes** | — | ManageEngine internal asset ID (numeric, e.g. `256963000000366263`) |
| `device_name` | No | `null` (auto-fetched from API) | Human-readable device name for logging/reporting. If omitted, fetched automatically from ManageEngine assets API |
| `change_type` | No | `null` | Type of change, e.g. `"configuration"` |
| `change_details` | No | `null` (falls back to CR description) | Description of what changed. If omitted, falls back to the matched CR's description or title from ManageEngine |
| `previous_state` | No | `null` | State before the change |
| `current_state` | No | `null` | State after the change |
| `change_timestamp` | No | `null` (skips window check) | ISO-8601 timestamp — used for time-window matching against CRs. If omitted, time-window validation is skipped entirely and approval status alone determines the classification |

### Agent integration (`fetch_dag_metadata`)

The `fetch_dag_metadata` function must use the schema type (not just `value`) to detect required vs optional:

```python
schema_type = schema.get("type", "string")
is_nullable = isinstance(schema_type, list) and "null" in schema_type
param = {
    "name": param_name,
    "type": schema_type if not is_nullable else [t for t in schema_type if t != "null"][0],
    "required": param_schema.get("value") is None and not is_nullable,
    "description": param_schema.get("description", ""),
}
```

- `asset_id`: `schema.type = "string"` → `is_nullable = False` → `required = True`
- All others: `schema.type = ["string", "null"]` → `is_nullable = True` → `required = False`

### Auto-enrichment from API

When optional fields are omitted, the DAG enriches them from ManageEngine:

| Field | Enrichment source |
|---|---|
| `device_name` | `GET /api/v3/assets/{asset_id}` → `asset.name` (cached per asset to avoid duplicate calls) |
| `change_details` | Matched CR's `description` field (HTML-stripped), falling back to CR `title` |
| Email "Matching CR" column | Shows `{display_id}: {title}` (e.g. `CH-9: SDWAN Branch upgrade`) instead of just the ID |

### Authentication

- The DAG automatically fetches a **Zoho OAuth2 access token** at the start of each run using the `client_credentials` grant
- No manual token is needed in `dag_run.conf` — credentials are stored as Airflow Variables
- Token endpoint: `POST https://accounts.zoho.com/oauth/v2/token`
- Scopes requested: `SDPOnDemand.requests.ALL`, `SDPOnDemand.requests.READ`, `SDPOnDemand.assets.READ`, `SDPOnDemand.changes.READ`, `SDPOnDemand.requests.CREATE`

### Airflow Variables

| Variable | Purpose |
|---|---|
| `ltai.change_validation.me.base_url` | ManageEngine SDP Cloud base URL (e.g. `https://sdpondemand.manageengine.com/app/itdesk`) |
| `APEXAIQ_ZOHO_CLIENT_ID` | Zoho OAuth2 client ID for token generation |
| `APEXAIQ_ZOHO_CLIENT_SECRET` | Zoho OAuth2 client secret for token generation |
| `SMTP_USER` | SMTP username for sending notification emails |
| `SMTP_PASSWORD` | SMTP password for sending notification emails |
| `SMTP_HOST` | SMTP server hostname (default: `mail.authsmtp.com`) |
| `SMTP_PORT` | SMTP server port (default: `2525`) |
| `SMTP_FROM_SUFFIX` | Email "From" suffix (default: `via lowtouch.ai <webmaster@ecloudcontrol.com>`) |
| `APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL` | Comma-separated recipient email addresses for notifications |

## Task Pipeline

```
fetch_zoho_token >> extract_asset_ids >> fetch_change_requests >> correlate_and_classify >> choose_email_tasks
                                                                                              ├── compose_approved_email ──┐
                                                                                              ├── compose_partial_approval ┼── send_notification_emails
                                                                                              └── compose_no_approval ─────┘
```

1. **fetch_zoho_token** — Obtains a fresh Zoho OAuth2 access token using `client_credentials` grant and pushes it to XCom
2. **extract_asset_ids** — Reads flat params from `dag_run.conf`, validates `asset_id` (required), normalises optional fields with defaults, wraps into a single-entry list for downstream tasks. If `device_name` is missing, fetches asset name from ManageEngine API
3. **fetch_change_requests** — Three-step correlation against ManageEngine:
   - **Step 1:** `GET /api/v3/changes` — fetches all change summaries (the list endpoint does not return asset associations)
   - **Step 2:** `GET /api/v3/changes/{change_id}` — for each change, fetches full details which includes the `assets` array
   - **Step 3:** `GET /api/v3/changes/{change_id}/approval_levels` + `/approval_levels/{level_id}/approvals` — resolves real approval status by walking each approval level and its individual approvals (the top-level `approval_status` field on a change is unreliable)
   - Filters client-side: matches changes where the `assets[]` array contains the target asset ID
   - Resolved approval stored as `_resolved_approval` on each change detail: `approved` (all levels approved), `rejected` (any rejected), or `pending`
   - Stores 8 fields per CR in XCom: `id`, `display_id`, `title`, `description`, `approval_status`, `scheduled_start_time`, `scheduled_end_time`, `_resolved_approval`
4. **correlate_and_classify** — Matches device changes to CRs and classifies using resolved approval status:
   - `Validated Change` — Approved CR found; if `change_timestamp` provided, must be within scheduled window
   - `Unapproved Change` — No matching CR or no CRs at all
   - `Missing or Partial Approval` — CR exists but not approved, or `change_timestamp` provided and falls outside time window
   - `Data Inconclusive` — CRs exist but could not be matched
   - **Time-window behaviour:** If `change_timestamp` is `null`/omitted, time-window validation is skipped — approval status alone determines the result
   - Reason strings include per-level detail (e.g. `[L1=approved, L2=pending]`)
   - Falls back to CR description/title for empty `change_details`
5. **choose_email_tasks** (`BranchPythonOperator`) — Reads `validation_summary` from XCom and returns only the compose task_id(s) whose classification count > 0; unselected compose tasks are marked **skipped**
6. **compose_approved_email** — Builds a green-themed (`#1b5e20`) HTML email for `Validated Change` results
7. **compose_partial_approval_email** — Builds an amber-themed (`#e65100`) HTML email for `Missing or Partial Approval` results
8. **compose_no_approval_email** — Builds a red-themed (`#b71c1c`) HTML email for `Unapproved Change` results
9. **send_notification_emails** — Sends all non-empty composed emails via SMTP to `APEXAIQ_CHANGE_VALIDATION_NOTIFY_EMAIL` recipients (first = TO, rest = CC); uses `trigger_rule="none_failed_min_one_success"` so it runs even when some compose tasks are skipped

### Email table columns

| Column | Source |
|---|---|
| Device Name | `device_name` from conf, or asset name fetched from API |
| Asset ID | `asset_id` from conf |
| Change Details | `change_details` from conf, or CR `description`/`title` from API (HTML-stripped) |
| Timestamp | `change_timestamp` from conf, or `N/A` |
| Reason | Classification reason with approval level details |
| Matching CR | `{display_id}: {title}` from ManageEngine (e.g. `CH-9: SDWAN Branch upgrade`) |

All dynamic content in the email table is HTML-escaped via `html.escape()` to prevent broken layout from API responses containing raw HTML (especially ManageEngine `description` fields). The table always renders its full structure — empty results show a colspan placeholder row instead of omitting the table.

### Branching behaviour

- If only `partial_approval > 0` → only `compose_partial_approval_email` runs, the other two compose tasks are skipped
- If `validated > 0` AND `unapproved > 0` → both `compose_approved_email` and `compose_no_approval_email` run
- If all counts are 0 (only inconclusive) → falls back to `compose_approved_email` which early-exits with empty HTML

## ManageEngine SDP Cloud API Notes

### Key endpoints used

| Purpose | Endpoint | Notes |
|---|---|---|
| List all changes | `GET /api/v3/changes` | Returns summary only — no `assets` field |
| Get change detail | `GET /api/v3/changes/{id}` | Returns full detail including `assets[]`, `configuration_items[]`, `title`, `description` |
| List approval levels | `GET /api/v3/changes/{id}/approval_levels` | Returns approval levels for a change |
| List approvals per level | `GET /api/v3/changes/{id}/approval_levels/{level_id}/approvals` | Returns individual approvals (approver, status) for a level |
| Get asset detail | `GET /api/v3/assets/{id}` | Returns asset info including `name` — used for `device_name` enrichment |

### Asset API notes

- Assets are organized by **product type** — each type has its own `api_plural_name` (e.g. `asset_mobiles`, `asset_smartphones`)
- The generic `GET /api/v3/assets` may return a **400 Bad Request** for some product types — use the product-type-specific endpoint instead (e.g. `GET /api/v3/asset_mobiles`)
- `GET /api/v3/assets/{id}` (direct fetch by ID) works regardless of product type
- Default list pagination is small — pass `row_count: 100` in `list_info` to get more results

### Important API constraints

- **No `/api/v3/assets/{id}/changes` endpoint** — this sub-resource does not exist (returns 404)
- **No `search_criteria` filter for assets on changes** — `assets.id` / `asset.id` are not valid search fields on the list changes endpoint
- **Asset-to-change correlation must be done client-side** by fetching each change's full detail and inspecting its `assets[]` array
- All API requests use `application/x-www-form-urlencoded` with `input_data` as the form key
- Auth header format: `Authorization: Zoho-oauthtoken {token}`
- Accept header: `application/vnd.manageengine.sdp.v3+json`

### Approval resolution logic

- The top-level `approval_status` field on a change is **not reliable** — use the approval levels API instead
- The DAG walks `approval_levels` → `approvals` for each level and resolves:
  - **approved** — all levels have status `approved`
  - **rejected** — any individual approval has `rejected` in its status
  - **pending** — some levels are not yet approved
  - **no_levels** — no approval levels found for the change
- The resolved status is used by `correlate_and_classify` to determine the classification; the static `approval_status` field is only used as a fallback when resolved approval data is unavailable

### Logging

The `fetch_change_requests` task logs detailed debug info for each API call:
- Request URL and params
- Response status codes
- Per-change: title, status, approval status, scheduled window, linked assets, linked CIs
- Per-change: resolved approval status and per-level breakdown
- Match/no-match results per asset ID

**Note:** Airflow uses structlog — all logged objects must be serialized via `json.dumps()` or `str()` (raw dicts/lists cause formatting errors).

## Reference: CH-342 (HDFC AMC test case)

The PDF `#CH-342 Change Details - HDFC Asset Management Company Limited (1).pdf` documents a real-world change request used as the reference for this DAG:

| Field | Value |
|---|---|
| Title | HDFC AMC \|\| SDWAN Branch upgrade Version from 17.9.4 to 17.12.5 |
| Workflow | Network CR |
| Approval | 1 level — CAB Evaluation, approved by Vikas R Salunkhe |
| Scheduled window | Aug 6, 2025 05:00 PM → Aug 23, 2025 08:00 PM |
| Final status | Close / Completed |

**Key observation:** The real CH-342 had no assets linked (`Assets Involved: -`). For the DAG to correlate a change, the **asset must be linked** to the CR in ManageEngine.

## Postman Collection

The file `ServiceDesk Plus Cloud.postman_collection.json` in this directory contains the full ManageEngine SDP Cloud API reference (~170+ endpoints).
