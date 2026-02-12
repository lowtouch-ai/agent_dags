# Change Validation Agent

## Overview

Validates infrastructure device changes against approved ManageEngine ServiceDesk Plus Cloud change controls. Detects unauthorized or mismatched changes and classifies them for compliance review.

## DAG: `apexaiq_change_validation_agent`

- **Trigger:** Manual only (no schedule) — triggered via `dag_run.conf`
- **Airflow version:** 3.1.3

## Trigger Config

The DAG expects this payload in `dag_run.conf`:

```json
{
  "device_changes": [
    {
      "device_id": "RTR-CORE-01",
      "asset_id": "256963000000366263",
      "device_ip": "10.0.1.1",
      "change_type": "configuration",
      "change_details": "Updated ACL rules on interface GigabitEthernet0/1",
      "change_timestamp": "2025-02-10T14:30:00Z",
      "config_before_hash": "abc123def456",
      "config_after_hash": "xyz789uvw012",
      "changed_attributes": ["interface.GigabitEthernet0/1.access-list"]
    }
  ]
}
```

### Required fields per device change

| Field | Description |
|---|---|
| `asset_id` | **ManageEngine internal asset ID** (numeric, e.g. `256963000000366263`) — not a custom tag |
| `device_id` | Device identifier for logging/reporting |
| `change_details` | Human-readable description of the change |
| `change_timestamp` | ISO-8601 timestamp — used for time-window matching against CRs |

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

## Task Pipeline

```
fetch_zoho_token >> extract_asset_ids >> fetch_change_requests >> correlate_and_classify
```

1. **fetch_zoho_token** — Obtains a fresh Zoho OAuth2 access token using `client_credentials` grant and pushes it to XCom
2. **extract_asset_ids** — Parses `dag_run.conf`, validates entries, deduplicates asset IDs
3. **fetch_change_requests** — Three-step correlation against ManageEngine:
   - **Step 1:** `GET /api/v3/changes` — fetches all change summaries (the list endpoint does not return asset associations)
   - **Step 2:** `GET /api/v3/changes/{change_id}` — for each change, fetches full details which includes the `assets` array
   - **Step 3:** `GET /api/v3/changes/{change_id}/approval_levels` + `/approval_levels/{level_id}/approvals` — resolves real approval status by walking each approval level and its individual approvals (the top-level `approval_status` field on a change is unreliable)
   - Filters client-side: matches changes where the `assets[]` array contains the target asset ID
   - Resolved approval stored as `_resolved_approval` on each change detail: `approved` (all levels approved), `rejected` (any rejected), or `pending`
4. **correlate_and_classify** — Matches device changes to CRs and classifies using resolved approval status:
   - `Validated Change` — Approved CR found within scheduled window
   - `Unapproved Change` — No matching CR or no CRs at all
   - `Missing or Partial Approval` — CR exists but not approved or outside time window
   - `Data Inconclusive` — CRs exist but could not be matched
   - Reason strings include per-level detail (e.g. `[L1=approved, L2=pending]`)

## ManageEngine SDP Cloud API Notes

### Key endpoints used

| Purpose | Endpoint | Notes |
|---|---|---|
| List all changes | `GET /api/v3/changes` | Returns summary only — no `assets` field |
| Get change detail | `GET /api/v3/changes/{id}` | Returns full detail including `assets[]` and `configuration_items[]` |
| List approval levels | `GET /api/v3/changes/{id}/approval_levels` | Returns approval levels for a change |
| List approvals per level | `GET /api/v3/changes/{id}/approval_levels/{level_id}/approvals` | Returns individual approvals (approver, status) for a level |
| List assets | `GET /api/v3/assets` | Use to look up asset IDs by name |
| Get asset detail | `GET /api/v3/assets/{id}` | Returns asset info (no reverse link to changes) |

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

## Postman Collection

The file `ServiceDesk Plus Cloud.postman_collection.json` in this directory contains the full ManageEngine SDP Cloud API reference (~170+ endpoints).
