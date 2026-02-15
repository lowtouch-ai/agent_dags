"""
Thought-logging utility for Airflow DAG tasks.

Publishes log messages to Redis ``think:{request_id}`` channels so that
agentomatic can stream per-task progress to the WebUI in real time.

Usage in a DAG task callable::

    from agent_dags.utils.think_logging import get_logger, set_request_id

    lot = get_logger("my_dag")

    def my_task(**context):
        set_request_id(context)
        lot.info("doing something...")
"""

import json
import logging
import os
from contextvars import ContextVar

import redis

# ---------------------------------------------------------------------------
# Context variable — stores request_id per task execution
# ---------------------------------------------------------------------------
_current_request_id: ContextVar[str | None] = ContextVar(
    "current_request_id", default=None
)


def set_request_id(context: dict) -> str | None:
    """Extract ``__request_id`` from Airflow task *context* and store it.

    Checks ``dag_run.conf`` first (where the API trigger places it),
    then falls back to ``params`` (merged view).  Returns the id or
    ``None`` if absent — callers never need to guard against this.
    """
    rid = None
    try:
        dag_run = context.get("dag_run")
        if dag_run and getattr(dag_run, "conf", None):
            rid = dag_run.conf.get("__request_id")
        if rid is None:
            rid = context.get("params", {}).get("__request_id")
    except Exception:
        pass

    if rid:
        _current_request_id.set(rid)
    return rid


# ---------------------------------------------------------------------------
# Logging filter — injects request_id into every LogRecord
# ---------------------------------------------------------------------------
class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", None) or _current_request_id.get()  # type: ignore[attr-defined]
        return True


# ---------------------------------------------------------------------------
# Redis handler — publishes to think:{request_id}
# ---------------------------------------------------------------------------
class RedisThinkHandler(logging.Handler):
    """Logging handler that publishes to Redis Pub/Sub for thought streaming."""

    def __init__(
        self,
        redis_url: str | None = None,
        channel_prefix: str = "think:",
    ):
        super().__init__()
        if redis_url is None:
            host = os.getenv("REDIS_HOST", "redis")
            port = int(os.getenv("REDIS_PORT", "6379").split(":")[-1])
            db = os.getenv("REDIS_DB", "0")
            redis_url = f"redis://{host}:{port}/{db}"
        self._client = redis.from_url(redis_url, decode_responses=True)
        self._prefix = channel_prefix

    def emit(self, record: logging.LogRecord) -> None:
        try:
            rid = getattr(record, "request_id", None) or _current_request_id.get()
            if not rid:
                return  # no request context — silently skip

            payload = {
                "request_id": rid,
                "source": "airflow_dag",
                "level": record.levelname,
                "message": self.format(record),
                "logger": record.name,
            }
            self._client.publish(f"{self._prefix}{rid}", json.dumps(payload))
        except Exception:
            self.handleError(record)


# ---------------------------------------------------------------------------
# Public helper — returns a ready-to-use thought logger
# ---------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    """Return a logger that publishes to Redis ``think:{request_id}``.

    The logger has ``propagate=False`` so messages only go to Redis, not
    to Airflow's root logger (the regular ``logger`` still handles that).
    """
    log = logging.getLogger(f"airflow_dag.think.{name}")
    if not log.handlers:
        handler = RedisThinkHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(handler)
        log.addFilter(_RequestIdFilter())
        log.setLevel(logging.INFO)
        log.propagate = False
    return log
