"""CloudWatch custom metrics — the pipeline's observability surface.

Counting rows in a log line means nobody can alarm on them. This emitter turns
the numbers each stage already computes (records in, rejected, duplicates,
quality score, revenue) into CloudWatch metrics you can chart and alert on.

Design rules:

* **Never raise.** An observability failure must not fail a data run. Every
  boto3 error is logged and swallowed.
* **Never require AWS.** With ``enabled=False`` (or no namespace configured) the
  emitter buffers nothing and calls nothing, so unit tests and local runs are
  unaffected.
* **Respect the API limits.** CloudWatch accepts 1000 metric data items per
  ``PutMetricData`` call; we chunk at 20 to keep payloads small and partial
  failures cheap.

Typical use::

    metrics = MetricsEmitter.from_config(config, stage="stream_processor")
    metrics.count("RecordsIngested", 128)
    metrics.count("RecordsRejected", 3)
    metrics.gauge("QualityPassPct", 97.6, unit="Percent")
    metrics.flush()
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "Ecommerce/Pipeline"
MAX_ITEMS_PER_CALL = 20


def as_bool(value: Any, default: bool = False) -> bool:
    """Read a boolean that may have arrived as a string.

    A config file written by Terraform, or built from environment variables,
    carries ``"true"`` and ``"false"`` as strings — and ``bool("false")`` is
    ``True``, which silently turns a kill switch into an on switch. Anything
    that is already a bool passes through.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() not in ("", "false", "0", "no", "off", "none")

class MetricsEmitter:
    """Buffers metric data and ships it to CloudWatch on :meth:`flush`."""

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        dimensions: Optional[Dict[str, str]] = None,
        enabled: bool = True,
        client: Any = None,
    ) -> None:
        self.namespace = namespace
        self.dimensions = {k: str(v) for k, v in (dimensions or {}).items() if v is not None}
        self.enabled = bool(enabled and namespace)
        self._client = client
        self._buffer: List[Dict[str, Any]] = []

    # ── construction ──

    @classmethod
    def from_config(cls, config: Dict[str, Any], stage: str, client: Any = None) -> "MetricsEmitter":
        """Build an emitter from the job config.

        ``METRICS_ENABLED: false`` turns the whole thing off; ``METRICS_NAMESPACE``
        and ``METRICS_DIMENSIONS`` let you separate dev from prod on the same account.
        """
        config = config or {}
        dimensions = {"Stage": stage, **(config.get("METRICS_DIMENSIONS") or {})}
        if config.get("ENVIRONMENT"):
            dimensions.setdefault("Environment", config["ENVIRONMENT"])

        # The env var is the operator's kill switch — it lets you silence metrics
        # in a sandbox (or a test run) without editing every config file.
        env_default = os.environ.get("METRICS_ENABLED", "true").strip().lower() not in ("false", "0", "no")

        return cls(
            namespace=config.get("METRICS_NAMESPACE", DEFAULT_NAMESPACE),
            dimensions=dimensions,
            enabled=as_bool(config.get("METRICS_ENABLED"), env_default),
            client=client,
        )

    def _cloudwatch(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("cloudwatch")
        return self._client

    # ── recording ──

    def put(self, name: str, value: float, unit: str = "None", dimensions: Optional[Dict[str, str]] = None) -> None:
        if not self.enabled:
            return
        merged = {**self.dimensions, **{k: str(v) for k, v in (dimensions or {}).items() if v is not None}}
        self._buffer.append({
            "MetricName": name,
            "Value": float(value),
            "Unit": unit,
            "Dimensions": [{"Name": k, "Value": v} for k, v in merged.items()],
        })

    def count(self, name: str, value: float = 1, **dimensions: str) -> None:
        self.put(name, value, unit="Count", dimensions=dimensions or None)

    def gauge(self, name: str, value: float, unit: str = "None", **dimensions: str) -> None:
        self.put(name, value, unit=unit, dimensions=dimensions or None)

    def duration_ms(self, name: str, value: float, **dimensions: str) -> None:
        self.put(name, value, unit="Milliseconds", dimensions=dimensions or None)

    def record_many(self, values: Dict[str, float], unit: str = "Count") -> None:
        """Emit a whole metrics dict — e.g. the counters a stage already returns."""
        for name, value in (values or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                self.put(name, value, unit=unit)

    # ── shipping ──

    def flush(self) -> int:
        """Send buffered metrics. Returns how many were accepted for delivery."""
        if not self.enabled or not self._buffer:
            self._buffer.clear()
            return 0

        pending, self._buffer = self._buffer, []
        sent = 0
        try:
            client = self._cloudwatch()
        except Exception as exc:  # noqa: BLE001 - observability must not break the run
            LOGGER.warning("CloudWatch client unavailable, dropping %d metrics: %s", len(pending), exc)
            return 0

        for start in range(0, len(pending), MAX_ITEMS_PER_CALL):
            chunk = pending[start:start + MAX_ITEMS_PER_CALL]
            try:
                client.put_metric_data(Namespace=self.namespace, MetricData=chunk)
                sent += len(chunk)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("put_metric_data failed for %d metrics: %s", len(chunk), exc)
        return sent

    # ── context manager ──

    def __enter__(self) -> "MetricsEmitter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.flush()
        return False

    @property
    def pending(self) -> List[Dict[str, Any]]:
        """Buffered metrics — exposed for assertions in tests."""
        return list(self._buffer)
