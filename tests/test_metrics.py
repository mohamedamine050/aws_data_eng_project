"""Unit tests for common.metrics — the CloudWatch emitter."""

from common.metrics import DEFAULT_NAMESPACE, MAX_ITEMS_PER_CALL, MetricsEmitter


class FakeCloudWatch:
    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def put_metric_data(self, Namespace, MetricData):
        self.calls.append({"Namespace": Namespace, "MetricData": MetricData})
        if self.fail_on is not None and len(self.calls) == self.fail_on:
            raise RuntimeError("throttled")


def _emitter(**kwargs):
    client = kwargs.pop("client", FakeCloudWatch())
    return MetricsEmitter(dimensions={"Stage": "test"}, client=client, **kwargs), client


# ── RECORDING ────────────────────────────────────────────────

def test_counts_and_gauges_are_buffered_until_flush():
    metrics, client = _emitter()
    metrics.count("RecordsIn", 5)
    metrics.gauge("QualityPct", 99.5, unit="Percent")

    assert len(metrics.pending) == 2
    assert client.calls == []

    assert metrics.flush() == 2
    assert client.calls[0]["Namespace"] == DEFAULT_NAMESPACE
    assert metrics.pending == []


def test_dimensions_merge_base_and_per_metric():
    metrics, client = _emitter()
    metrics.count("EventsByType", 3, EventType="order_placed")
    metrics.flush()

    dimensions = {d["Name"]: d["Value"] for d in client.calls[0]["MetricData"][0]["Dimensions"]}
    assert dimensions == {"Stage": "test", "EventType": "order_placed"}


def test_record_many_skips_non_numeric_values():
    metrics, client = _emitter()
    metrics.record_many({"a": 1, "b": 2.5, "c": "text", "d": True, "e": None})
    metrics.flush()

    names = {item["MetricName"] for item in client.calls[0]["MetricData"]}
    assert names == {"a", "b"}


def test_units_are_carried_through():
    metrics, client = _emitter()
    metrics.duration_ms("JobDuration", 1234)
    metrics.flush()

    assert client.calls[0]["MetricData"][0]["Unit"] == "Milliseconds"


# ── CHUNKING ─────────────────────────────────────────────────

def test_flush_chunks_at_the_api_limit():
    metrics, client = _emitter()
    for index in range(MAX_ITEMS_PER_CALL * 2 + 3):
        metrics.count(f"M{index}")

    sent = metrics.flush()

    assert sent == MAX_ITEMS_PER_CALL * 2 + 3
    assert [len(call["MetricData"]) for call in client.calls] == [MAX_ITEMS_PER_CALL, MAX_ITEMS_PER_CALL, 3]


# ── FAILURE TOLERANCE ────────────────────────────────────────

def test_a_failing_chunk_does_not_raise_or_stop_the_rest():
    client = FakeCloudWatch(fail_on=1)
    metrics = MetricsEmitter(client=client)
    for index in range(MAX_ITEMS_PER_CALL + 5):
        metrics.count(f"M{index}")

    sent = metrics.flush()

    assert sent == 5           # first chunk failed, second went through
    assert len(client.calls) == 2


def test_disabled_emitter_records_and_sends_nothing():
    client = FakeCloudWatch()
    metrics = MetricsEmitter(enabled=False, client=client)
    metrics.count("RecordsIn", 5)

    assert metrics.pending == []
    assert metrics.flush() == 0
    assert client.calls == []


def test_empty_namespace_disables_the_emitter():
    metrics = MetricsEmitter(namespace="", client=FakeCloudWatch())
    metrics.count("RecordsIn", 5)
    assert metrics.pending == []


# ── CONFIG ───────────────────────────────────────────────────

def test_from_config_reads_namespace_and_dimensions():
    metrics = MetricsEmitter.from_config(
        {"METRICS_NAMESPACE": "Custom/NS", "ENVIRONMENT": "prod",
         "METRICS_DIMENSIONS": {"Team": "data"}, "METRICS_ENABLED": True},
        stage="producer",
        client=FakeCloudWatch(),
    )

    assert metrics.namespace == "Custom/NS"
    assert metrics.dimensions == {"Stage": "producer", "Team": "data", "Environment": "prod"}


def test_from_config_can_disable():
    metrics = MetricsEmitter.from_config({"METRICS_ENABLED": False}, stage="producer")
    assert metrics.enabled is False


def test_env_var_is_the_kill_switch(monkeypatch):
    monkeypatch.setenv("METRICS_ENABLED", "false")
    assert MetricsEmitter.from_config({}, stage="producer").enabled is False

    monkeypatch.setenv("METRICS_ENABLED", "true")
    assert MetricsEmitter.from_config({}, stage="producer").enabled is True


def test_config_wins_over_the_env_var(monkeypatch):
    monkeypatch.setenv("METRICS_ENABLED", "false")
    assert MetricsEmitter.from_config({"METRICS_ENABLED": True}, stage="producer").enabled is True


# ── CONTEXT MANAGER ──────────────────────────────────────────

def test_context_manager_flushes_on_exit():
    client = FakeCloudWatch()
    with MetricsEmitter(client=client) as metrics:
        metrics.count("RecordsIn", 1)

    assert len(client.calls) == 1
