"""The toy protocol end-to-end under LocalRemote, via the CLI entry point."""

import json

from kitchen.events import read_all
from kitchen.run import toy


def test_toy_static_sweep(tmp_path):
    out = tmp_path / "run"
    rc = toy.main([
        "--output-dir", str(out), "--hosts", "2", "--capacity", "4000",
        "--duration-secs", "0.01", "--rates", "1000", "8000",
        "--dim", "payload=16,64",
    ])
    # 8000 > capacity: delivered caps at 4000, still committing → not dead.
    assert rc == 0
    summary = json.loads(
        (out / "toy/payload_16/rate_8000/trial_0/summary.json").read_text())
    assert summary["offered_rate"] == 8000
    assert summary["delivered_rate"] == 4000
    assert summary["num_clients"] == 2
    assert summary["payload"] == 16
    events, _ = read_all(out / "events.jsonl")
    assert [e["type"] for e in events if e["type"].startswith("run.")] == [
        "run.started", "run.phase", "run.phase", "run.finished"]
    assert len([e for e in events if e["type"] == "point.finished"]) == 4


def test_toy_dead_rates_degrade(tmp_path):
    out = tmp_path / "run"
    rc = toy.main([
        "--output-dir", str(out), "--hosts", "1", "--capacity", "4000",
        "--duration-secs", "0.01", "--dead-above", "5000",
        "--rates", "1000", "6000",
    ])
    assert rc == 2
    events, _ = read_all(out / "events.jsonl")
    statuses = [e["data"]["status"] for e in events
                if e["type"] == "point.finished"]
    assert statuses == ["ok", "dead"]


def test_toy_rate_search(tmp_path):
    out = tmp_path / "run"
    rc = toy.main([
        "--output-dir", str(out), "--hosts", "2", "--capacity", "4000",
        "--duration-secs", "0.01", "--rate-search",
        "--rate-search-start", "1000", "--refine-steps", "1",
    ])
    assert rc == 0
    record = json.loads((out / "toy/searched_rates.json").read_text())
    # Climb 1000→2000→4000→8000 (ratio 0.5, saturated), refine midpoint 6000.
    assert record["points"][0]["rates"] == [1000, 2000, 4000, 8000, 6000]
    assert record["points"][0]["complete"] is True
