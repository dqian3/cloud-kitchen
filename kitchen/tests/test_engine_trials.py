import json

from kitchen.run import SweepEngine, SweepSpec


class Adapter:
    name = "test"

    def deploy(self, ctx):
        pass

    def launch(self, ctx, point):
        pass

    def analyze(self, ctx, point):
        return {"total_completed": 1, "value": point.trial,
                "generation": getattr(self, "generation", 0)}


def test_later_trial_invocation_preserves_sweep_results(tmp_path):
    engine = SweepEngine(Adapter(), tmp_path)
    engine.run(SweepSpec(name="exp", dims={"size": (1,)}, rates=(100,),
                         trials=1))
    engine.run(SweepSpec(name="exp", dims={"size": (1,)}, rates=(100,),
                         trials=2, trial_offset=1, resume=True))

    entries = json.loads((tmp_path / "exp" / "sweep_results.json").read_text())
    assert sorted(entry["trial"] for entry in entries) == [0, 1, 2]
    assert sorted(entry["value"] for entry in entries) == [0, 1, 2]


def test_retry_point_overwrites_only_exact_trial(tmp_path):
    adapter = Adapter()
    engine = SweepEngine(adapter, tmp_path)
    base = dict(name="exp", dims={"size": (1, 2)}, rates=(100, 200),
                trials=2)
    engine.run(SweepSpec(**base))
    adapter.generation = 1
    engine.run(SweepSpec(**(base | {
        "trial_offset": 1, "trials": 1, "resume": True,
        "retry_point": {"dims": {"size": 2}, "rate": 100, "trial": 1},
    })))

    entries = json.loads((tmp_path / "exp" / "sweep_results.json").read_text())
    assert len(entries) == 8
    changed = [e for e in entries if e["generation"] == 1]
    assert [(e["size"], e["rate"], e["trial"]) for e in changed] == [
        (2, 100, 1)
    ]
    summary = json.loads((tmp_path / "exp" / "size_2" / "rate_100" /
                          "trial_1" / "summary.json").read_text())
    assert summary["generation"] == 1
