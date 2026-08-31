import json

from kitchen.run import SweepEngine, SweepSpec


class Adapter:
    name = "test"

    def deploy(self, ctx):
        pass

    def launch(self, ctx, point):
        pass

    def analyze(self, ctx, point):
        return {"total_completed": 1, "value": point.trial}


def test_later_trial_invocation_preserves_sweep_results(tmp_path):
    engine = SweepEngine(Adapter(), tmp_path)
    engine.run(SweepSpec(name="exp", dims={"size": (1,)}, rates=(100,),
                         trials=1))
    engine.run(SweepSpec(name="exp", dims={"size": (1,)}, rates=(100,),
                         trials=2, trial_offset=1, resume=True))

    entries = json.loads((tmp_path / "exp" / "sweep_results.json").read_text())
    assert sorted(entry["trial"] for entry in entries) == [0, 1, 2]
    assert sorted(entry["value"] for entry in entries) == [0, 1, 2]
