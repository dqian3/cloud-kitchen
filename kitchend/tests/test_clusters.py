import asyncio

import pytest
import yaml

from kitchen.remote import MockRemote
from kitchend.config import ClusterConfig, Config, ProjectConfig
from kitchend.core.clusters import ClusterManager, vms_from_yaml
from kitchend.core.db import Db
from kitchend.core.hub import EventHub


def test_vms_from_yaml_aspen_shape(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.dump({
        "replica": {"vms": ["r0", "r1"]},
        "client": {"vms": ["c0"]},
    }))
    assert vms_from_yaml(p) == ["r0", "r1", "c0"]


def test_vms_from_yaml_lazylog_shape(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.dump({
        "durlog": {"vms": ["d0", "d1"]},
        "conslog": {"vm": "cons"},
        "shards": [{"primary_vm": "s0p", "backup_vm": "s0b"}],
        "client": {"vms": ["c0", "c1"]},
    }))
    assert vms_from_yaml(p) == ["d0", "d1", "cons", "s0p", "s0b", "c0", "c1"]


def test_vms_from_yaml_plain_list(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.dump({"vms": ["a", "b"]}))
    assert vms_from_yaml(p) == ["a", "b"]


def test_vms_from_yaml_unknown_shape(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.dump({"what": 1}))
    with pytest.raises(ValueError, match="unrecognized"):
        vms_from_yaml(p)


@pytest.fixture
def manager(tmp_path):
    cfg_yaml = tmp_path / "cluster.yaml"
    cfg_yaml.write_text(yaml.dump({
        "replica": {"vms": ["r0", "r1"]}, "client": {"vms": ["c0"]},
    }))
    project = ProjectConfig(
        name="stub", repo_path=tmp_path,
        clusters=(ClusterConfig(name="main", config="cluster.yaml",
                                hourly_usd=0.5),),
    )
    config = Config(db_path=tmp_path / "db.sqlite3",
                    jobs_dir=tmp_path / "jobs", projects=(project,))
    db = Db(config.db_path)
    hub = EventHub(db)
    mgr = ClusterManager(config, db, hub)
    mc = mgr.clusters["stub/main"]
    mc.remote = MockRemote()
    mc.state.dir.mkdir(parents=True, exist_ok=True)
    # Redirect the cluster state dir into the tmp tree.
    from kitchen.cluster import ClusterState
    mc.state = ClusterState("stub-main", root=tmp_path / "state")
    yield mgr, mc, db
    db.close()


def test_up_then_down(manager):
    mgr, mc, db = manager

    async def main():
        lease_id = await mgr.up("stub/main", ttl_minutes=60)
        assert mc.remote.vm_states == {"r0": "RUNNING", "r1": "RUNNING",
                                       "c0": "RUNNING"}
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "running"
        assert snap["leases"][0]["id"] == lease_id
        assert snap["burn_usd_per_hr"] == pytest.approx(1.5)  # 3 VMs * 0.5

        # Down without force refuses while the lease is live.
        with pytest.raises(RuntimeError, match="live leases"):
            await mgr.down("stub/main")

        mgr.release("stub/main", lease_id)
        await mgr.down("stub/main")
        assert all(s == "TERMINATED" for s in mc.remote.vm_states.values())
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "terminated"
        assert not snap["active"]
        # Session was closed and accrued some (tiny) cost.
        row = db.query_one("SELECT stopped_at FROM cluster_sessions")
        assert row["stopped_at"] is not None

    asyncio.run(main())


def test_lease_expiry_auto_stops(manager, monkeypatch):
    mgr, mc, db = manager
    monkeypatch.setattr(ClusterManager, "TICK_S", 0.05)

    async def main():
        await mgr.up("stub/main", ttl_minutes=1)
        # Fabricate expiry: rewrite the lease with a tiny TTL.
        lease = next(iter(mc.lease_handles.values()))
        lease.renew(ttl_s=0.01)
        await asyncio.sleep(0.5)
        assert all(s == "TERMINATED" for s in mc.remote.vm_states.values())
        assert mc.task is None or mc.task.done()

    asyncio.run(main())


def test_vms_from_yaml_corfu_prefix_shape(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.dump({
        "replica": {"vms": ["r0", "r1", "r2"]},
        "sequencer": {"vm": "seq0"},
        "client": {"vm_prefix": "cl-", "count": 3},
    }))
    assert vms_from_yaml(p) == ["r0", "r1", "r2", "seq0", "cl-0", "cl-1", "cl-2"]


def test_create_runs_setup_script_and_captures_output(tmp_path, manager):
    mgr, mc, db = manager
    mc.create_cmd = ("bash", "-c", "echo creating r0; echo creating r1")
    mc.create_cwd = tmp_path

    async def main():
        await mgr.create("stub/main")
        await mc.create_task
        assert mc.create_rc == 0
        assert mc.create_log == ["creating r0", "creating r1"]
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["create"] == {"running": False, "rc": 0,
                                  "log_tail": ["creating r0", "creating r1"]}
        # start/finish landed in the events audit trail
        types = [r["type"] for r in db.query(
            "SELECT type FROM events ORDER BY id")]
        assert "cluster.create.started" in types
        assert "cluster.create.finished" in types

    asyncio.run(main())


def test_create_guards(manager):
    mgr, mc, db = manager

    async def main():
        # No create_cmd configured -> refused.
        with pytest.raises(ValueError, match="no create_cmd"):
            await mgr.create("stub/main")
        # Refused while the cluster is up under daemon management.
        mc.create_cmd = ("true",)
        await mgr.up("stub/main", ttl_minutes=60)
        with pytest.raises(RuntimeError, match="up under daemon management"):
            await mgr.create("stub/main")
        for lease_id in list(mc.lease_handles):
            mgr.release("stub/main", lease_id)
        await mgr.down("stub/main")

    asyncio.run(main())


def test_failed_start_stops_partial_and_releases_lease(manager):
    mgr, mc, db = manager

    def failing_start(vm_names):
        mc.remote.vm_states["r0"] = "RUNNING"
        raise RuntimeError("r1: zone out of capacity")

    mc.remote.vm_start = failing_start

    async def main():
        with pytest.raises(RuntimeError, match="failed to start"):
            await mgr.up("stub/main", ttl_minutes=60)
        # The started VM was stopped again, the lease released, and the
        # cluster is back to terminated — nothing is held at cost.
        assert mc.remote.vm_states["r0"] == "TERMINATED"
        assert mc.state.live_leases() == []
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "terminated"
        assert not snap["active"]
        row = db.query_one(
            "SELECT released_at FROM cluster_leases ORDER BY id DESC")
        assert row["released_at"] is not None

    asyncio.run(main())


def test_poll_status_flags_unmanaged_vms(manager):
    mgr, mc, db = manager
    mc.remote.vm_states = {"r0": "RUNNING", "r1": "RUNNING", "c0": "TERMINATED"}

    async def main():
        await mgr.poll_status(mc)
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "unmanaged"
        assert snap["vms_running"] == 2
        assert snap["burn_usd_per_hr"] == pytest.approx(1.0)  # 2 up * 0.5
        types = [r["type"] for r in db.query("SELECT type FROM events")]
        assert "cluster.unmanaged" in types

        # VMs gone again -> back to terminated.
        mc.remote.vm_states = {v: "TERMINATED" for v in mc.remote.vm_states}
        await mgr.poll_status(mc)
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "terminated"

    asyncio.run(main())


def test_poll_status_leaves_managed_state_alone(manager):
    mgr, mc, db = manager

    async def main():
        lease_id = await mgr.up("stub/main", ttl_minutes=60)
        await mgr.poll_status(mc)
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "running"
        mgr.release("stub/main", lease_id)
        await mgr.down("stub/main")

    asyncio.run(main())
