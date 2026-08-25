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
        mc.remote.vm_states = {"r0": "TERMINATED", "r1": "TERMINATED",
                               "c0": "TERMINATED"}
        await mgr.create("stub/main")
        await mc.create_task
        assert mc.create_rc == 0
        assert [l for l in mc.create_log if not l.startswith("[daemon]")] == \
            ["creating r0", "creating r1"]
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        cr = snap["create"]
        assert (cr["running"], cr["rc"], cr["attempt"], cr["missing"]) == \
            (False, 0, 1, [])
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


def test_poll_status_ignores_vms_leased_by_an_overlapping_cluster(manager):
    mgr, mc, db = manager
    # A second cluster whose fleet includes this one's VMs, currently up.
    import copy
    other = copy.copy(mc)
    other.key = "stub/big"; other.name = "big"
    other.vms = ["r0", "r1", "c0", "x0"]

    async def main():
        other.task = asyncio.get_running_loop().create_task(asyncio.sleep(3600))
        mgr.clusters["stub/big"] = other
        mc.remote.vm_states = {"r0": "RUNNING", "r1": "RUNNING", "c0": "RUNNING"}
        await mgr.poll_status(mc)
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "terminated"      # not flagged unmanaged
        other.task.cancel()

    asyncio.run(main())


def test_create_retries_until_fleet_complete(manager):
    mgr, mc, db = manager
    mc.create_cmd = ("true",)

    async def main():
        # First attempt: c0 never appears (stockout); r0 came up running.
        mc.remote.vm_states = {"r0": "RUNNING", "r1": "TERMINATED"}
        await mgr.create("stub/main", retry_delay_s=0.2, max_attempts=3)
        while mc.create_attempt < 1 or mc.create_next_at is None:
            await asyncio.sleep(0.02)
        assert mc.create_missing == ["c0"]
        assert mc.remote.vm_states["r0"] == "TERMINATED"   # stopped after
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["create"]["running"] and snap["create"]["attempt"] == 1
        # Capacity returns before the next attempt.
        mc.remote.vm_states["c0"] = "RUNNING"
        await mc.create_task
        assert mc.create_attempt == 2 and mc.create_missing == []
        assert mc.remote.vm_states["c0"] == "TERMINATED"
        types = [r["type"] for r in db.query("SELECT type FROM events")]
        assert types.count("cluster.create.attempt") == 2

    asyncio.run(main())


def test_create_gives_up_after_max_attempts_and_can_be_canceled(manager):
    mgr, mc, db = manager
    mc.create_cmd = ("true",)

    async def main():
        mc.remote.vm_states = {"r0": "TERMINATED"}   # r1, c0 never appear
        await mgr.create("stub/main", retry_delay_s=0.05, max_attempts=2)
        await mc.create_task
        assert mc.create_attempt == 2 and sorted(mc.create_missing) == ["c0", "r1"]

        await mgr.create("stub/main", retry_delay_s=60, max_attempts=5)
        while mc.create_next_at is None:
            await asyncio.sleep(0.02)
        mgr.cancel_create("stub/main")
        try:
            await mc.create_task
        except asyncio.CancelledError:
            pass
        assert mc.create_task.cancelled()

    asyncio.run(main())


def test_registry_picks_docker_backend_from_yaml(tmp_path):
    from kitchen.remote import DockerRemote

    compose = tmp_path / "docker-compose.yml"
    compose.write_text("services: {r0: {image: x}, c0: {image: x}}\n")
    cfg_yaml = tmp_path / "docker.yaml"
    cfg_yaml.write_text(yaml.dump({
        "platform": "docker", "compose_file": "docker-compose.yml",
        "replica": {"vms": ["r0"]}, "client": {"vms": ["c0"]},
    }))
    project = ProjectConfig(
        name="stub", repo_path=tmp_path,
        clusters=(ClusterConfig(name="docker", config="docker.yaml"),))
    config = Config(db_path=tmp_path / "db.sqlite3",
                    jobs_dir=tmp_path / "jobs", projects=(project,))
    db = Db(config.db_path)
    mgr = ClusterManager(config, db, EventHub(db))
    mc = mgr.clusters["stub/docker"]
    assert isinstance(mc.remote, DockerRemote)
    assert mc.remote.compose_file == str(compose)
    assert mgr.estimate_hourly("stub/docker") is None     # free
    db.close()


def test_hold_keeps_cluster_up_after_last_lease(manager, monkeypatch):
    mgr, mc, db = manager
    monkeypatch.setattr(ClusterManager, "TICK_S", 0.05)

    async def main():
        lease_id = await mgr.up("stub/main", ttl_minutes=60)
        mgr.hold("stub/main", 0.5, for_job=42)
        mgr.release("stub/main", lease_id)
        await asyncio.sleep(0.2)
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "running" and snap["hold_for"] == 42
        await asyncio.sleep(0.6)      # the hold lapses with no new lease
        snap = [s for s in mgr.snapshot() if s["key"] == "stub/main"][0]
        assert snap["state"] == "terminated"

    asyncio.run(main())


def test_poll_interval_follows_cluster_state(manager):
    mgr, mc, db = manager
    assert mgr.poll_interval(mc) == ClusterManager.POLL_IDLE_S
    db.execute("UPDATE clusters SET state = 'starting' WHERE id = ?", (mc.db_id,))
    assert mgr.poll_interval(mc) == ClusterManager.POLL_TRANSITIONAL_S
    db.execute("UPDATE clusters SET state = 'running' WHERE id = ?", (mc.db_id,))
    assert mgr.poll_interval(mc) == ClusterManager.POLL_RUNNING_S
    db.execute("UPDATE clusters SET state = 'unmanaged' WHERE id = ?", (mc.db_id,))
    assert mgr.poll_interval(mc) == ClusterManager.POLL_RUNNING_S
