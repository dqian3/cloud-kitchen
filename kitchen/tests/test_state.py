import json
import time

import pytest

from kitchen.cluster import ClusterState


def test_lease_lifecycle(tmp_path):
    state = ClusterState("c1", root=tmp_path)
    with state.acquire_lease("sweep", ttl_s=100) as lease:
        live = state.live_leases()
        assert [l.purpose for l in live] == ["sweep"]
        assert live[0].pid == lease.info.pid
    assert state.live_leases() == []


def test_expired_lease_is_garbage_collected(tmp_path):
    state = ClusterState("c1", root=tmp_path)
    lease = state.acquire_lease("old", ttl_s=0.001)
    time.sleep(0.01)
    assert state.live_leases() == []
    assert not lease.info.path.exists()


def test_dead_pid_lease_is_garbage_collected(tmp_path):
    state = ClusterState("c1", root=tmp_path)
    lease = state.acquire_lease("ghost", ttl_s=1000)
    data = json.loads(lease.info.path.read_text())
    data["pid"] = 2 ** 22 + 1  # beyond default pid_max: never a live pid
    lease.info.path.write_text(json.dumps(data))
    assert state.live_leases() == []


def test_corrupt_lease_file_is_removed(tmp_path):
    state = ClusterState("c1", root=tmp_path)
    bad = state.leases_dir / "junk.json"
    bad.write_text("{not json")
    assert state.live_leases() == []
    assert not bad.exists()


def test_renew_extends_lease(tmp_path):
    state = ClusterState("c1", root=tmp_path)
    lease = state.acquire_lease("sweep", ttl_s=100)
    before = state.live_leases()[0].expires_at
    time.sleep(0.01)
    lease.renew()
    after = state.live_leases()[0].expires_at
    assert after > before
    lease.release()


def test_bad_cluster_name_rejected(tmp_path):
    with pytest.raises(ValueError):
        ClusterState("", root=tmp_path)
    with pytest.raises(ValueError):
        ClusterState("../evil", root=tmp_path)
