"""Clock synchronization via chrony, with offset verification and retries."""

import re


def parse_chronyc_offset(stdout):
    """Parse chronyc sources output and return the offset of the selected source in ms.

    chronyc sources lines look like:
      ^* ntp.ubuntu.com  2   6   377   23  +1234us[ +1234us] +/-   12ms
    The selected source is marked with '*'. The offset is in brackets like [+1234us] or [+12ms].
    """
    if not stdout:
        return None
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("^*") and not line.startswith("^+"):
            continue
        # Prefer the selected source (^*)
        if not line.startswith("^*"):
            continue
        # Find bracketed offset like [+1234us] or [-12ms] or [+1234ns]
        m = re.search(r'\[\s*([+-]?\d+(?:\.\d+)?)(ns|us|ms|s)\s*\]', line)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            if unit == 'ns':
                return val / 1e6
            elif unit == 'us':
                return val / 1e3
            elif unit == 'ms':
                return val
            elif unit == 's':
                return val * 1e3
    return None


def sync_clocks(remote, vms, threshold_ms=1.0, max_retries=5):
    """Sync clocks on the given VMs using chrony, retrying until all are under threshold."""
    for attempt in range(1, max_retries + 1):
        print(f"Syncing clocks on {len(vms)} VMs (attempt {attempt}/{max_retries}, threshold={threshold_ms}ms)...")
        results = remote.run_on_all(vms, "sudo chronyc -a 'burst 4/4' && sleep 10 && sudo chronyc -a makestep && sleep 5 && chronyc sources")
        errors = []
        offsets = {}
        for vm in vms:
            r = results.get(vm)
            if isinstance(r, Exception):
                errors.append((vm, r))
            elif r is not None:
                offset = parse_chronyc_offset(r.stdout)
                if offset is not None:
                    offsets[vm] = offset
                else:
                    errors.append((vm, "could not parse chronyc offset"))
        for vm, err in errors:
            print(f"  {vm}: ERROR: {err}")
        ok_count = len(offsets)
        err_count = len(errors)
        print(f"  {ok_count}/{len(vms)} VMs responded OK, {err_count} error(s)")
        diverged = {vm: off for vm, off in offsets.items() if abs(off) > threshold_ms}
        need_retry = bool(errors or diverged)
        if diverged:
            print(f"  {len(diverged)} VM(s) with clock offset > {threshold_ms}ms:")
            for vm in sorted(diverged, key=lambda v: abs(diverged[v]), reverse=True):
                print(f"    {vm}: {diverged[vm]:+.3f}ms")
        if need_retry:
            if attempt < max_retries:
                print("  Retrying...")
                continue
            else:
                print(f"  WARNING: giving up after {max_retries} attempts")
        else:
            max_off = max((abs(v) for v in offsets.values()), default=0)
            print(f"  All {ok_count} VMs synced (max offset: {max_off:.3f}ms)")
        break
    print("Done.")
