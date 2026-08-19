#!/usr/bin/env bash
# Re-record the golden dry-run fixtures from the aspen repo.
# Run after any INTENTIONAL change to the catalog, cost model, or driver
# output; the test compares future runs against these bytes.
set -euo pipefail
cd "$(dirname "$0")"
ASPEN=${ASPEN_REPO:-$HOME/Projects/bft/aspen-bft}
norm() { sed -E 's/[0-9]{8}_[0-9]{6}/TIMESTAMP/g'; }
for name in aspen full; do
  (cd "$ASPEN/scripts/benchmarks" && python3 run_experiment.py $name --dry-run) | norm > "dryrun_${name}.txt"
  echo "recorded dryrun_${name}.txt"
done
# dry-run creates empty run dirs; drop them
rmdir "$ASPEN"/runs/experiment_* 2>/dev/null || true
