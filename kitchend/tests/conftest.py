"""Keep tests out of the real state directory (~/.cloud-kitchen)."""

import os
import tempfile

# Set before any kitchen module is imported: the state dir is read at import.
os.environ["KITCHEN_STATE_DIR"] = tempfile.mkdtemp(prefix="kitchend-tests-")
