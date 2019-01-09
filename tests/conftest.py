import sys
from pathlib import Path

root = Path(__file__).parent.parent
sys.path.insert(0, str(root / "src"))

from pypipegraph.tests.fixtures import new_pipegraph  # NOQA:F401
