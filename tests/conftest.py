import sys
from pathlib import Path
import os

root = Path(__file__).parent.parent
sys.path.insert(0, str(root / "src"))

# os.environ["PYPIPEGRAPH_NO_LOGGING"] = "1"
from pypipegraph.tests.fixtures import new_pipegraph
