import pytest
from pathlib import Path
import shutil
import os
import pypipegraph as ppg

@pytest.fixture
def new_pipegraph(request):
    if request.cls is None:
        target_path = Path(__file__).parent / "run" / ("." + request.node.name)
    else:
        target_path = (
            Path(__file__).parent
            / "run"
            / (request.cls.__name__ + "." + request.node.name)
        )
    if target_path.exists():
        shutil.rmtree(target_path)
    Path(target_path).mkdir(parents=True, exist_ok=True)
    old_dir = Path(os.getcwd()).absolute()
    try:
        os.chdir(target_path)
        try:
            Path("logs").mkdir(parents=True, exist_ok=True)
            Path("cache").mkdir(parents=True, exist_ok=True)
            Path("results").mkdir(parents=True, exist_ok=True)
            Path("out").mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        rc = ppg.resource_coordinators.LocalSystem(1)

        def np():
            ppg.new_pipegraph(rc, quiet=True)
            ppg.util.global_pipegraph.result_dir = Path("results")
            g = ppg.util.global_pipegraph
            g.new_pipeline = np
            return g

        yield np()
        try:
            # shutil.rmtree(Path(__file__).parent / "run")
            pass
        except OSError:
            pass
    finally:
        os.chdir(old_dir)

