import pytest
from pathlib import Path
import shutil
import os
import pypipegraph as ppg


# support code to remove test created files
# only if the test suceeded


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def new_pipegraph(request):
    if request.cls is None:
        target_path = Path(request.fspath).parent / "run" / ("." + request.node.name)
    else:
        target_path = (
            Path(request.fspath).parent
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

        def np():
            rc = ppg.resource_coordinators.LocalSystem(1)
            ppg.new_pipegraph(rc, quiet=True, dump_graph=False)
            ppg.util.global_pipegraph.result_dir = Path("results")
            g = ppg.util.global_pipegraph
            g.new_pipegraph = np
            return g

        def finalize():
            if hasattr(request.node, "rep_setup"):
                if request.node.rep_setup.passed and request.node.rep_call.passed:
                    print("executing test succeeded", request.node.nodeid)
                    try:
                        shutil.rmtree(target_path)
                    except OSError:
                        pass

        request.addfinalizer(finalize)
        yield np()

    finally:
        os.chdir(old_dir)
