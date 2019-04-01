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
    import sys

    if request.cls is None:
        target_path = Path(request.fspath).parent / "run" / ("." + request.node.name)
    else:
        target_path = (
            Path(request.fspath).parent
            / "run"
            / (request.cls.__name__ + "." + request.node.name)
        )
    if target_path.exists():  # pragma: no cover
        shutil.rmtree(target_path)
    target_path = target_path.absolute()
    old_dir = Path(os.getcwd()).absolute()
    try:
        first = [False]

        def np():
            if not first[0]:
                Path(target_path).mkdir(parents=True, exist_ok=True)
                os.chdir(target_path)
                Path("logs").mkdir()
                Path("cache").mkdir()
                Path("results").mkdir()
                Path("out").mkdir()
                import logging

                h = logging.getLogger("pypipegraph")
                h.setLevel(logging.WARNING)
                first[0] = True

            rc = ppg.resource_coordinators.LocalSystem(1)
            ppg.new_pipegraph(rc, quiet=True, dump_graph=False)
            ppg.util.global_pipegraph.result_dir = Path("results")
            g = ppg.util.global_pipegraph
            g.new_pipegraph = np
            return g

        def finalize():
            if hasattr(request.node, "rep_setup"):

                if request.node.rep_setup.passed and (
                    request.node.rep_call.passed
                    or request.node.rep_call.outcome == "skipped"
                ):
                    try:
                        if not hasattr(ppg.util.global_pipegraph,
                                       'test_keep_output'):
                            if '--profile' not in sys.argv:
                                shutil.rmtree(target_path)
                    except OSError:  # pragma: no cover
                        pass

        request.addfinalizer(finalize)
        yield np()

    finally:
        os.chdir(old_dir)
