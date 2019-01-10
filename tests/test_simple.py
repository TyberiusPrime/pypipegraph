import os
import pytest
import pypipegraph as ppg
from .shared import assertRaises, write


@pytest.mark.usefixtures("new_pipegraph")
class TestSimple:
    def test_job_creation_before_pipegraph_creation_raises(self):
        ppg.destroy_global_pipegraph()

        def inner():
            ppg.FileGeneratingJob("A", lambda: None)

        assertRaises(ValueError, inner)

    def test_run_pipegraph_without_pipegraph_raises(self):
        ppg.destroy_global_pipegraph()

        def inner():
            ppg.run_pipegraph()

        assertRaises(ValueError, inner)

    def test_can_not_run_twice(self):
        ppg.destroy_global_pipegraph()
        ppg.new_pipegraph(dump_graph=False)
        ppg.run_pipegraph()
        try:
            ppg.run_pipegraph()
            assert False  # "Exception not correctly raised"
        except ValueError as e:
            print(e)
            assert "Each pipegraph may be run only once." in str(e)

    def test_can_not_add_jobs_after_run(self):
        ppg.destroy_global_pipegraph()
        ppg.new_pipegraph(dump_graph=False)
        ppg.run_pipegraph()
        try:
            ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            assert False  # , "Exception not correctly raised")
        except ValueError as e:
            print(e)
            assert (
                "This pipegraph was already run. You need to create a new one for more jobs"
                in str(e)
            )

    def test_job_creation_after_pipegraph_run_raises(self):
        def inner():
            ppg.FileGeneratingJob("A", lambda: None)

        ppg.new_pipegraph(quiet=True, dump_graph=False)
        ppg.run_pipegraph()
        assertRaises(ValueError, inner)

    def test_run_may_be_called_only_once(self):
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        ppg.run_pipegraph()

        def inner():
            ppg.run_pipegraph()

        assertRaises(ValueError, inner)

    def test_non_default_status_filename(self):
        try:
            ppg.forget_job_status("shu.dat")
            ppg.forget_job_status()
            ppg.new_pipegraph(
                quiet=True, invariant_status_filename="shu.dat", dump_graph=False
            )
            ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            ppg.run_pipegraph()
            assert os.path.exists("shu.dat")
            assert not (os.path.exists(ppg.graph.invariant_status_filename_default))
        finally:
            ppg.forget_job_status("shu.dat")
