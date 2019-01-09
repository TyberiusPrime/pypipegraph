"""
The MIT License (MIT)

Copyright (c) 2012, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import time
import sys

sys.path.append("../../")
import pypipegraph as ppg

logger = ppg.util.start_logging("test")
import os
import shutil
import subprocess
import hashlib
from six.moves import xrange
import stat
import platform
import pytest


# rc_gen = lambda : ppg.resource_coordinators.LocalTwisted()
rc_gen = lambda: ppg.resource_coordinators.LocalSystem()  # noqa:E731
test_count = 0


def read(filename):
    """simply read a file"""
    op = open(filename)
    data = op.read()
    op.close()
    return data


def write(filename, string):
    """open file for writing, dump string, close file"""
    op = open(filename, "w")
    op.write(string)
    op.close()


def append(filename, string):
    """open file for appending, dump string, close file"""
    op = open(filename, "a")
    op.write(string)
    op.close()


def writeappend(filename_write, filename_append, string):
    write(filename_write, string)
    append(filename_append, string)


def assertRaises(exception, func):
    with pytest.raises(exception):
        func()


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


@pytest.mark.usefixtures("new_pipegraph")
class TestCycles:
    def test_simple_cycle(self):
        def inner():
            ppg.new_pipegraph(quiet=True, dump_graph=False)
            jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
            jobB = ppg.FileGeneratingJob("A", lambda: write("B", "A"))
            jobA.depends_on(jobB)
            jobB.depends_on(jobA)
            # ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    def test_indirect_cicle(self):
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda: write("B", "A"))
        jobC = ppg.FileGeneratingJob("C", lambda: write("C", "A"))
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        jobA.depends_on(jobC)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    def test_exceeding_max_cycle(self):
        max_depth = 50
        # this raisess...
        jobs = []
        for x in xrange(0, max_depth - 1):
            j = ppg.FileGeneratingJob(str(x), lambda: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobs = []
        for x in xrange(0, max_depth + 10):
            j = ppg.FileGeneratingJob(str(x), lambda: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    @pytest.mark.xfail
    def test_prioritize_simple(self):
        raise NotImplementedError()
        jobA = ppg.Job("A")
        jobB = ppg.Job("B")
        jobA.depends_on(jobB)
        jobC = ppg.Job("C")
        jobD = ppg.Job("D")
        jobC.depends_on(jobD)
        ppg.util.global_pipegraph.connect_graph()
        ppg.util.global_pipegraph.check_cycles()
        assert jobD in ppg.util.global_pipegraph.possible_execution_order
        if jobD == ppg.util.global_pipegraph.possible_execution_order[0]:
            to_prio = jobB
        else:
            to_prio = jobD
        assert not (to_prio == ppg.util.global_pipegraph.possible_execution_order[0])
        ppg.util.global_pipegraph.prioritize(to_prio)

        assert to_prio == ppg.util.global_pipegraph.possible_execution_order[0]

        ppg.util.global_pipegraph.prioritize(jobB)
        print("after prio b")
        for x in ppg.util.global_pipegraph.possible_execution_order:
            print(x.job_id)

        assert jobB == ppg.util.global_pipegraph.possible_execution_order[0]
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)
        ppg.util.global_pipegraph.prioritize(jobC)
        print("after prio c")
        for x in ppg.util.global_pipegraph.possible_execution_order:
            print(x.job_id)

        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobC
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobD)

        assert jobD == ppg.util.global_pipegraph.possible_execution_order[0]
        assert jobC == ppg.util.global_pipegraph.possible_execution_order[1]

        ppg.util.global_pipegraph.prioritize(jobB)
        assert jobB == ppg.util.global_pipegraph.possible_execution_order[0]
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)

    def test_prioritize_raises_on_done_job(self):
        def dump():
            pass

        ppg.FileGeneratingJob("out/A", dump)
        jobB = ppg.FileGeneratingJob("out/B", dump)
        jobB.ignore_code_changes()
        with open("out/B", "wb") as op:
            op.write(b"Done")
        ppg.util.global_pipegraph.connect_graph()
        ppg.util.global_pipegraph.check_cycles()
        ppg.util.global_pipegraph.load_invariant_status()
        ppg.util.global_pipegraph.distribute_invariant_changes()
        ppg.util.global_pipegraph.dump_invariant_status()  # the jobs will have removed their output, so we can safely store the invariant data
        ppg.util.global_pipegraph.build_todo_list()

        def inner():
            ppg.util.global_pipegraph.prioritize(jobB)

        assertRaises(ValueError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class TestJobs:
    def test_assert_singletonicity_of_jobs(self):
        ppg.forget_job_status()
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        job2 = ppg.FileGeneratingJob(of, do_write)
        assert job is job2

    def test_redifining_a_jobid_with_different_class_raises(self):
        ppg.forget_job_status()
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)

        ppg.FileGeneratingJob(of, do_write)

        def load():
            return "shu"

        def inner():
            ppg.DataLoadingJob(of, load)

        assertRaises(ValueError, inner)

    def test_addition(self):
        def write_func(of):
            def do_write():
                write(of, "do_write done")

            return of, do_write

        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(*write_func("out/a"))
        jobB = ppg.FileGeneratingJob(*write_func("out/b"))
        jobC = ppg.FileGeneratingJob(*write_func("out/c"))
        jobD = ppg.FileGeneratingJob(*write_func("out/d"))

        aAndB = jobA + jobB
        assert len(aAndB) == 2
        assert jobA in aAndB
        assert jobB in aAndB

        aAndBandC = aAndB + jobC
        assert jobA in aAndBandC
        assert jobB in aAndBandC
        assert jobC in aAndBandC

        aAndBAndD = jobD + aAndB
        assert jobA in aAndBAndD
        assert jobB in aAndBAndD
        assert jobD in aAndBAndD

        cAndD = jobC + jobD
        all = aAndB + cAndD
        assert len(all) == 4
        assert jobA in all
        assert jobB in all
        assert jobC in all
        assert jobD in all

    def test_raises_on_non_str_job_id(self):
        def inner():
            ppg.FileGeneratingJob(1234, lambda: None)

        assertRaises(ValueError, inner)

    def test_equality_is_identity(self):
        def write_func(of):
            def do_write():
                write(of, "do_write done")

            return of, do_write

        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(*write_func("out/a"))
        jobA1 = ppg.FileGeneratingJob(*write_func("out/a"))
        jobB = ppg.FileGeneratingJob(*write_func("out/b"))
        assert jobA is jobA1
        assert jobA == jobA1
        assert not (jobA == jobB)

    def test_has_hash(self):
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob("out/", lambda: None)
        assert hasattr(jobA, "__hash__")


@pytest.mark.usefixtures("new_pipegraph")
class TestJobs2:
    def test_ignore_code_changes_raises(self):
        jobA = ppg.Job("shu")

        def inner():
            jobA.ignore_code_changes()

        assertRaises(ValueError, inner)

    def test_load_raises(self):
        jobA = ppg.Job("shu")

        def inner():
            jobA.load()

        assertRaises(ValueError, inner)

    def test_is_in_dependency_chain_direct(self):
        jobA = ppg.Job("A")
        jobB = ppg.Job("B")
        jobA.depends_on(jobB)
        assert jobA.is_in_dependency_chain(jobB, 100)

    def test_is_in_dependency_chain_direct2(self):
        jobA = ppg.Job("A")
        jobs = []
        for x in xrange(0, 10):
            j = ppg.Job(str(x))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobA.depends_on(jobs[-1])
        assert jobA.is_in_dependency_chain(jobs[0], 100)
        assert jobA.is_in_dependency_chain(jobs[0], 10)
        # max_depth reached -> answer with false
        assert not (jobA.is_in_dependency_chain(jobs[0], 5))

    def test_str(self):
        a = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "hello"))
        assert isinstance(str(a), str)

        a = ppg.ParameterInvariant("out/A", "hello")
        assert isinstance(str(a), str)

        a = ppg.JobGeneratingJob("out/Ax", lambda: "hello")
        assert isinstance(str(a), str)


@pytest.mark.usefixtures("new_pipegraph")
class TestFileGeneratingJob:
    def test_basic(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        assert not (job.failed)
        assert os.path.exists(of)
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.was_run

    def test_basic_with_parameter(self):
        data_to_write = "hello"

        def do_write(filename):
            print("do_write was called")
            write(filename, data_to_write)

        job = ppg.FileGeneratingJob("out/a", do_write)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        assert not (job.failed)
        assert os.path.exists("out/a")
        op = open("out/a", "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.was_run

    def test_simple_filegeneration_with_function_dependency(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        # job.ignore_code_changes() this would be the magic line to remove the function dependency
        ppg.run_pipegraph()
        assert not (job.failed)
        assert os.path.exists(of)
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write

    def test_filejob_raises_if_no_data_is_written(self):
        of = "out/a"

        def do_write():
            pass

        job = ppg.FileGeneratingJob(of, do_write)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.RuntimeError, inner)
        assert job.failed
        assert isinstance(job.exception, ppg.JobContractError)
        assert not (os.path.exists(of))

    def test_simple_filegeneration_removes_file_on_exception(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)
            raise ValueError("shu")

        job = ppg.FileGeneratingJob(of, do_write)
        try:
            ppg.run_pipegraph()
            raise ValueError("should have raised RuntimeError")
        except ppg.RuntimeError:
            pass
        assert job.failed
        assert not (os.path.exists(of))
        assert isinstance(job.exception, ValueError)

    def test_simple_filegeneration_renames_file_on_exception(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)
            raise ValueError("shu")

        job = ppg.FileGeneratingJob(of, do_write, rename_broken=True)
        try:
            ppg.run_pipegraph()
            raise ValueError("should have raised RuntimeError")
        except ppg.RuntimeError:
            pass
        assert job.failed
        assert not (os.path.exists(of))
        assert os.path.exists(of + ".broken")
        assert isinstance(job.exception, ValueError)

    def test_simple_filegeneration_captures_stdout_stderr(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            op = open(of, "w")
            op.write(data_to_write)
            op.close()
            print("stdout is cool")
            sys.stderr.write("I am stderr")

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert not (job.failed)
        assert os.path.exists(of)
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.stdout == "stdout is cool\n"
        assert job.stderr == "I am stderr"  # no \n here

    def test_filegeneration_does_not_change_mcp(self):
        global global_test
        global_test = 1
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)
            global global_test
            global_test = 2

        ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert global_test == 1

    def test_file_generation_chaining_simple(self):
        ofA = "out/a"

        def writeA():
            write(ofA, "Hello")

        jobA = ppg.FileGeneratingJob(ofA, writeA)
        ofB = "out/b"

        def writeB():
            op = open(ofB, "w")
            ip = open(ofA, "r")
            op.write(ip.read()[::-1])
            op.close()
            ip.close()

        jobB = ppg.FileGeneratingJob(ofB, writeB)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        assert read(ofA) == read(ofB)[::-1]

    def test_file_generation_multicore(self):
        # one fork per FileGeneratingJob...
        ofA = "out/a"

        def writeA():
            write(ofA, "%i" % os.getpid())

        ofB = "out/b"

        def writeB():
            write(ofB, "%i" % os.getpid())

        ppg.FileGeneratingJob(ofA, writeA)
        ppg.FileGeneratingJob(ofB, writeB)
        ppg.run_pipegraph()
        assert read(ofA) != read(ofB)

    def test_invaliding_removes_file(self):
        of = "out/a"
        sentinel = "out/b"

        def do_write():
            if os.path.exists(sentinel):
                raise ValueError("second run")
            write(of, "shu")
            write(sentinel, "done")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.ParameterInvariant("my_params", (1,))
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert os.path.exists(of)
        assert os.path.exists(sentinel)

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.ParameterInvariant(
            "my_params", (2,)
        )  # same name ,changed params, job needs to rerun, but explodes...
        job.depends_on(dep)  # on average, half the mistakes are in the tests...
        try:
            ppg.run_pipegraph()
            raise ValueError("Should not have been reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists(of))

    def test_passing_non_function(self):
        def inner():
            ppg.FileGeneratingJob("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.FileGeneratingJob(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_exceptions_are_preserved(self):
        def shu():
            write("out/A", "A")
            write("out/Ay", "ax")
            raise IndexError("twenty-five")  # just some exception

        jobA = ppg.FileGeneratingJob("out/A", shu)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.RuntimeError, inner)
        print(jobA.exception)
        assert isinstance(jobA.exception, IndexError)
        assert not (
            os.path.exists("out/A")
        )  # should clobber the resulting files in this case - just a double check to test_invaliding_removes_file
        assert read("out/Ay") == "ax"  # but the job did run, right?

    def test_dumping_graph(self):
        ppg.new_pipegraph(
            quiet=True, invariant_status_filename="shu.dat", dump_graph=True
        )
        ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        ppg.run_pipegraph()
        pid = ppg.util.global_pipegraph.dump_pid
        os.waitpid(pid, 0)
        print(os.listdir("logs"))
        assert os.path.exists("logs/ppg_graph.gml")


@pytest.mark.usefixtures("new_pipegraph")
class TestMultiFileGeneratingJob:
    def test_basic(self):
        of = ["out/a", "out/b"]

        def do_write():
            for f in of:
                append(f, "shu")

        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            assert read(f) == "shu"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            assert read(f) == "shu"  # ie. job has net been rerun...
        # but if I now delete one...
        os.unlink(of[0])
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of[0]) == "shu"
        assert (
            read(of[1]) == "shu"
        )  # Since that file was also deleted when MultiFileGeneratingJob was invalidated...

    def test_exception_destroys_all_files(self):
        of = ["out/a", "out/b"]

        def do_write():
            for f in of:
                append(f, "shu")
            raise ValueError("explode")

        ppg.MultiFileGeneratingJob(of, do_write)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            assert not (os.path.exists(f))

    def test_exception_destroys_renames_files(self):
        of = ["out/a", "out/b"]

        def do_write():
            for f in of:
                append(f, "shu")
            raise ValueError("explode")

        ppg.MultiFileGeneratingJob(of, do_write, rename_broken=True)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            assert os.path.exists(f + ".broken")

    def test_invalidation_removes_all_files(self):
        of = ["out/a", "out/b"]
        sentinel = (
            "out/sentinel"
        )  # hack so this one does something different the second time around...

        def do_write():
            if os.path.exists(sentinel):
                raise ValueError("explode")
            write(sentinel, "shu")
            for f in of:
                append(f, "shu")

        ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant("myparam", (1,))
        )
        ppg.run_pipegraph()
        for f in of:
            assert os.path.exists(f)
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant("myparam", (2,))
        )
        try:
            ppg.run_pipegraph()  # since this should blow up
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            assert not (os.path.exists(f))

    def test_passing_not_a_list_of_str(self):
        def inner():
            ppg.MultiFileGeneratingJob("out/a", lambda: 1)

        assertRaises(ValueError, inner)

    def test_passing_non_function(self):
        def inner():
            ppg.MultiFileGeneratingJob(["out/a"], "shu")

        assertRaises(ValueError, inner)

    def test_exceptions_are_preserved(self):
        def shu():
            write("out/A", "A")
            write("out/B", "B")
            write("out/Az", "ax")
            raise IndexError("twenty-five")  # just some exception

        jobA = ppg.MultiFileGeneratingJob(["out/A", "out/B"], shu)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.RuntimeError, inner)
        assert isinstance(jobA.exception, IndexError)
        assert not (
            os.path.exists("out/A")
        )  # should clobber the resulting files in this case - just a double check to test_invaliding_removes_file
        assert not (
            os.path.exists("out/B")
        )  # should clobber the resulting files in this case - just a double check to test_invaliding_removes_file
        assert read("out/Az") == "ax"  # but the job did run, right?

    def raises_on_non_string_filnames(self):
        def inner():
            ppg.MultiFileGeneratingJob(["one", 2], lambda: write("out/A"))

        assertRaises(ValueError, inner)

    def test_raises_on_collision(self):
        def inner():
            ppg.MultiFileGeneratingJob(["test1", "test2"], lambda: 5)
            ppg.MultiFileGeneratingJob(["test2", "test3"], lambda: 5)

        assertRaises(ValueError, inner)

    def test_duplicate_prevention(self):
        param = "A"
        ppg.FileGeneratingJob("out/A", lambda: write("out/A", param))

        def inner():
            ppg.MultiFileGeneratingJob(["out/A"], lambda: write("out/A", param))

        assertRaises(ValueError, inner)

    def test_non_str(self):
        param = "A"

        def inner():
            ppg.MultiFileGeneratingJob([25], lambda: write("out/A", param))

        assertRaises(TypeError, inner)

    def test_non_iterable(self):
        param = "A"
        try:
            ppg.MultiFileGeneratingJob(25, lambda: write("out/A", param))
            assert not ("Exception not raised")
        except TypeError as e:
            print(e)
            assert "filenames was not iterable" in str(e)

    def test_single_stre(self):
        param = "A"

        def inner():
            ppg.MultiFileGeneratingJob("A", lambda: write("out/A", param))

        assertRaises(ValueError, inner)


test_modifies_shared_global = []


@pytest.mark.usefixtures("new_pipegraph")
class TestDataLoadingJob:
    def test_modifies_slave(self):
        # global shared
        # shared = "I was the the global in the mcp"
        def load():
            test_modifies_shared_global.append("shared data")

        of = "out/a"

        def do_write():
            write(
                of, "\n".join(test_modifies_shared_global)
            )  # this might actually be a problem when defining this?

        dlJo = ppg.DataLoadingJob("myjob", load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)
        ppg.run_pipegraph()
        assert read(of) == "shared data"

    def test_global_statement_works(self):
        # this currently does not work in the cloudpickle transmitted jobs -
        # two jobs refereing to global have different globals afterwards
        # or the 'global shared' does not work as expected after loading
        global shared
        shared = "I was the the global in the mcp"

        def load():
            global shared
            shared = "shared data"

        of = "out/a"

        def do_write():
            write(of, shared)

        dlJo = ppg.DataLoadingJob("myjob", load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)
        ppg.run_pipegraph()
        assert read(of) == "shared data"

    def test_does_not_get_run_without_dep_job(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable

        ppg.DataLoadingJob("myjob", load)
        ppg.run_pipegraph()
        assert not (os.path.exists(of))

    def test_does_not_get_run_in_chain_without_final_dep(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            write(ofB, "sha")

        ppg.DataLoadingJob("myjobB", loadB).depends_on(job)
        ppg.run_pipegraph()
        assert not (os.path.exists(of))
        assert not (os.path.exists(ofB))

    def test_does_get_run_in_chain_all(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            write(ofB, "sha")

        jobB = ppg.DataLoadingJob("myjobB", loadB).depends_on(job)
        ofC = "out/c"

        def do_write():
            write(ofC, ofC)

        ppg.FileGeneratingJob(ofC, do_write).depends_on(jobB)
        ppg.run_pipegraph()
        assert os.path.exists(of)
        assert os.path.exists(ofB)
        assert os.path.exists(ofC)

    def test_chain_with_filegenerating_works(self):
        of = "out/a"

        def do_write():
            write(of, of)

        jobA = ppg.FileGeneratingJob(of, do_write)
        o = Dummy()

        def do_load():
            o.a = read(of)

        jobB = ppg.DataLoadingJob("loadme", do_load).depends_on(jobA)
        ofB = "out/b"

        def write2():
            write(ofB, o.a)

        ppg.FileGeneratingJob(ofB, write2).depends_on(jobB)
        ppg.run_pipegraph()
        assert read(of) == of
        assert read(ofB) == of

    def test_does_get_run_depending_on_jobgenjob(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable

        job = ppg.DataLoadingJob("myjob", load)

        def gen():
            ofB = "out/b"

            def do_write():
                write(ofB, "hello")

            ppg.FileGeneratingJob(ofB, do_write)

        ppg.JobGeneratingJob("mygen", gen).depends_on(job)
        ppg.run_pipegraph()
        assert os.path.exists(of)  # so the data loading job was run
        assert read("out/b") == "hello"  # and so was the jobgen and filegen job.

    def test_passing_non_function(self):
        def inner():
            ppg.DataLoadingJob("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.DataLoadingJob(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_failing_dataloading_jobs(self):
        o = Dummy()
        of = "out/A"

        def write():
            write(of, o.a)

        def load():
            o.a = "shu"
            raise ValueError()

        job_fg = ppg.FileGeneratingJob(of, write)
        job_dl = ppg.DataLoadingJob("doload", load)
        job_fg.depends_on(job_dl)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists(of))
        assert job_dl.failed
        assert job_fg.failed
        assert isinstance(job_dl.exception, ValueError)

    def test_prev_dataloading_jobs_not_done_if_there_is_a_non_dataloading_job_inbetween_that_is_done(
        self
    ):
        # so, A = DataLoadingJob, B = FileGeneratingJob, C = DataLoadingJob, D = FileGeneratingJob
        # D.depends_on(C)
        # C.depends_on(B)
        # B.depends_on(A)
        # B is done.
        # D is not
        # since a would be loaded, and then cleaned up right away (because B is Done)
        # it should never be loaded
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")

        def b():
            append("out/B", "B")
            append("out/Bx", "B")

        def c():
            o.c = "C"
            append("out/C", "C")

        def d():
            append("out/D", "D")
            append("out/Dx", "D")

        jobA = ppg.DataLoadingJob("out/A", a)
        jobB = ppg.FileGeneratingJob("out/B", b)
        jobC = ppg.DataLoadingJob("out/C", c)
        jobD = ppg.FileGeneratingJob("out/D", d)
        jobD.depends_on(jobC)
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        jobA.ignore_code_changes()
        jobB.ignore_code_changes()
        jobC.ignore_code_changes()
        jobD.ignore_code_changes()
        write("out/B", "already done")
        assert not (os.path.exists("out/D"))
        ppg.run_pipegraph()
        assert read("out/D") == "D"
        assert read("out/Dx") == "D"
        assert not (
            os.path.exists("out/A")
        )  # A was not executed (as per the premise of the test)
        assert not (
            os.path.exists("out/Bx")
        )  # so B was not executed (we removed the function invariants for this test)
        assert read("out/C") == "C"

    def test_sending_a_non_pickable_exception(self):
        class UnpickableException(Exception):
            def __getstate__(self):
                raise ValueError("Can't pickle me")

        def load():
            raise UnpickableException()

        jobA = ppg.DataLoadingJob("out/A", load)
        jobB = ppg.FileGeneratingJob("out/B", lambda: True)
        jobB.depends_on(jobA)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.RuntimeError, inner)
        print(jobA.exception)
        assert isinstance(jobA.exception, str)


class Dummy(object):
    pass


@pytest.mark.usefixtures("new_pipegraph")
class TestAttributeJob:
    def test_basic_attribute_loading(self):
        o = Dummy()

        def load():
            return "shu"

        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        of = "out/a"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == "shu"

    def test_attribute_loading_does_not_affect_mcp(self):
        o = Dummy()

        def load():
            return "shu"

        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        of = "out/a"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == "shu"
        assert not (hasattr(o, "a"))

    def test_attribute_loading_does_not_run_withot_dependency(self):
        o = Dummy()
        tf = "out/testfile"

        def load():
            write(tf, "hello")
            return "shu"

        ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        ppg.run_pipegraph()
        assert not (hasattr(o, "a"))
        assert not (os.path.exists(tf))

    def test_attribute_disappears_after_direct_dependency(self):
        o = Dummy()
        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", lambda: "shu")
        of = "out/A"

        def do_write():
            write(of, o.a)

        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = "out/B"

        def later_write():
            write(of2, o.a)

        ppg.FileGeneratingJob(of2, later_write).depends_on(fgjob)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert read(of) == "shu"
        assert not (os.path.exists(of2))

    def test_attribute_disappears_after_direct_dependencies(self):
        o = Dummy()
        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", lambda: "shu")
        of = "out/A"

        def do_write():
            write(of, o.a)

        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = "out/B"

        def later_write():
            write(of2, o.a)

        fgjobB = ppg.FileGeneratingJob(of2, later_write).depends_on(
            fgjob
        )  # now, this one does not depend on job, o it should not be able to access o.a
        of3 = "out/C"

        def also_write():
            write(of3, o.a)

        fgjobC = ppg.FileGeneratingJob(of3, also_write).depends_on(job)
        fgjobB.depends_on(
            fgjobC
        )  # otherwise, B might be started C returned, and the cleanup will not have occured!
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert read(of) == "shu"
        assert read(of3) == "shu"
        assert not (os.path.exists(of2))

    def test_passing_non_string_as_attribute(self):
        o = Dummy()

        def inner():
            ppg.AttributeLoadingJob("out/a", o, 5, 55)

        assertRaises(ValueError, inner)

    def test_passing_non_function(self):
        o = Dummy()

        def inner():
            ppg.AttributeLoadingJob("out/a", o, "a", 55)

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()

        def inner():
            ppg.AttributeLoadingJob(5, o, "a", lambda: 55)

        assertRaises(ValueError, inner)

    def test_no_swapping_attributes_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        ppg.AttributeLoadingJob("out/A", o, "a", cache)

        def inner():
            ppg.AttributeLoadingJob("out/A", o, "b", cache)

        assertRaises(ppg.JobContractError, inner)

    def test_raises_on_non_string_attribute_name(self):
        def inner():
            o = Dummy()
            ppg.AttributeLoadingJob("out/A", o, 23, lambda: 5)

        assertRaises(ValueError, inner)

    def test_raises_on_non_function_callback(self):
        def inner():
            o = Dummy()
            ppg.AttributeLoadingJob("out/A", o, 23, 55)

        assertRaises(ValueError, inner)

    def test_no_swapping_objects_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        o2 = Dummy()
        ppg.CachedAttributeLoadingJob("out/A", o, "a", cache)

        def inner():
            ppg.CachedAttributeLoadingJob("out/A", o2, "a", cache)

        assertRaises(ppg.JobContractError, inner)

    def test_ignore_code_changes(self):
        def a():
            append("out/Aa", "A")
            return "5"

        o = Dummy()
        jobA = ppg.AttributeLoadingJob("out/A", o, "a", a)
        jobA.ignore_code_changes()
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", o.a))
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/Aa") == "A"
        assert read("out/B") == "5"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def b():
            append("out/Aa", "B")
            return "5"

        jobA = ppg.AttributeLoadingJob("out/A", o, "a", b)
        jobA.ignore_code_changes()
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", o.a))
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        # not rerun
        assert read("out/Aa") == "A"
        assert read("out/B") == "5"

    def test_callback_must_be_callable(self):
        def inner():
            o = Dummy()
            ppg.AttributeLoadingJob("load_dummy_shu", o, "a", "shu")

        assertRaises(ValueError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class TestTempFileGeneratingJob:
    def test_basic(self):
        temp_file = "out/temp"

        def write_temp():
            write(temp_file, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A():
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()
        assert read(ofA) == "hello"
        assert not (os.path.exists(temp_file))

    def test_does_not_get_return_if_output_is_done(self):
        temp_file = "out/temp"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count():
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp():
            write(temp_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        # now, rerun. Tempfile has been deleted,
        # and should not be regenerated
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"

    def test_does_not_get_return_if_output_is_not(self):
        temp_file = "out/temp"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count():
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp():
            write(temp_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        # now, rerun. Tempfile has been deleted,
        # and should  be regenerated
        os.unlink(out_file)
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"  # since the outfile was removed...
        assert read(count_file) == "XX"
        assert read(normal_count_file) == "AA"

    def test_dependand_explodes(self):
        temp_file = "out/temp"

        def write_temp():
            append(temp_file, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A():
            raise ValueError("shu")

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        # ppg.run_pipegraph()
        assert not (os.path.exists(ofA))
        assert os.path.exists(temp_file)

        ppg.new_pipegraph(rc_gen(), dump_graph=False)

        def write_A_ok():
            write(ofA, read(temp_file))

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        fgjob = ppg.FileGeneratingJob(ofA, write_A_ok)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()

        assert read(ofA) == "hello"  # tempfile job has not been rerun
        assert not (os.path.exists(temp_file))  # and the tempfile has been removed...

    def test_removes_tempfile_on_exception(self):
        temp_file = "out/temp"

        def write_temp():
            write(temp_file, "hello")
            raise ValueError("should")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A():
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists(temp_file))
        assert not (os.path.exists(ofA))

    def test_renames_tempfile_on_exception_if_requested(self):
        temp_file = "out/temp"

        def write_temp():
            write(temp_file, "hello")
            raise ValueError("should")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp, rename_broken=True)
        ofA = "out/A"

        def write_A():
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists(temp_file))
        assert os.path.exists(temp_file + ".broken")
        assert not (os.path.exists(ofA))

    def test_passing_non_function(self):
        def inner():
            ppg.TempFileGeneratingJob("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.TempFileGeneratingJob(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_rerun_because_of_new_dependency_does_not_rerun_old(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: append("out/A", read("out/temp")) or append("out/Ab", "A")
        )
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda: write("out/temp", "T"))
        jobA.depends_on(jobB)
        ppg.run_pipegraph()
        assert not (os.path.exists("out/temp"))
        assert read("out/A") == "T"
        assert read("out/Ab") == "A"  # ran once

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob("out/A", lambda: append("out/A", read("out/temp")))
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda: write("out/temp", "T"))
        jobA.depends_on(jobB)
        jobC = ppg.FileGeneratingJob("out/C", lambda: append("out/C", read("out/temp")))
        jobC.depends_on(jobB)
        ppg.run_pipegraph()
        assert not (os.path.exists("out/temp"))
        assert read("out/Ab") == "A"  # ran once, not rewritten
        assert read("out/C") == "T"  # a new file

    def test_chaining_multiple(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.TempFileGeneratingJob(
            "out/C", lambda: write("out/C", read("out/A") + "C")
        )
        jobD = ppg.FileGeneratingJob(
            "out/D", lambda: write("out/D", read("out/B") + read("out/C"))
        )
        jobD.depends_on(jobC)
        jobD.depends_on(jobB)
        jobC.depends_on(jobA)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/D") == "ABAC"
        assert not (os.path.exists("out/A"))
        assert not (os.path.exists("out/B"))
        assert not (os.path.exists("out/C"))

    def test_chaining_multiple_differently(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda: write("out/B", read("out/A") + "B")
        )
        jobD = ppg.FileGeneratingJob(
            "out/D", lambda: write("out/D", read("out/B") + "D")
        )
        jobE = ppg.FileGeneratingJob(
            "out/E", lambda: write("out/E", read("out/B") + "E")
        )
        jobF = ppg.FileGeneratingJob(
            "out/F", lambda: write("out/F", read("out/A") + "F")
        )
        jobD.depends_on(jobB)
        jobE.depends_on(jobB)
        jobB.depends_on(jobA)
        jobF.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/D") == "ABD"
        assert read("out/E") == "ABE"
        assert read("out/F") == "AF"
        assert not (os.path.exists("out/A"))
        assert not (os.path.exists("out/B"))
        assert not (os.path.exists("out/C"))

    def test_rerun_because_of_new_dependency_does_not_rerun_old_chained(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run_pipegraph()
        assert read("out/C") == "ABC"
        assert read("out/Cx") == "1"

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobD = ppg.FileGeneratingJob(
            "out/D",
            lambda: write("out/D", read("out/A") + "D") or append("out/Dx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        jobD.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/D") == "AD"
        assert read("out/Dx") == "1"
        assert read("out/C") == "ABC"
        assert read("out/Cx") == "1"

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.TempFileGeneratingJob(
            "out/A", lambda: write("out/A", "a")
        )  # note changing function code!
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobD = ppg.FileGeneratingJob(
            "out/D",
            lambda: write("out/D", read("out/A") + "D") or append("out/Dx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        jobD.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/D") == "aD"
        assert read("out/Dx") == "11"  # both get rerun
        assert read("out/C") == "aBC"
        assert read("out/Cx") == "11"

    def test_cleanup_if_never_run(self):
        temp_file = "out/temp"

        def write_temp():
            write(temp_file, "hello")

        def write_a():
            write("A", "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        temp_job.ignore_code_changes()
        jobA = ppg.FileGeneratingJob("A", write_a)
        jobA.ignore_code_changes()
        write_a()  # so the file is there!
        ppg.run_pipegraph()
        assert not (os.path.exists("out/temp"))
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        write_temp()
        assert os.path.exists("out/temp")
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        temp_job.ignore_code_changes()
        temp_job.do_cleanup_if_was_never_run = True
        ppg.run_pipegraph()
        assert not (os.path.exists("out/temp"))


@pytest.mark.usefixtures("new_pipegraph")
class TestMultiTempFileGeneratingJob:
    def test_basic(self):
        ppg.new_pipegraph(rc_gen(), quiet=False, dump_graph=False)

        temp_files = ["out/temp", "out/temp2"]

        def write_temp():
            for temp_file in temp_files:
                write(temp_file, "hello")

        temp_job = ppg.MultiTempFileGeneratingJob(temp_files, write_temp)
        ofA = "out/A"

        def write_A():
            write(ofA, read(temp_files[0]) + read(temp_files[1]))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()
        assert read(ofA) == "hellohello"
        assert not (os.path.exists(temp_files[0]))
        assert not (os.path.exists(temp_files[1]))


@pytest.mark.usefixtures("new_pipegraph")
class TestTempFilePlusGeneratingJob:
    def test_basic(self):
        ppg.new_pipegraph(quiet=False, dump_graph=False)
        temp_file = "out/temp"
        keep_file = "out/keep"

        def write_temp():
            write(temp_file, "hello")
            write(keep_file, "hello")

        temp_job = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        ofA = "out/A"

        def write_A():
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()
        assert read(ofA) == "hello"
        assert not (os.path.exists(temp_file))
        assert os.path.exists(keep_file)

    def test_raises_on_keep_equal_temp_file(self):
        temp_file = "out/temp"
        keep_file = temp_file

        def write_temp():
            write(temp_file, "hello")
            write(keep_file, "hello")

        def inner():
            ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)

        assertRaises(ValueError, inner)

    def test_does_not_get_return_if_output_is_done(self):
        temp_file = "out/temp"
        keep_file = "out/keep"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count():
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp():
            write(temp_file, "temp")
            write(keep_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        assert os.path.exists(keep_file)
        # now, rerun. Tempfile has been deleted,
        # and should not be regenerated
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        assert os.path.exists(keep_file)

    def test_fails_if_keep_file_is_not_generated(self):
        temp_file = "out/temp"
        keep_file = "out/keep"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count():
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp():
            write(temp_file, "temp")
            # write(keep_file, 'temp')
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.RuntimeError, inner)
        assert not (os.path.exists(out_file))
        assert not (os.path.exists(keep_file))
        assert os.path.exists(temp_file)

    def test_does_get_rerun_if_keep_file_is_gone(self):
        temp_file = "out/temp"
        keep_file = "out/keep"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count():
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp():
            write(temp_file, "temp")
            write(keep_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        assert os.path.exists(keep_file)
        os.unlink(keep_file)
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert read(out_file) == "1:temp"
        assert read(count_file) == "XX"  # where we see the temp file job ran again
        assert read(normal_count_file) == "AA"  # which is where we see it ran again...
        assert os.path.exists(keep_file)


@pytest.mark.usefixtures("new_pipegraph")
class TestInvariant:
    def sentinel_count(self):
        sentinel = "out/sentinel"
        try:
            op = open(sentinel, "r")
            count = int(op.read())
            op.close()
        except IOError:
            count = 1
        op = open(sentinel, "w")
        op.write("%i" % (count + 1))
        op.close()
        return count

    def test_filegen_jobs_detect_code_change(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again...

        def do_write2():
            append(of, "sha")

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        assert read(of) == "sha"  # has been run again ;).

    def test_filegen_jobs_ignores_code_change(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()

        assert read(of) == "shu"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again, for no change

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def do_write2():
            append(of, "sha")

        job = ppg.FileGeneratingJob(of, do_write2)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again, since we ignored the changes

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        assert (
            read(of) == "sha"
        )  # But the new code had not been stored, not ignoring => redoing.

    def test_parameter_dependency(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again...
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3, 4))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        assert read(of) == "shushu"  # has been run again ;).

    def test_parameter_invariant_adds_hidden_job_id_prefix(self):
        param = "A"
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", param))
        jobB = ppg.ParameterInvariant("out/A", param)
        jobA.depends_on(jobB)
        ppg.run_pipegraph()
        assert read("out/A") == param

    def test_filetime_dependency(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        write(of, "hello")
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileTimeInvariant was not stored before...
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        write(ftfn, "hello")  # same content, different time

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job does not get rerun - filetime invariant is now filechecksum invariant...

    def test_filechecksum_dependency(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileTimeInvariant was not stored before...
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        # logging.info("NOW REWRITE")
        write(ftfn, "hello")  # same content, different time

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        # time.sleep(1) #we don't care about the time, size should be enough...
        write(ftfn, "hello world!!")  # different time
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shushu"  # job does get rerun

    def test_robust_filechecksum_invariant(self):
        of = "out/B"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileTimeInvariant was not stored before...

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

    def test_robust_filechecksum_invariant_after_normal(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileTimeInvariant was not stored before...
        assert read("out/sentinel") == "2"  # job does not get rerun...

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...
        assert read("out/sentinel") == "2"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        assert read("out/sentinel") == "2"  # job does not get rerun...
        print("now it counts")
        ppg.run_pipegraph()
        assert read("out/sentinel") == "2"  # job does not get rerun...
        assert read(of) == "shu"  # job does not get rerun...

    def test_file_invariant_with_md5sum(self):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileTimeInvariant was not stored before...

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello world")  # different content
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shushu"
        )  # job get's run though there is a file, because the md5sum changed.

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello")  # different content, but the md5sum is stil the same!
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shushu"  # job does not get rerun, md5sum did not change...

        t = time.time() - 100  # force a file time mismatch
        os.utime(
            ftfn, (t, t)
        )  # I must change the one on the actual file, otherwise the 'size+filetime is the same' optimization bytes me

        ppg.util.stat_cache = {}
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shushushu"
        )  # job does get rerun, md5sum and file time mismatch
        assert os.stat(ftfn)[stat.ST_MTIME] == os.stat(ftfn + ".md5sum")[stat.ST_MTIME]

    def test_invariant_dumping_on_job_failure(self):
        def w():
            write("out/A", "A")
            append("out/B", "B")

        def func_c():
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ppg.FileGeneratingJob("out/A", w)
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        ppg.run_pipegraph()
        assert func_dep.was_invalidated
        assert fg.was_invalidated
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def func_c1():
            append("out/C", "D")

        def w2():
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ppg.FileGeneratingJob("out/A", w2)  # and this job crashes
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        try:
            ppg.run_pipegraph()
        except ppg.RuntimeError:
            pass
        assert func_dep.was_invalidated
        assert fg.was_invalidated
        assert not (os.path.exists("out/A"))  # since it was removed, and not recreated
        assert read("out/B") == "B"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        fg = ppg.FileGeneratingJob("out/A", w)  # but this was not done the last time...
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        ppg.run_pipegraph()
        assert not (func_dep.was_invalidated)  # not invalidated
        assert fg.was_invalidated  # yeah
        assert read("out/A") == "A"
        assert read("out/B") == "BB"

    def test_invariant_dumping_on_graph_exception(self):
        # when an exception occurs not within a job
        # but within the pipegraph itself (e.g. when the user hit's CTRL-C
        # which we simulate here
        class ExplodingJob(ppg.FileGeneratingJob):
            def __setattr__(self, name, value):
                if (
                    name == "stdout"
                    and value is not None
                    and hasattr(self, "do_explode")
                    and self.do_explode
                ):
                    raise KeyboardInterrupt("simulated")
                else:
                    self.__dict__[name] = value

        def w():
            write("out/A", "A")
            append("out/B", "B")

        def func_c():
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ExplodingJob("out/A", w)
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        ppg.run_pipegraph()
        assert func_dep.was_invalidated
        assert fg.was_invalidated
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def func_c1():
            append("out/C", "D")

        def w2():
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ExplodingJob("out/A", w2)  # and this job crashes
        fg.do_explode = True
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        ki_raised = False
        try:
            ppg.run_pipegraph()
        except ppg.RuntimeError:
            pass
        except KeyboardInterrupt:  # we expect this to be raised
            ki_raised = True
            pass
        if not ki_raised:
            raise ValueError("KeyboardInterrupt was not raised")
        assert func_dep.was_invalidated
        assert fg.was_invalidated
        assert not (os.path.exists("out/A"))  # since it was removed, and not recreated
        assert read("out/B") == "B"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        fg = ExplodingJob("out/A", w)  # but this was not done the last time...
        fg.ignore_code_changes()  # no auto invariants for this test...
        fg.depends_on(func_dep)
        ppg.run_pipegraph()
        assert not (func_dep.was_invalidated)  # not invalidated
        assert fg.was_invalidated  # yeah
        assert read("out/A") == "A"
        assert read("out/B") == "BB"

    def test_FileTimeInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        write("out/shu", "shu")
        job = ppg.FileTimeInvariant("out/shu")
        jobB = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "a"))

        def inner():
            job.depends_on(jobB)

        assertRaises(ppg.JobContractError, inner)

    def test_FileChecksumInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        write("out/shu", "shu")
        job = ppg.FileChecksumInvariant("out/shu")
        jobB = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "a"))

        def inner():
            job.depends_on(jobB)

        assertRaises(ppg.JobContractError, inner)

    def test_ParameterInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        job = ppg.ParameterInvariant("out/shu", ("123",))
        jobB = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "a"))

        def inner():
            job.depends_on(jobB)

        assertRaises(ppg.JobContractError, inner)

    def test_unpickable_raises(self):
        class Unpickable(object):
            def __getstate__(self):
                raise ValueError("SHU")

        ppg.ParameterInvariant("a", (Unpickable(), "shu"))

        def inner():
            ppg.run_pipegraph()

        assertRaises(ValueError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class TestFunctionInvariant:
    # most of the function invariant testing is handled by other test classes.
    # but these are more specialized.

    def test_generator_expressions(self):
        def get_func(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func2(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func3(r):
            def shu():
                return sum(i + 1 for i in r)

            return shu

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_lambdas(self):
        def get_func(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func2(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func3(x):
            def inner():
                arg = lambda y: x + x  # noqa:E731
                return arg(1)

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        self.maxDiff = 20000
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_inner_functions(self):
        def get_func(x):
            def inner():
                return 23

            return inner

        def get_func2(x):
            def inner():
                return 23

            return inner

        def get_func3(x):
            def inner():
                return 23 + 5

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_nested_inner_functions(self):
        def get_func(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func2(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func3(x):
            def inner():
                def shu():
                    return 23 + 5

                return shu

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)  # constat value is different

    def test_inner_functions_with_parameters(self):
        def get_func(x):
            def inner():
                return x

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func(2000)
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_passing_non_function_raises(self):
        def inner():
            ppg.FunctionInvariant("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_none_as_function_is_ok(self):
        job = ppg.FunctionInvariant("out/a", None)
        jobB = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB.depends_on(job)
        ppg.run_pipegraph()
        assert read("out/A") == "A"

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.FunctionInvariant(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        def shu():
            pass

        job = ppg.FunctionInvariant("shu", shu)
        jobB = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "a"))

        def inner():
            job.depends_on(jobB)

        assertRaises(ppg.JobContractError, inner)

    def test_raises_on_duplicate_with_different_functions(self):
        def shu():
            return "a"

        ppg.FunctionInvariant("A", shu)
        ppg.FunctionInvariant("A", shu)  # ok.

        def inner():
            ppg.FunctionInvariant("A", lambda: "b")  # raises ValueError

        assertRaises(ppg.JobContractError, inner)

    def test_instance_functions_raise(self):
        class shu:
            def __init__(self, letter):
                self.letter = letter

            def get_job(self):
                job = ppg.FileGeneratingJob(
                    "out/" + self.letter, lambda: append("out/" + self.letter, "A")
                )
                job.depends_on(ppg.FunctionInvariant("shu.sha", self.sha))
                return job

            def sha(self):
                return 55 * 23

        x = shu("A")
        x.get_job()
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        append("out/A", "A")

        ppg.new_pipegraph(dump_graph=False)
        x.get_job()
        y = shu("B")

        def inner():
            y.get_job()

        assertRaises(ppg.JobContractError, inner)

    def test_invariant_build_in_function(self):
        a = ppg.FunctionInvariant("test", sorted)
        a._get_invariant(None, [])

    def test_cython_function(self):
        # horrible mocking hack to see that it actually extracts something, - not tested if it's the right thing...
        import stat

        class MockImClass:
            __module__ = "stat"  # whatever
            __name__ = "MockIM"

        class MockCython:
            __doc__ = "File:stat.py starting at line 0)\nHello world"
            im_func = "cyfunction shu"
            im_class = MockImClass

            def __call__(self):
                pass

            def __repr__(self):
                return "cyfunction mockup"

        c = MockCython()
        mi = MockImClass()
        stat.MockIM = mi
        c.im_class = mi
        print(c.im_class.__module__ in sys.modules)

        a = ppg.FunctionInvariant("test", c)
        a._get_invariant(None, [])

    def test_closure_capturing(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func([1, 2, 3]))
        b = ppg.FunctionInvariant(
            "b", func([1, 2, 3])
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func([1, 2, 3, 4])
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_closure_capturing_dict(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func({"1": "a", "3": "b", "2": "c"}))
        b = ppg.FunctionInvariant(
            "b", func({"1": "a", "3": "b", "2": "c"})
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"1": "a", "3": "b", "2": "d"})
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_closure_capturing_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = set(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = set(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"3", "2"})
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)

    def test_closure_capturing_frozen_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = frozenset(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = frozenset(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func(frozenset({"3", "2"}))
        )  # and this invariant should be different
        av = a.get_invariant(False, [])
        bv = b.get_invariant(False, [])
        cv = c.get_invariant(False, [])
        assert a.get_invariant(False, [])
        assert bv == av
        assert not (av == cv)


@pytest.mark.usefixtures("new_pipegraph")
class TestMultiFileInvariant:
    def test_new_raises_unchanged(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])

        def inner():
            jobA.get_invariant(False, {})

        assertRaises(ppg.job.util.NothingChanged, inner)

    def test_no_raise_on_no_change(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs

    def test_filetime_changed_contents_the_same(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        subprocess.check_call(["touch", "--date=2004-02-29", "out/b"])
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert not (cs2 == cs)
        assert not ([x[1] for x in cs2] == [x[1] for x in cs])  # times changed
        assert [x[2] for x in cs2] == [x[2] for x in cs]  # sizes did not
        assert [x[3] for x in cs2] == [x[3] for x in cs]

    def test_changed_file(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        write("out/b", "world!")
        cs2 = jobA.get_invariant(cs, {jobA.job_id: cs})
        assert not (cs2 == cs)
        assert [x[0] for x in cs2] == [x[0] for x in cs]  # file names the same
        # assert not ( [x[1] for x in cs2] == [x[1] for x in cs])  # don't test times, might not have changed
        assert not ([x[2] for x in cs2] == [x[2] for x in cs])  # sizes changed
        assert not ([x[3] for x in cs2] == [x[2] for x in cs])  # checksums changed

    def test_changed_file_same_size(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        time.sleep(2)  # must be certain we have a changed filetime!
        write("out/b", "worlt")
        cs2 = jobA.get_invariant(cs, {jobA.job_id: cs})
        assert not (cs2 == cs)
        assert [x[0] for x in cs2] == [x[0] for x in cs]  # file names the same
        assert [x[2] for x in cs2] == [x[2] for x in cs]  # sizes the same
        assert not ([x[3] for x in cs2] == [x[2] for x in cs])  # checksums changed

    def test_rehome_no_change(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        write("out2/b", "world")
        jobB = ppg.MultiFileInvariant(["out2/a", "out2/b"])

        def inner():
            jobB.get_invariant(False, {jobA.job_id: cs})

        assertRaises(ppg.job.util.NothingChanged, inner)

    def test_rehome_and_change(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        write("out2/b", "worl!x")  # either change the length, or wait 2 seconds...
        jobB = ppg.MultiFileInvariant(["out2/a", "out2/b"])
        cs3 = jobB.get_invariant(False, {jobA.job_id: cs})
        assert not ([x[3] for x in cs2] == [x[2] for x in cs3])  # checksums changed

    def test_non_existant_file_raises(self):
        def inner():
            ppg.MultiFileInvariant(["out/a"])

        assertRaises(ValueError, inner)

    def test_rehome_and_additional_file(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        write("out2/b", "world")
        write("out2/c", "worl!x")  # either change the length, or wait 2 seconds...
        jobB = ppg.MultiFileInvariant(["out2/a", "out2/b", "out2/c"])
        cs3 = jobB.get_invariant(False, {jobA.job_id: cs})
        assert not ([x[3] for x in cs2] == [x[2] for x in cs3])  # checksums changed

    def test_rehome_and_missing_file(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.job.util.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        jobB = ppg.MultiFileInvariant(["out2/a"])
        cs3 = jobB.get_invariant(False, {jobA.job_id: cs})
        assert not ([x[3] for x in cs2] == [x[2] for x in cs3])  # checksums changed


@pytest.mark.usefixtures("new_pipegraph")
class TestDependency:
    def test_simple_chain(self):
        o = Dummy()

        def load_a():
            return "shu"

        jobA = ppg.AttributeLoadingJob("a", o, "myattr", load_a)
        ofB = "out/B"

        def do_write_b():
            write(ofB, o.myattr)

        jobB = ppg.FileGeneratingJob(ofB, do_write_b).depends_on(jobA)
        ofC = "out/C"

        def do_write_C():
            write(ofC, o.myattr)

        ppg.FileGeneratingJob(ofC, do_write_C).depends_on(jobA)

        ofD = "out/D"

        def do_write_d():
            write(ofD, read(ofC) + read(ofB))

        ppg.FileGeneratingJob(ofD, do_write_d).depends_on([jobA, jobB])

    def test_failed_job_kills_those_after(self):
        ofA = "out/A"

        def write_a():
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a)

        ofB = "out/B"

        def write_b():
            raise ValueError("shu")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c():
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert os.path.exists(ofA)  # which was before the error
        assert not (os.path.exists(ofB))  # which was on the error
        assert not (os.path.exists(ofC))  # which was after the error
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobC = ppg.FileGeneratingJob(ofC, write_c)

        def write_b_ok():
            write(ofB, "BB")

        jobB = ppg.FileGeneratingJob(ofB, write_b_ok)
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run_pipegraph()

        assert os.path.exists(ofA)
        assert read(ofA) == "hello"  # run only once!
        assert os.path.exists(ofB)
        assert os.path.exists(ofC)

    def test_done_filejob_does_not_gum_up_execution(self):
        ofA = "out/A"
        write(ofA, "1111")

        def write_a():
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobA.ignore_code_changes()  # or it will inject a function dependency and run never the less...

        ofB = "out/B"

        def write_b():
            append(ofB, "hello")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c():
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        assert os.path.exists(ofA)

        ppg.run_pipegraph()

        assert os.path.exists(ofB)
        assert os.path.exists(ofC)
        assert read(ofA) == "1111"

    def test_invariant_violation_redoes_deps_but_not_nondeps(self):
        def get_job(name):
            fn = "out/" + name

            def do_write():
                if os.path.exists(fn + ".sentinel"):
                    d = read(fn + ".sentinel")
                else:
                    d = ""
                append(fn + ".sentinel", name)  # get's longer all the time...
                write(fn, d + name)  # get's deleted anyhow...

            return ppg.FileGeneratingJob(fn, do_write)

        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello",))
        jobA.depends_on(dep)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello stranger",))
        jobA.depends_on(dep)  # now, the invariant has been changed, all jobs rerun...
        ppg.run_pipegraph()
        assert read("out/A") == "AA"  # thanks to our smart rerun aware job definition..
        assert read("out/B") == "BB"
        assert read("out/C") == "CC"
        assert read("out/D") == "D"  # since that one does not to be rerun...

    def test_depends_on_accepts_a_list(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
        jobC.depends_on([jobA, jobB])
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_accepts_multiple_values(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
        jobC.depends_on(jobA, jobB)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_accepts_multiple_values_mixed(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB])
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_none_ignored(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB], None, [None])
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_excludes_on_non_jobs(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))

        def inner():
            jobA.depends_on("SHU")

        assertRaises(ValueError, inner)

    def test_depends_on_instant_cycle_check(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))

        def inner():
            jobA.depends_on(jobA)

        assertRaises(ppg.CycleError, inner)

    def test_depends_on_accepts_a_list_of_lists(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda: write("out/C", read("out/A") + read("out/B") + read("out/D")),
        )
        jobD = ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))
        jobC.depends_on([jobA, [jobB, jobD]])
        assert jobD in jobC.prerequisites
        assert jobA in jobC.prerequisites
        assert jobB in jobC.prerequisites
        ppg.run_pipegraph()
        assert jobC.prerequisites is None
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "ABD"
        assert read("out/D") == "D"


@pytest.mark.usefixtures("new_pipegraph")
class TestDependencyInjectionJob:
    def test_basic(self):
        # TODO: there is a problem with this apporach. The AttributeLoadingJob
        # references different objects, since it get's pickled alongside with the method,
        # and depickled again, and then it's not the same object anymore,
        # so the FileGeneratingJob and the AttributeLoadingJob in this test
        # reference different objects.
        # I'm not sure how to handle this right now though.

        # I have an idea: Do JobGraphModifyingJobs in each slave, and send back just the
        # dependency data (and new job name).
        # that way, we can still execute on any slave, and all the pointers should be
        # right.
        ppg.new_pipegraph(rc_gen(), dump_graph=False)

        o = Dummy()
        of = "out/A"

        def do_write():
            # logging.info("Accessing dummy (o) %i in pid %s" % (id(o), os.getpid()))
            write(of, o.A + o.B)

        job = ppg.FileGeneratingJob(of, do_write)

        def generate_deps():
            def load_a():
                # logging.info('executing load A')
                return "A"

            def load_b():
                # logging.info('executing load B')
                return "B"

            # logging.info("Creating dl on %i in pid %s" % (id(o), os.getpid()))
            dlA = ppg.AttributeLoadingJob("dlA", o, "A", load_a)
            # logging.info("created dlA")
            dlB = ppg.AttributeLoadingJob("dlB", o, "B", load_b)
            job.depends_on(dlA)
            job.depends_on(dlB)

        gen_job = ppg.DependencyInjectionJob("C", generate_deps)
        job.depends_on(gen_job)
        ppg.run_pipegraph()
        assert read(of) == "AB"

    def test_raises_on_non_dependend_job_injection(self):
        o = Dummy()
        of = "out/A"

        def do_write():
            write(of, o.A + o.B)

        job = ppg.FileGeneratingJob(of, do_write)
        jobD = ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

        def generate_deps():
            def load_a():
                return "A"

            def load_b():
                return "B"

            dlA = ppg.AttributeLoadingJob("dlA", o, "A", load_a)
            dlB = ppg.AttributeLoadingJob("dlB", o, "B", load_b)
            job.depends_on(dlA)
            jobD.depends_on(dlB)  # this line must raise

        gen_job = ppg.DependencyInjectionJob("C", generate_deps)
        job.depends_on(gen_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists(of))  # since the gen job crashed
        assert os.path.exists(
            "out/D"
        )  # since it has no relation to the gen job actually...
        assert isinstance(gen_job.exception, ppg.JobContractError)

    def test_injecting_filegenerating_job(self):
        of = "out/A"

        def do_write():
            write(of, read("out/B"))

        job = ppg.FileGeneratingJob(of, do_write)

        def generate_dep():
            def write_B():
                write("out/B", "B")

            inner_job = ppg.FileGeneratingJob("out/B", write_B)
            job.depends_on(inner_job)

        job_gen = ppg.DependencyInjectionJob("gen_job", generate_dep)
        job.depends_on(job_gen)
        ppg.run_pipegraph()
        assert read("out/A") == "B"

    def test_passing_non_function(self):
        def inner():
            ppg.DependencyInjectionJob("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.DependencyInjectionJob(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_injecting_into_data_loading_does_not_retrigger(self):
        o = Dummy()

        def do_write():
            append("out/A", o.a + o.b)
            append("out/B", "X")

        def dl_a():
            o.a = "A"

        def do_run():
            of = "out/A"

            def inject():
                def dl_b():
                    o.b = "B"

                job_dl_b = ppg.DataLoadingJob("ob", dl_b)
                job_dl.depends_on(job_dl_b)

            job_fg = ppg.FileGeneratingJob(of, do_write)
            job_dl = ppg.DataLoadingJob("oa", dl_a)
            job_fg.depends_on(job_dl)
            job_inject = ppg.DependencyInjectionJob("inject", inject)
            job_dl.depends_on(job_inject)
            ppg.run_pipegraph()

        do_run()
        assert read("out/A") == "AB"
        assert read("out/B") == "X"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        do_run()
        assert read("out/A") == "AB"  # same data
        assert read("out/B") == "X"  # no rerun!
        # now let's test if a change triggers the rerun

        def do_run2():
            of = "out/A"

            def inject():
                def dl_b():
                    o.b = "C"  # so this dl has changed...

                job_dl_b = ppg.DataLoadingJob("ob", dl_b)
                job_dl.depends_on(job_dl_b)

            job_fg = ppg.FileGeneratingJob(of, do_write)
            job_dl = ppg.DataLoadingJob("oa", dl_a)
            job_fg.depends_on(job_dl)
            job_inject = ppg.DependencyInjectionJob("inject", inject)
            job_dl.depends_on(job_inject)
            ppg.run_pipegraph()

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        do_run2()
        assert read("out/A") == "AC"  # same data
        assert read("out/B") == "XX"  # one rerun...


@pytest.mark.usefixtures("new_pipegraph")
class TestJobGeneratingJob:
    def test_basic(self):
        def gen():
            ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
            ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))

        ppg.JobGeneratingJob("genjob", gen)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_raises_if_needs_more_cores_than_we_have(self):
        def gen():
            jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            jobA.cores_needed = 20000

        ppg.JobGeneratingJob("genjob", gen)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))  # since the gen job crashed
        jobGenerated = ppg.util.global_pipegraph.jobs["out/A"]
        assert jobGenerated.failed
        assert jobGenerated.error_reason == "Needed to much memory/cores"

    def test_raises_if_needs_more_ram_than_we_have(self):
        def gen():
            jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            jobA.memory_needed = 1024 * 1024 * 1024 * 1024

        ppg.JobGeneratingJob("genjob", gen)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))  # since the gen job crashed
        jobGenerated = ppg.util.global_pipegraph.jobs["out/A"]
        assert jobGenerated.failed
        assert jobGenerated.error_reason == "Needed to much memory/cores"

    def test_with_memory_needed(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobA.memory_needed = 1024
        ppg.run_pipegraph()
        assert os.path.exists("out/A")  # since the gen job crashed

    def test_injecting_multiple_stages(self):
        def gen():
            def genB():
                def genC():
                    ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "D"

    def test_generated_job_depending_on_each_other_one_of_them_is_Invariant(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also ParameterDependencyC that A depends on
        # does that work
        def gen():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
            jobB.ignore_code_changes()
            jobC = ppg.ParameterInvariant("C", ("ccc",))
            jobB.depends_on(jobC)

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/B") == "B"

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def gen2():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "C"))
            jobB.ignore_code_changes()
            jobC = ppg.ParameterInvariant("C", ("ccc",))
            jobB.depends_on(jobC)

        ppg.JobGeneratingJob("A", gen2)
        ppg.run_pipegraph()
        assert read("out/B") == "B"  # no rerun

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def gen3():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "C"))
            jobB.ignore_code_changes()
            jobCX = ppg.ParameterInvariant("C", ("DDD",))
            jobB.depends_on(jobCX)

        ppg.JobGeneratingJob("A", gen3)
        ppg.run_pipegraph()
        assert read("out/B") == "C"  # did get rerun

    def test_generated_job_depending_on_job_that_cant_have_finished(self):
        # basic idea. You have jobgen A, and filegen B.
        # filegenB depends on jobgenA.
        # jobGenA created C depends on filegenB
        # Perhaps add a filegen D that's independand of jobGenA, but C also deps on D
        def a():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))

            def genA():
                jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
                jobC.depends_on(jobB)

            jobA = ppg.JobGeneratingJob("A", genA)
            jobB.depends_on(jobA)
            ppg.run_pipegraph()
            assert read("out/B") == "B"
            assert read("out/C") == "C"

        def b():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
            jobD = ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

            def genA():
                jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
                jobC.depends_on(jobB)
                jobC.depends_on(jobD)

            jobA = ppg.JobGeneratingJob("A", genA)
            jobB.depends_on(jobA)
            ppg.run_pipegraph()
            assert read("out/B") == "B"
            assert read("out/C") == "C"

        a()
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        b()

    def test_generated_job_depending_on_each_other(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also filegenC that depends on B
        # does that work
        def gen():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
            jobC = ppg.FileGeneratingJob("out/C", lambda: write("out/C", read("out/B")))
            jobC.depends_on(jobB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/B") == "B"
        assert read("out/C") == "B"

    def test_generated_job_depending_on_each_other_one_of_them_is_loading(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also DataloadingC that depends on B
        # does that work
        def gen():
            def load():
                global shu
                shu = "123"

            def do_write():
                global shu
                write("out/A", shu)

            dl = ppg.DataLoadingJob("dl", load)
            jobB = ppg.FileGeneratingJob("out/A", do_write)
            jobB.depends_on(dl)

        ppg.JobGeneratingJob("gen", gen)
        ppg.run_pipegraph()
        assert read("out/A") == "123"

    def test_passing_non_function(self):
        def inner():
            ppg.JobGeneratingJob("out/a", "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.JobGeneratingJob(5, lambda: 1)

        assertRaises(ValueError, inner)

    def test_generated_jobs_that_can_not_run_right_away_because_of_dataloading_do_not_crash(
        self
    ):
        o = Dummy()
        existing_dl = ppg.AttributeLoadingJob("a", o, "a", lambda: "Ashu")

        def gen():
            new_dl = ppg.AttributeLoadingJob("b", o, "b", lambda: "Bshu")
            fg_a = ppg.FileGeneratingJob("out/C", lambda: write("out/C", o.a))
            fg_b = ppg.FileGeneratingJob("out/D", lambda: write("out/D", o.b))
            fg_a.depends_on(existing_dl)
            fg_b.depends_on(new_dl)

        ppg.JobGeneratingJob("E", gen)
        ppg.run_pipegraph()
        assert read("out/C") == "Ashu"
        assert read("out/D") == "Bshu"

    def test_filegen_invalidated_jobgen_created_filegen_later_also_invalidated(self):
        a = ppg.FileGeneratingJob("out/A", lambda: writeappend("out/A", "out/Ac", "A"))
        p = ppg.ParameterInvariant("p", "p")
        a.depends_on(p)

        def gen():
            c = ppg.FileGeneratingJob(
                "out/C", lambda: writeappend("out/C", "out/Cx", "C")
            )
            c.depends_on(a)

        ppg.JobGeneratingJob("b", gen)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/Ac") == "A"
        assert read("out/C") == "C"
        assert read("out/Cx") == "C"
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        a = ppg.FileGeneratingJob("out/A", lambda: writeappend("out/A", "out/Ac", "A"))
        p = ppg.ParameterInvariant("p", "p2")
        a.depends_on(p)
        ppg.JobGeneratingJob("b", gen)
        ppg.run_pipegraph()
        assert read("out/Ac") == "AA"
        assert read("out/Cx") == "CC"


@pytest.mark.usefixtures("new_pipegraph")
class TestCachedAttributeJob:
    def test_simple(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

    def test_preqrequisites_end_up_on_lfg(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        job_preq = ppg.FileGeneratingJob("out/B", do_write)
        job.depends_on(job_preq)
        assert not (job_preq in job.prerequisites)
        assert job_preq in job.lfg.prerequisites

    def test_no_dependand_still_calc(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc)
        assert not (os.path.exists("out/mycalc"))
        ppg.run_pipegraph()
        assert os.path.exists("out/mycalc")

    def test_invalidation_redoes_output(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job = ppg.CachedAttributeLoadingJob(
            "out/mycalc", o, "a", calc2
        )  # now, jobB should be deleted...
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 200))

    def test_invalidation_ignored_does_not_redo_output(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc2)
        job.ignore_code_changes()
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc2)
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result

    def test_throws_on_non_function_func(self):
        o = Dummy()

        def calc():
            return 55

        def inner():
            ppg.CachedAttributeLoadingJob("out/mycalc", calc, o, "a")

        assertRaises(ValueError, inner)

    def test_calc_depends_on_added_dependencies(self):
        o = Dummy()
        load_attr = ppg.AttributeLoadingJob("load_attr", o, "o", lambda: 55)

        def calc():
            return o.o

        def out():
            write("out/A", str(o.o2))

        cached = ppg.CachedAttributeLoadingJob("out/cached_job", o, "o2", calc)
        fg = ppg.FileGeneratingJob("out/A", out)
        fg.depends_on(cached)
        cached.depends_on(load_attr)
        ppg.run_pipegraph()
        assert read("out/A") == "55"

    def test_depends_on_returns_self(self):
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        o = Dummy()
        jobA = ppg.CachedAttributeLoadingJob(
            "out/A", o, "shu", lambda: write("out/A", "shu")
        )
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "shu"))
        assert jobA.depends_on(jobB) is jobA

    def test_passing_non_function(self):
        o = Dummy()

        def inner():
            ppg.CachedAttributeLoadingJob("out/a", o, "a", 55)

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()

        def inner():
            ppg.CachedAttributeLoadingJob(5, o, "a", lambda: 55)

        assertRaises(ValueError, inner)

    def test_no_swapping_attributes_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        ppg.CachedAttributeLoadingJob("out/A", o, "a", cache)

        def inner():
            ppg.CachedAttributeLoadingJob("out/A", o, "b", cache)

        assertRaises(ppg.JobContractError, inner)

    def test_no_swapping_objects_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        o2 = Dummy()
        ppg.CachedAttributeLoadingJob("out/A", o, "a", cache)

        def inner():
            ppg.CachedAttributeLoadingJob("out/A", o2, "a", cache)

        assertRaises(ppg.JobContractError, inner)

    def test_cached_jobs_get_depencies_only_on_the_lazy_filegenerator_not_on_the_loading_job(
        self
    ):
        o = Dummy()

        def calc():
            return list(range(0, o.b))

        job = ppg.CachedAttributeLoadingJob("a", o, "a", calc)

        def do_b():
            return 100

        jobB = ppg.AttributeLoadingJob("b", o, "b", do_b)
        job.depends_on(jobB)
        assert not (jobB in job.prerequisites)
        assert jobB in job.lfg.prerequisites
        ppg.run_pipegraph()
        assert jobB.was_invalidated
        assert job.was_invalidated

    def test_cached_attribute_job_does_not_load_its_preqs_on_cached(self):
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")

        def calc():
            append("out/B", "B")
            return o.a * 2

        def output():
            write("out/D", o.c)

        dl = ppg.DataLoadingJob("out/A", a)
        ca = ppg.CachedAttributeLoadingJob("out/C", o, "c", calc)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        ca.depends_on(dl)
        ppg.run_pipegraph()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # ran the dl job
        assert read("out/B") == "B"  # ran the calc job...
        os.unlink("out/D")  # so the filegen and the loadjob of cached should rerun...
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        dl = ppg.DataLoadingJob("out/A", a)
        ca = ppg.CachedAttributeLoadingJob("out/C", o, "c", calc)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        ca.depends_on(dl)
        ppg.run_pipegraph()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # did not run the dl job
        assert read("out/B") == "B"  # did not run the calc job again

    def test_raises_on_non_string_filename(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob(55, o, "c", lambda: 55)

        assertRaises(ValueError, inner)

    def test_raises_on_non_string_attribute(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob("out/C", o, 354, lambda: 55)

        assertRaises(ValueError, inner)

    def test_callback_must_be_callable(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob("x", o, "a", "shu")

        assertRaises(ValueError, inner)

    def test_name_must_be_str(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob(123, o, "a", lambda: 123)

        assertRaises(ValueError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class CachedDataLoadingJobTests:
    def test_simple(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        job = ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

    def test_no_dependand_still_calc(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        # job.ignore_code_changes() #or it would run anyway... hm.
        assert not (os.path.exists("out/mycalc"))
        ppg.run_pipegraph()
        assert os.path.exists("out/mycalc")

    def test_preqrequisites_end_up_on_lfg(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        job = ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        job_preq = ppg.FileGeneratingJob("out/B", do_write)
        job.depends_on(job_preq)
        assert not (job_preq in job.prerequisites)
        assert job_preq in job.lfg.prerequisites

    def test_passing_non_function_to_calc(self):
        def inner():
            ppg.CachedDataLoadingJob("out/a", "shu", lambda value: 55)

        assertRaises(ValueError, inner)

    def test_passing_non_function_to_store(self):
        def inner():
            ppg.CachedDataLoadingJob("out/a", lambda value: 55, "shu")

        assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            ppg.CachedDataLoadingJob(5, lambda: 1, lambda value: 55)

        assertRaises(ValueError, inner)

    def test_being_generated(self):
        o = Dummy()

        def calc():
            return 55

        def store(value):
            o.a = value

        def dump():
            write("out/A", str(o.a))

        def gen():
            calc_job = ppg.CachedDataLoadingJob("out/B", calc, store)
            dump_job = ppg.FileGeneratingJob("out/A", dump)
            dump_job.depends_on(calc_job)

        ppg.JobGeneratingJob("out/C", gen)
        ppg.run_pipegraph()
        assert read("out/A") == "55"

    def test_being_generated_nested(self):
        o = Dummy()

        def calc():
            return 55

        def store(value):
            o.a = value

        def dump():
            write("out/A", str(o.a))

        def gen():
            calc_job = ppg.CachedDataLoadingJob("out/B", calc, store)

            def gen2():
                dump_job = ppg.FileGeneratingJob("out/A", dump)
                dump_job.depends_on(calc_job)

            ppg.JobGeneratingJob("out/D", gen2)

        ppg.JobGeneratingJob("out/C", gen)
        ppg.run_pipegraph()
        assert read("out/A") == "55"

    def test_cached_jobs_get_depencies_only_on_the_lazy_filegenerator_not_on_the_loading_job(
        self
    ):
        o = Dummy()

        def calc():
            return list(range(0, o.b))

        def load(value):
            o.a = value

        job = ppg.CachedDataLoadingJob("a", calc, load)

        def do_b():
            return 100

        jobB = ppg.AttributeLoadingJob("b", o, "b", do_b)
        job.depends_on(jobB)
        assert not (jobB in job.prerequisites)
        assert jobB in job.lfg.prerequisites

    def test_cached_dataloading_job_does_not_load_its_preqs_on_cached(self):
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")

        def calc():
            append("out/B", "B")
            return o.a * 2

        def load(value):
            o.c = value
            append("out/Cx", "C")  # not C, that's the cached file, you know...

        def output():
            write("out/D", o.c)

        dl = ppg.DataLoadingJob("out/A", a)
        ca = ppg.CachedDataLoadingJob("out/C", calc, load)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        ca.depends_on(dl)
        ppg.run_pipegraph()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # ran the dl job
        assert read("out/B") == "B"  # ran the calc job...
        assert read("out/Cx") == "C"  # ran the load jobo
        os.unlink("out/D")  # so the filegen and the loadjob of cached should rerun...
        ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
        dl = ppg.DataLoadingJob("out/A", a)
        ca = ppg.CachedDataLoadingJob("out/C", calc, load)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        ca.depends_on(dl)
        ppg.run_pipegraph()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # did not run the dl job
        assert read("out/B") == "B"  # did not run the calc job again
        assert read("out/Cx") == "CC"  # did run the load job again

    def test_name_must_be_str(self):
        def inner():
            o = Dummy()
            ppg.CachedDataLoadingJob(123, o, "a", lambda: 123)

        assertRaises(ValueError, inner)


is_pypy = platform.python_implementation() == "PyPy"
if (  # noqa: C901
    is_pypy
):  # right now, pypy not support numpypy.memmap. IF it ever exists, this will raise...

    class DoesMemMapNowExistOnPyPy:
        def test_does_not_exist(self):
            import numpypy

            assert not ("memmap" in dir(numpypy))

        def test_memmap_job_creation_raises(self):
            import numpypy

            o = Dummy()

            def calc():
                return numpypy.array(range(0, 10), dtype=numpypy.uint32)

            def store(value):
                o.append(value)

            def cleanup():
                del o[0]

            def inner():
                ppg.MemMappedDataLoadingJob("out/A", calc, store, numpypy.uint32)

            ppg.new_pipegraph(rc_gen(), dump_graph=False)
            assertRaises(NotImplementedError, inner)


else:

    @pytest.mark.usefixtures("new_pipegraph")
    class TestMemMappedDataLoadingJob:
        """Similar to a CachedDataLoadingJob, except that the data in question is a numpy
        array that get's memmapped in later on"""

        def test_simple(self):
            ppg.new_pipegraph(rc_gen(), dump_graph=False)
            import numpy

            o = []

            def calc():
                return numpy.array(range(0, 10), dtype=numpy.uint32)

            def store(value):
                o.append(value)

            def cleanup():
                del o[0]

            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"

            def do_write():
                assert isinstance(o[0], numpy.core.memmap)
                write(of, ",".join(str(x) for x in o[0]))

            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            ppg.run_pipegraph()
            assert read("out/B") == "0,1,2,3,4,5,6,7,8,9"

        def test_invalidation(self):
            import numpy

            o = {}

            def calc():
                return numpy.array(range(0, 10), dtype=numpy.uint32)

            def store(value):
                o[0] = value

            def cleanup():
                del o[0]

            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"

            def do_write():
                assert isinstance(o[0], numpy.core.memmap)
                write(of, ",".join(str(x) for x in o[0]))
                append("out/C", "a")

            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            ppg.run_pipegraph()
            assert read("out/B") == "0,1,2,3,4,5,6,7,8,9"
            assert read("out/C") == "a"
            # now, no run...
            ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)
            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            ppg.run_pipegraph()
            assert read("out/C") == "a"

            ppg.new_pipegraph(rc_gen(), quiet=True, dump_graph=False)

            def calc2():
                append("out/D", "a")
                return numpy.array(range(0, 12), dtype=numpy.uint32)

            dl = ppg.MemMappedDataLoadingJob("out/A", calc2, store, numpy.uint32)
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.cleanup = cleanup

            ppg.run_pipegraph()
            assert read("out/D") == "a"
            assert read("out/B") == "0,1,2,3,4,5,6,7,8,9,10,11"
            assert read("out/C") == "aa"

        def test_raises_on_non_array(self):
            import numpy

            o = []

            def calc():
                return list(range(0, 10))

            def store(value):
                o.append(value)

            def cleanup():
                del o[0]

            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"

            def do_write():
                assert isinstance(o[0], numpy.core.memmap)
                write(of, ",".join(str(x) for x in o[0]))

            ppg.FileGeneratingJob(of, do_write).depends_on(dl)

            def inner():
                ppg.run_pipegraph()

            assertRaises(ppg.RuntimeError, inner)
            assert isinstance(dl.lfg.exception, ppg.JobContractError)
            assert dl.failed

        def test_raises_on_wrong_dtype(self):
            import numpy

            o = []

            def calc():
                return numpy.array(range(0, 10), dtype=numpy.float)

            def store(value):
                o.append(value)

            def cleanup():
                del o[0]

            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"

            def do_write():
                assert isinstance(o[0], numpy.core.memmap)
                write(of, ",".join(str(x) for x in o[0]))

            ppg.FileGeneratingJob(of, do_write).depends_on(dl)

            def inner():
                ppg.run_pipegraph()

            assertRaises(ppg.RuntimeError, inner)
            assert isinstance(dl.lfg.exception, ppg.JobContractError)
            assert dl.failed


@pytest.mark.usefixtures("new_pipegraph")
class TestResourceCoordinator:
    def test_jobs_that_need_all_cores_are_spawned_one_by_one(self):
        # we'll determine this by the start respective end times..
        ppg.new_pipegraph(
            ppg.resource_coordinators.LocalSystem(max_cores_to_use=2),
            quiet=True,
            dump_graph=False,
        )
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobA.cores_needed = -1
        jobB.cores_needed = -1
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        if jobA.start_time < jobB.start_time:
            first_job = jobA
            second_job = jobB
        else:
            first_job = jobB
            second_job = jobA
        print(
            "times",
            first_job.start_time,
            first_job.stop_time,
            second_job.start_time,
            second_job.stop_time,
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time < second_job.start_time

    def test_jobs_concurrent_jobs_run_concurrently(self):
        # we'll determine this by the start respective end times..
        ppg.new_pipegraph(
            ppg.resource_coordinators.LocalSystem(max_cores_to_use=2),
            quiet=True,
            dump_graph=False,
        )
        jobA = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))
        jobA.cores_needed = 1
        jobB.cores_needed = 1
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        if jobA.start_time < jobB.start_time:
            first_job = jobA
            second_job = jobB
        else:
            first_job = jobB
            second_job = jobA
        print(
            "times",
            first_job.start_time,
            first_job.stop_time,
            second_job.start_time,
            second_job.stop_time,
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time > second_job.start_time


class CantDepickle:
    """A class that can't be depickled (throws a type error,
    just like the numpy.maskedarray does occacionally)"""

    def __getstate__(self):
        return {"shu": "5"}

    def __setstate__(self, state):
        print(state)
        raise TypeError("I can be pickled, but not unpickled")


@pytest.mark.usefixtures("new_pipegraph")
class TestingTheUnexpectedTests:
    def test_job_killing_python(self):
        def dies():
            import sys

            # logging.info("Now terminating child python")
            sys.exit(5)

        fg = ppg.FileGeneratingJob("out/A", dies)
        try:
            ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))
        assert isinstance(fg.exception, ppg.JobDiedException)
        assert fg.exception.exit_code == 5

    def test_job_killing_python_stdout_stderr_logged(self):
        def dies():
            import sys

            # logging.info("Now terminating child python")
            print("hello")
            sys.stderr.write("I am stderr\n")
            sys.stdout.flush()
            sys.exit(5)

        fg = ppg.FileGeneratingJob("out/A", dies)
        try:
            ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))
        assert isinstance(fg.exception, ppg.JobDiedException)
        assert fg.exception.exit_code == 5
        assert fg.stdout == "hello\n"
        assert fg.stderr == "I am stderr\n"

    def test_unpickle_bug_prevents_single_job_from_unpickling(self):
        def do_a():
            write("out/A", "A")
            append("out/As", "A")

        ppg.FileGeneratingJob("out/A", do_a)

        def do_b():
            write("out/B", "A")
            append("out/Bs", "A")

        job_B = ppg.FileGeneratingJob("out/B", do_b)
        cd = CantDepickle()
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd,))
        job_B.depends_on(job_parameter_unpickle_problem)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/As") == "A"
        assert read("out/B") == "A"
        assert read("out/Bs") == "A"
        print("second run")
        ppg.new_pipegraph(dump_graph=False)

        ppg.FileGeneratingJob("out/A", do_a)
        job_B = ppg.FileGeneratingJob("out/B", do_b)
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd,))
        job_B.depends_on(job_parameter_unpickle_problem)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/As") == "A"
        assert read("out/B") == "A"
        assert (
            read("out/Bs") == "AA"
        )  # this one got rerun because we could not load the invariant...

    def testing_import_does_not_hang(self):  # see python issue22853
        old_dir = os.getcwd()
        os.chdir(os.path.dirname(__file__))
        p = subprocess.Popen(
            ["python", "_import_does_not_hang.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout, stderr)
        assert b"OK" in stdout
        os.chdir(old_dir)


class TestsNotImplemented:
    @pytest.mark.xfail
    def test_temp_jobs_and_gen_jobs(self):
        # DependencyInjection A creates TempJob B and job C (c is already done)
        # DependencyInjeciton D (dep on A) creates TempJob B and job E
        # Now, When A runs, B is created, and not added to the jobs-to-run list
        # since it is not necessary (with C being done).
        # now D runs, B would not be required by E, but does not get added to the
        # run list (since it is not new), and later on, the sanity check crashes.

        # alternativly, if C is not done, execution order is A, B, C. Then cleanup
        # for B happens, then D is run, the E explodes, because cleanup has been done!

        # now, if A returns B, it get's injected into the dependenies of D,
        # the exeuction order is correct, but B get's done no matter what because D
        # now requires it, even if both C and E have already been done.

        # what a conundrum
        raise NotImplementedError()

    @pytest.mark.xfail
    def test_cached_job_done_but_gets_invalidated_by_dependency_injection_generated_job(
        self
    ):
        # very similar to the previous case,
        # this basically directly get's you into the 'Job execution order territory...'
        raise NotImplementedError()


@pytest.mark.usefixtures("new_pipegraph")
class TestUtils:
    def test_assert_uniqueness_simple(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_ok(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy("sha")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_ok_multi_classes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy2("shu")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_slashes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        def inner():
            Dummy("shu/sha")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        Dummy("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check_no_instance_of_second_class(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        # a = Dummy('shu')
        # does not raise of course...
        Dummy2("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check_list(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=[Dummy])

        Dummy("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class TestFinalJobs:
    def test_correct_dependencies(self):
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")

        def b():
            append("out/B", "B")
            append("out/Bx", "B")

        def c():
            o.c = "C"
            append("out/C", "C")

        def d():
            append("out/D", "D")
            append("out/Dx", "D")

        jobA = ppg.DataLoadingJob("out/A", a)
        jobB = ppg.FileGeneratingJob("out/B", b)
        jobC = ppg.DataLoadingJob("out/C", c)
        jobD = ppg.FileGeneratingJob("out/D", d)
        # jobD.depends_on(jobC)
        # jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        final_job = ppg.FinalJob("da_final", lambda: None)
        ppg.util.global_pipegraph.connect_graph()
        print(final_job.prerequisites)
        for x in jobB, jobC, jobD:
            assert x in final_job.prerequisites
            assert final_job in x.dependants
        assert not (jobA in final_job.prerequisites)

    def test_cannot_depend_on_final_job(self):
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        final_job = ppg.FinalJob("da_final", lambda: None)
        try:
            jobA.depends_on(final_job)
            assert not ("Exception not raised")
        except ppg.JobContractError as e:
            print(e)
            assert "No jobs can depend on FinalJobs" in str(e)


@pytest.mark.usefixtures("new_pipegraph")
class TestJobList:
    def raises_on_non_job(self):
        def inner():
            ppg.JobList("shu")

        assertRaises(ValueError, inner)

    def raises_on_non_job_in_list(self):
        try:
            a = ppg.DataLoadingJob("a", lambda: 5)
            ppg.JobList([a, "shu"])
            assert not ("Exception not raised")
        except ValueError as e:
            assert " was not a job object" in str(e)

    def test_add(self):
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda: write("A", "A"))
        jobC = ppg.FileGeneratingJob("C", lambda: write("C", "A"))
        jobD = ppg.FileGeneratingJob("D", lambda: write("D", "A"))
        l1 = ppg.JobList([jobA])
        l2 = l1 + jobB
        assert len(l2) == 2
        l2 = l1 + jobB
        assert len(l2) == 2
        l3 = l2 + ppg.JobList(jobC)
        assert len(l3) == 3
        l4 = l3 + [jobD]
        assert len(l4) == 4

    def test_depends_on(self):
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda: write("A", "A"))
        jobC = ppg.FileGeneratingJob("C", lambda: write("C", "A"))
        l1 = ppg.JobList([jobA, jobB])
        l1.depends_on(jobC)
        assert jobC in jobA.prerequisites
        assert jobC in jobB.prerequisites
        ppg.util.global_pipegraph.connect_graph()
        assert jobA in jobC.dependants
        assert jobB in jobC.dependants

    def test_str(self):
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        l1 = ppg.JobList([jobA])
        x = str(l1)
        assert x.startswith("JobList of ")

    def test_adding_two_jobs(self):
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda: write("A", "A"))
        x = jobA + jobB
        assert isinstance(x, ppg.JobList)
        assert len(x) == 2


@pytest.mark.usefixtures("new_pipegraph")
class TestDefinitionErrors:
    def test_defining_function_invariant_twice(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.FunctionInvariant("a", a)

        def inner():
            ppg.FunctionInvariant("a", b)

        assertRaises(ppg.JobContractError, inner)

    def test_defining_function_and_parameter_invariant_with_same_name(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.FunctionInvariant("PIa", a)

        def inner():
            ppg.ParameterInvariant("a", b)

        assertRaises(ppg.JobContractError, inner)

    def test_defining_function_and_parameter_invariant_with_same_name_reversed(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.ParameterInvariant("a", b)

        def inner():
            ppg.FunctionInvariant("PIa", a)

        assertRaises(ppg.JobContractError, inner)


@pytest.mark.usefixtures("new_pipegraph")
class TestPathLib:
    def test_multifilegenerating_job_requires_string_filenames(self):
        import pathlib

        x = lambda: 5  # noqa:E731
        ppg.MultiFileGeneratingJob(["a"], x)
        ppg.MultiFileGeneratingJob([pathlib.Path("a")], x)

        def inner():
            ppg.MultiFileGeneratingJob([0])

        assertRaises(TypeError, inner)

        def inner():
            ppg.MultiFileGeneratingJob([b"a"])  # bytes is not a string type

        assertRaises(TypeError, inner)

    def test_accepts(self):
        import pathlib

        write("aaa", "hello")
        write("bbb", "hello")
        write("ccc", "hello")
        a = ppg.FileTimeInvariant(pathlib.Path("aaa"))
        a1 = ppg.MultiFileInvariant([pathlib.Path("bbb"), "ccc"])
        b = ppg.FileGeneratingJob(
            pathlib.Path("b"),
            lambda of: write(of, "bb" + read("aaa") + read("bbb") + read("ccc")),
        )
        b.depends_on(a)
        b.depends_on(a1)

        class Dummy:
            pass

        dd = Dummy()

        def mf():
            write("c", "cc" + read("g"))
            write("d", "dd" + read("h") + dd.attr)
            write("e", "ee" + read("i") + read("j"))

        c = ppg.MultiFileGeneratingJob([pathlib.Path("c"), "d", pathlib.Path("e")], mf)
        c.depends_on(b)
        d = ppg.FunctionInvariant(pathlib.Path("f"), lambda x: x + 1)
        c.depends_on(d)
        e = ppg.ParameterInvariant(pathlib.Path("c"), "hello")
        c.depends_on(e)
        f = ppg.TempFileGeneratingJob(pathlib.Path("g"), lambda: write("g", "gg"))
        c.depends_on(f)

        def tmf():
            write("h", "hh")
            write("i", "ii")

        g = ppg.MultiTempFileGeneratingJob([pathlib.Path("h"), "i"], tmf)
        c.depends_on(g)

        def tpf():
            write("j", "jjjj")
            write("k", "kkkk")

        h = ppg.TempFilePlusGeneratingJob(pathlib.Path("j"), pathlib.Path("k"), tpf)
        c.depends_on(h)

        i = ppg.CachedDataLoadingJob(
            pathlib.Path("l"), lambda: write("l", "llll"), lambda res: res
        )
        c.depends_on(i)

        m = ppg.CachedAttributeLoadingJob(pathlib.Path("m"), dd, "attr", lambda: "55")
        c.depends_on(m)
        ppg.run_pipegraph()
        assert read("aaa") == "hello"
        assert read("b") == "bbhellohellohello"
        assert read("c") == "ccgg"
        assert read("d") == "ddhh55"
        assert read("e") == "eeiijjjj"
        assert not (os.path.exists("g"))
        assert not (os.path.exists("h"))
        assert not (os.path.exists("i"))
        assert not (os.path.exists("j"))
        assert read("k") == "kkkk"


def test_shu(new_pipegraph):
    assert 5
