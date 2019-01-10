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

import os
import sys
import pytest
import pypipegraph as ppg
from .shared import write, assertRaises, read, Dummy, append

global_test = 0


@pytest.mark.usefixtures("new_pipegraph")
class TestJobs:
    def test_assert_singletonicity_of_jobs(self, new_pipegraph):
        ppg.forget_job_status()
        new_pipegraph.new_pipegraph()
        of = "out/a"
        data_to_write = "hello"

        def do_write():
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        job2 = ppg.FileGeneratingJob(of, do_write)
        assert job is job2

    def test_redifining_a_jobid_with_different_class_raises(self, new_pipegraph):
        ppg.forget_job_status()
        new_pipegraph.new_pipegraph()
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

    def test_addition(self, new_pipegraph):
        def write_func(of):
            def do_write():
                write(of, "do_write done")

            return of, do_write

        new_pipegraph.new_pipegraph()
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

    def test_equality_is_identity(self, new_pipegraph):
        def write_func(of):
            def do_write():
                write(of, "do_write done")

            return of, do_write

        new_pipegraph.new_pipegraph()
        jobA = ppg.FileGeneratingJob(*write_func("out/a"))
        jobA1 = ppg.FileGeneratingJob(*write_func("out/a"))
        jobB = ppg.FileGeneratingJob(*write_func("out/b"))
        assert jobA is jobA1
        assert jobA == jobA1
        assert not (jobA == jobB)

    def test_has_hash(self, new_pipegraph):
        new_pipegraph.new_pipegraph()
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
        for x in range(0, 10):
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

    def test_invaliding_removes_file(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()
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
    def test_basic(self, new_pipegraph):
        of = ["out/a", "out/b"]

        def do_write():
            for f in of:
                append(f, "shu")

        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            assert read(f) == "shu"
        new_pipegraph.new_pipegraph()
        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            assert read(f) == "shu"  # ie. job has net been rerun...
        # but if I now delete one...
        os.unlink(of[0])
        new_pipegraph.new_pipegraph()
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

    def test_invalidation_removes_all_files(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
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
shared_value = ""


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
        global shared_value
        shared_value = "I was the the global in the mcp"

        def load():
            global shared_value
            shared_value = "shared data"

        of = "out/a"

        def do_write():
            write(of, shared_value)

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

    def test_ignore_code_changes(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()

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

    def test_does_not_get_return_if_output_is_done(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"

    def test_does_not_get_return_if_output_is_not(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert not (os.path.exists(temp_file))
        assert read(out_file) == "1:temp"  # since the outfile was removed...
        assert read(count_file) == "XX"
        assert read(normal_count_file) == "AA"

    def test_dependand_explodes(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()

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

    def test_rerun_because_of_new_dependency_does_not_rerun_old(self, new_pipegraph):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: append("out/A", read("out/temp")) or append("out/Ab", "A")
        )
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda: write("out/temp", "T"))
        jobA.depends_on(jobB)
        ppg.run_pipegraph()
        assert not (os.path.exists("out/temp"))
        assert read("out/A") == "T"
        assert read("out/Ab") == "A"  # ran once

        new_pipegraph.new_pipegraph()
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

    def test_rerun_because_of_new_dependency_does_not_rerun_old_chained(
        self, new_pipegraph
    ):
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

        new_pipegraph.new_pipegraph()
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

        new_pipegraph.new_pipegraph()
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

    def test_cleanup_if_never_run(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
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

    def test_does_not_get_return_if_output_is_done(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
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

    def test_does_get_rerun_if_keep_file_is_gone(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFilePlusGeneratingJob(temp_file, keep_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        assert read(out_file) == "1:temp"
        assert read(count_file) == "XX"  # where we see the temp file job ran again
        assert read(normal_count_file) == "AA"  # which is where we see it ran again...
        assert os.path.exists(keep_file)


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
