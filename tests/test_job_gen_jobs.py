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
import pytest
import pypipegraph as ppg
from .shared import write, assertRaises, read, append, writeappend, Dummy

shu = None


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

    def test_generated_job_depending_on_each_other_one_of_them_is_Invariant(
        self, new_pipegraph
    ):
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

        new_pipegraph.new_pipegraph()

        def gen2():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "C"))
            jobB.ignore_code_changes()
            jobC = ppg.ParameterInvariant("C", ("ccc",))
            jobB.depends_on(jobC)

        ppg.JobGeneratingJob("A", gen2)
        ppg.run_pipegraph()
        assert read("out/B") == "B"  # no rerun

        new_pipegraph.new_pipegraph()

        def gen3():
            jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "C"))
            jobB.ignore_code_changes()
            jobCX = ppg.ParameterInvariant("C", ("DDD",))
            jobB.depends_on(jobCX)

        ppg.JobGeneratingJob("A", gen3)
        ppg.run_pipegraph()
        assert read("out/B") == "C"  # did get rerun

    def test_generated_job_depending_on_job_that_cant_have_finished(
        self, new_pipegraph
    ):
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
        new_pipegraph.new_pipegraph()
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

        assertRaises(TypeError, inner)

    def test_generated_jobs_that_can_not_run_right_away_because_of_dataloading_do_not_crash(
        self,
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

    def test_filegen_invalidated_jobgen_created_filegen_later_also_invalidated(
        self, new_pipegraph
    ):
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
        new_pipegraph.new_pipegraph()

        a = ppg.FileGeneratingJob("out/A", lambda: writeappend("out/A", "out/Ac", "A"))
        p = ppg.ParameterInvariant("p", "p2")
        a.depends_on(p)
        ppg.JobGeneratingJob("b", gen)
        ppg.run_pipegraph()
        assert read("out/Ac") == "AA"
        assert read("out/Cx") == "CC"

    def test_raises_if_generating_within_dataload(self):
        ppg.util.global_pipegraph.quiet = False
        write_job = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "aa"))

        def load():
            ppg.FileGeneratingJob("out/B", lambda: write("out/B", "aa"))

        dl = ppg.DataLoadingJob("load_data", load)
        write_job.depends_on(dl)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()
        assert "Trying to add new jobs to running pipeline" in str(dl.exception)

    def test_ignored_if_generating_within_filegenerating(self):
        write_job = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "aa"))

        def load():
            ppg.FileGeneratingJob("out/B", lambda: write("out/B", "aa"))
            write("out/C", "c")

        dl = ppg.FileGeneratingJob("out/C", load)
        write_job.depends_on(dl)
        ppg.run_pipegraph()
        assert read("out/C") == "c"

    def test_jobgenerating_is_not_dependency_injection(self):
        old = ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

        def gen():
            write("out/E", "E")
            p = ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))
            old.depends_on(p)

        j = ppg.JobGeneratingJob("genjob", gen)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()
        assert isinstance(j.exception, ppg.JobContractError)
        assert read("out/E") == "E"
        assert not os.path.exists("out/C")  # that job never makes it to the pipeline
        assert read("out/D") == "D"

    def test_invalidation(self, new_pipegraph):
        def gen():
            ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "D"
        new_pipegraph.new_pipegraph()

        def gen():
            ppg.FileGeneratingJob("out/D", lambda: write("out/D", "E"))

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "E"

    def test_invalidation_multiple_stages(self, new_pipegraph):
        counter = [0]

        def count():
            counter[0] += 1
            return str(counter[0])

        def gen():
            def genB():
                def genC():
                    count()
                    ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "D"
        assert counter[0] == 1

        new_pipegraph.new_pipegraph()
        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "D"
        assert counter[0] == 2

        new_pipegraph.new_pipegraph()

        def gen():
            def genB():
                def genC():
                    count()
                    ppg.FileGeneratingJob("out/D", lambda: write("out/D", "E"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        assert read("out/D") == "E"
        assert counter[0] == 3


@pytest.mark.usefixtures("new_pipegraph")
class TestDependencyInjectionJob:
    def test_basic(self, new_pipegraph):
        # TODO: there is a problem with this apporach. The AttributeLoadingJob
        # references different objects, since it get's pickled alongside with the method,
        # and depickled again, and then it's not the same object anymore,
        # so the FileGeneratingJob and the AttributeLoadingJob in this test
        # reference different objects.
        # I'm not sure how to handle this right now though.

        # I have an idea: Do JobGraphModifyingJobs in each worker, and send back just the
        # dependency data (and new job name).
        # that way, we can still execute on any worker, and all the pointers should be
        # right.
        new_pipegraph.new_pipegraph()

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
            return [dlA, dlB]

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
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()

        assert not (os.path.exists(of))  # since the gen job crashed
        assert os.path.exists(
            "out/D"
        )  # since it has no relation to the gen job actually...
        assert isinstance(gen_job.exception, ppg.JobContractError)
        assert "was not dependand on the " in str(gen_job.exception)

    def test_raises_on_non_dependend_job_injection2(self):
        o = Dummy()
        of = "out/A"

        def do_write():
            write(of, o.A + o.B)

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

        def generate_deps():
            def load_a():
                return "A"

            def load_b():
                return "B"

            dlA = ppg.AttributeLoadingJob("dlA", o, "A", load_a)
            ppg.AttributeLoadingJob("dlB", o, "B", load_b)
            job.depends_on(dlA)
            # let's not do anything with dlA

        gen_job = ppg.DependencyInjectionJob("C", generate_deps)
        job.depends_on(gen_job)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()

        assert not (os.path.exists(of))  # since the gen job crashed
        assert os.path.exists(
            "out/D"
        )  # since it has no relation to the gen job actually...
        assert isinstance(gen_job.exception, ppg.JobContractError)
        assert "case 1" in str(gen_job.exception)

    def test_raises_on_non_dependend_job_injection2_can_be_ignored(self):
        o = Dummy()
        of = "out/A"

        def do_write():
            write(of, o.A)  # + o.B - but B is not in the dependency chain!

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.FileGeneratingJob("out/D", lambda: write("out/D", "D"))

        def generate_deps():
            def load_a():
                return "A"

            def load_b():
                return "B"

            dlA = ppg.AttributeLoadingJob("dlA", o, "A", load_a)
            ppg.AttributeLoadingJob("dlB", o, "B", load_b)
            job.depends_on(dlA)
            # let's not do anything with dlA

        gen_job = ppg.DependencyInjectionJob(
            "C", generate_deps, check_for_dependency_injections=False
        )
        job.depends_on(gen_job)
        ppg.run_pipegraph()

        assert os.path.exists(of)  # since the gen job crashed

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

        assertRaises(TypeError, inner)

    def test_injecting_into_data_loading_does_not_retrigger(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
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

        new_pipegraph.new_pipegraph()
        do_run2()
        assert read("out/A") == "AC"  # same data
        assert read("out/B") == "XX"  # one rerun...

    def test_generated_job_depends_on_failing_job(self, new_pipegraph):
        # import logging
        # new_pipegraph.new_pipegraph(log_file="debug.log", log_level=logging.DEBUG)
        def fn_a():
            raise ValueError()

        def fn_b():
            c = ppg.FileGeneratingJob("c", lambda: write("c", read("a")))
            c.depends_on(a)
            return [c]

        a = ppg.FileGeneratingJob("a", fn_a)
        b = ppg.JobGeneratingJob("b", fn_b)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()

        assert isinstance(a.exception, ValueError)
        assert a.error_reason == "Exception"
        assert b.error_reason == "no error"
        assert ppg.util.global_pipegraph.jobs["c"].error_reason == "Indirect"

    def test_generated_job_depends_on_failing_job_inverse(self, new_pipegraph):
        # import logging
        # new_pipegraph.new_pipegraph(log_file="debug.log", log_level=logging.DEBUG)
        def fn_a():
            raise ValueError()

        def fn_b():
            c = ppg.FileGeneratingJob("c", lambda: write("c", read("a")))
            c.depends_on(a)
            return [c]

        # note swapped order respective to previous test
        b = ppg.JobGeneratingJob("b", fn_b)
        a = ppg.FileGeneratingJob("a", fn_a)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()

        assert isinstance(a.exception, ValueError)
        assert a.error_reason == "Exception"
        assert b.error_reason == "no error"
        assert ppg.util.global_pipegraph.jobs["c"].error_reason == "Indirect"
