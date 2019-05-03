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
import platform
import pytest
import pypipegraph as ppg
from .shared import write, assertRaises, read, append, Dummy


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

    def test_invalidation_redoes_output(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job = ppg.CachedAttributeLoadingJob(
            "out/mycalc", o, "a", calc2
        )  # now, jobB should be deleted...
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 200))

    def test_invalidation_ignored_does_not_redo_output(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc2)
        job.ignore_code_changes()
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        new_pipegraph.new_pipegraph()
        job = ppg.CachedAttributeLoadingJob("out/mycalc", o, "a", calc2)
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result

    def test_throws_on_non_function_func(self):
        o = Dummy()

        with pytest.raises(ValueError):
            ppg.CachedAttributeLoadingJob(
                "out/mycalc", lambda: 5, o, "a"
            )  # wrong argument order

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

        assertRaises(TypeError, inner)

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

    def test_cached_attribute_job_does_not_load_its_preqs_on_cached(
        self, new_pipegraph
    ):
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
        new_pipegraph.new_pipegraph()
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

        assertRaises(TypeError, inner)

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

        assertRaises(TypeError, inner)

    def test_use_cores(self):
        o = Dummy()
        ca = ppg.CachedAttributeLoadingJob("out/C", o, "c", lambda: 55)
        assert ca.use_cores(5) is ca
        assert ca.lfg.cores_needed == 5


@pytest.mark.usefixtures("new_pipegraph")
class TestCachedDataLoadingJob:
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

        assertRaises(TypeError, inner)

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

    def test_cached_dataloading_job_does_not_load_its_preqs_on_cached(
        self, new_pipegraph
    ):
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
        new_pipegraph.new_pipegraph()
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
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob(123, lambda: 123, lambda: 5)
        with pytest.raises(ValueError):
            ppg.CachedDataLoadingJob("123", 123, lambda: 5)
        with pytest.raises(ValueError):
            ppg.CachedDataLoadingJob("123", lambda: 5, 123)

    def test_cant_unpickle(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        job = ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        job.ignore_code_changes()
        write("out/mycalc", "no unpickling this")
        of = "out/A"

        def do_write():
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        with pytest.raises(ValueError):
            ppg.run_pipegraph()
        assert isinstance(job.exception, ValueError)
        assert "Unpickling error" in str(job.exception)

    def test_use_cores(self):
        ca = ppg.CachedDataLoadingJob("out/C", lambda: 55, lambda x: None)
        assert ca.use_cores(5) is ca
        assert ca.lfg.cores_needed == 5


is_pypy = platform.python_implementation() == "PyPy"
if (  # noqa: C901
    is_pypy
):  # right now, pypy not support numpypy.memmap. IF it ever exists, this will raise...

    class DoesMemMapNowExistOnPyPy:
        def test_does_not_exist(self):
            import numpypy

            assert not ("memmap" in dir(numpypy))

        def test_memmap_job_creation_raises(self, new_pipegraph):
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

            new_pipegraph.new_pipegraph()
            assertRaises(NotImplementedError, inner)


else:

    @pytest.mark.usefixtures("new_pipegraph")
    class TestMemMappedDataLoadingJob:
        """Similar to a CachedDataLoadingJob, except that the data in question is a numpy
        array that get's memmapped in later on"""

        def test_simple(self, new_pipegraph):
            new_pipegraph.new_pipegraph()
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
            dl.depends_on_params(123)
            ppg.run_pipegraph()
            assert read("out/B") == "0,1,2,3,4,5,6,7,8,9"

        def test_raises_on_non_string(self):
            import numpy

            with pytest.raises(TypeError):
                ppg.MemMappedDataLoadingJob(55, lambda: 5, lambda: 5, numpy.uint32)
            with pytest.raises(ValueError):
                ppg.MemMappedDataLoadingJob("shu", 5, lambda: 5, numpy.uint32)
            with pytest.raises(ValueError):
                ppg.MemMappedDataLoadingJob("shu", lambda: 5, 5, numpy.uint32)

        def test_ignore_code_changes(self, new_pipegraph):
            import numpy
            import pathlib

            o = []

            def calc():
                return numpy.array(range(0, 10), dtype=numpy.uint32)

            def store(value):
                o.append(value)

            def cleanup():
                o.clear()

            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"

            def do_write():
                assert isinstance(o[0], numpy.core.memmap)
                write(of, ",".join(str(x) for x in o[0]))

            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            ppg.run_pipegraph()
            assert read("out/B") == "0,1,2,3,4,5,6,7,8,9"

            new_pipegraph.new_pipegraph()

            def calc2():
                return numpy.array(range(0, 10), dtype=numpy.uint32) + 1

            dl = ppg.MemMappedDataLoadingJob("out/A", calc2, store, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            ppg.run_pipegraph()
            assert read("out/B") == "1,2,3,4,5,6,7,8,9,10"

            new_pipegraph.new_pipegraph()

            def calc3():
                return numpy.array(range(0, 10), dtype=numpy.uint32) + 1

            dl = ppg.MemMappedDataLoadingJob("out/A", calc3, store, numpy.uint32)
            dl.ignore_code_changes()
            dl.cleanup = cleanup
            of = "out/B"
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            ppg.run_pipegraph()
            assert read("out/B") == "1,2,3,4,5,6,7,8,9,10"  # jup, no rerun

            new_pipegraph.new_pipegraph()

            def store2(value):
                o.append(value)
                o.append(value)

            dl = ppg.MemMappedDataLoadingJob("out/A", calc2, store2, numpy.uint32)
            dl.ignore_code_changes()
            dl.cleanup = cleanup
            of = "out/B"
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            ppg.run_pipegraph()
            assert read("out/B") == "1,2,3,4,5,6,7,8,9,10"  # jup, no rerun

            new_pipegraph.new_pipegraph()
            dl = ppg.MemMappedDataLoadingJob("out/A", calc2, store2, numpy.uint32)
            dl.cleanup = cleanup
            of = "out/B"
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            write("out/B", "replace me")
            ppg.run_pipegraph()
            assert read("out/B") == "1,2,3,4,5,6,7,8,9,10"  # jup, no rerun
            new_pipegraph.new_pipegraph()
            dl = ppg.MemMappedDataLoadingJob(
                pathlib.Path("out/A"), calc2, store2, numpy.uint32
            )
            dl.cleanup = cleanup
            of = "out/B"
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            dl.depends_on_params(123)
            dl.lfg.depends_on_params(
                456
            )  # this should trigger just the load invalidation
            write("out/B", "replace me")
            ppg.run_pipegraph()
            assert read("out/B") == "1,2,3,4,5,6,7,8,9,10"  # jup, no rerun

        def test_invalidation(self, new_pipegraph):
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
            new_pipegraph.new_pipegraph()
            dl = ppg.MemMappedDataLoadingJob("out/A", calc, store, numpy.uint32)
            dl.cleanup = cleanup
            ppg.FileGeneratingJob(of, do_write).depends_on(dl)
            ppg.run_pipegraph()
            assert read("out/C") == "a"

            new_pipegraph.new_pipegraph()

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
