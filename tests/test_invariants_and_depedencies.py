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
import stat
import time
import hashlib
import subprocess
import shutil
import pytest
import pypipegraph as ppg
from .shared import write, assertRaises, read, append, Dummy


class Undepickable(object):
    def __getstate__(self):
        return {"shu": 123}  # must not return falsey value

    def __setstate__(self, state):
        self.sha = state["shu"]
        import pickle

        raise pickle.UnpicklingError("SHU")


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

    def test_filegen_jobs_detect_code_change(self, new_pipegraph):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"
        new_pipegraph.new_pipegraph()
        ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again...

        def do_write2():
            append(of, "sha")

        new_pipegraph.new_pipegraph()
        ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        assert read(of) == "sha"  # has been run again ;).

    def test_filegen_jobs_ignores_code_change(self, new_pipegraph):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()

        assert read(of) == "shu"
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again, for no change

        new_pipegraph.new_pipegraph()

        def do_write2():
            append(of, "sha")

        job = ppg.FileGeneratingJob(of, do_write2)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again, since we ignored the changes

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        assert (
            read(of) == "sha"
        )  # But the new code had not been stored, not ignoring => redoing.

    def test_parameter_dependency(self, new_pipegraph):
        of = "out/a"

        def do_write():
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # has not been run again...
        new_pipegraph.new_pipegraph()
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

    def test_depends_on_func(self):
        a = ppg.FileGeneratingJob("out/A", lambda: write("a"))
        b = a.depends_on_func("a123", lambda: 123)
        assert b.job_id.startswith(a.job_id + "_")
        assert b in a.prerequisites

    def test_depends_on_file(self):
        a = ppg.FileGeneratingJob("out/A", lambda: write("a"))
        write("shu", "hello")
        b = a.depends_on_file("shu")
        assert b in a.prerequisites

    def test_depends_on_params(self):
        a = ppg.FileGeneratingJob("out/A", lambda: write("a"))
        b = a.depends_on_params(23)
        assert b.job_id == "PIout/A"
        assert b.parameters == 23
        assert b in a.prerequisites

    def test_parameter_invariant_twice_different_values(self):
        ppg.ParameterInvariant("a", (1, 2, 3))
        with pytest.raises(ValueError):
            ppg.ParameterInvariant("a", (1, 2, 4))

    def test_parameter_invariant_twice_different_accepts_func(self):
        def accept_as_unchanged(old):
            return True

        ppg.ParameterInvariant("a", (1, 2, 3))
        with pytest.raises(ValueError):
            ppg.ParameterInvariant("a", (1, 2, 3), accept_as_unchanged)

    def test_parameter_dependency_accepts_as_unchanged(self, new_pipegraph):
        write("out/A", "x")
        job = ppg.FileGeneratingJob("out/A", lambda: append("out/A", "A"))
        p = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(p)
        ppg.run_pipegraph()
        assert read("out/A") == "A"  # invalidation unlinks!

        new_pipegraph.new_pipegraph()

        def is_prefix(new):
            def inner(old):
                write("inner_check", "yes")
                if len(old) > len(new):
                    return False
                for ii in range(len(old)):
                    if new[ii] != old[ii]:
                        return False
                return True

            return inner

        job = ppg.FileGeneratingJob("out/A", lambda: append("out/A", "A"))
        param = (1, 2, 3, 4)
        p = ppg.ParameterInvariant(
            "myparam", param, accept_as_unchanged_func=is_prefix(param)
        )
        job.depends_on(p)
        ppg.run_pipegraph()
        assert read("out/A") == "A"  # no change
        assert read("inner_check") == "yes"

    def test_filetime_dependency(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        write(ftfn, "hello")  # same content, different time

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shu"
        )  # job does not get rerun - filetime invariant is now filechecksum invariant...

    def test_filechecksum_dependency_raises_on_too_short_a_filename(self):
        with pytest.raises(ValueError):
            ppg.RobustFileChecksumInvariant("a")

        with pytest.raises(ValueError):
            ppg.RobustFileChecksumInvariant("sh")
        ppg.RobustFileChecksumInvariant("shu")

    def test_filechecksum_dependency(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        # logging.info("NOW REWRITE")
        write(ftfn, "hello")  # same content, different time

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        # time.sleep(1) #we don't care about the time, size should be enough...
        write(ftfn, "hello world!!")  # different time
        time.sleep(1)  # give the file system a second to realize the change.

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shushu"  # job does get rerun

    def test_robust_filechecksum_invariant(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...

    def test_robust_filechecksum_invariant_after_normal(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shu"  # job does not get rerun...
        assert read("out/sentinel") == "2"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.RobustFileChecksumInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        assert read("out/sentinel") == "2"  # job does not get rerun...
        print("now it counts")
        ppg.run_pipegraph()
        assert read("out/sentinel") == "2"  # job does not get rerun...
        assert read(of) == "shu"  # job does not get rerun...

    def test_file_invariant_with_md5sum(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()
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

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert read(of) == "shushu"  # job does not get rerun, md5sum did not change...

        t = time.time() - 100  # force a file time mismatch
        os.utime(
            ftfn, (t, t)
        )  # I must change the one on the actual file, otherwise the 'size+filetime is the same' optimization bytes me

        new_pipegraph.new_pipegraph()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        assert (
            read(of) == "shushushu"
        )  # job does get rerun, md5sum and file time mismatch
        assert os.stat(ftfn)[stat.ST_MTIME] == os.stat(ftfn + ".md5sum")[stat.ST_MTIME]

    def test_invariant_dumping_on_job_failure(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()

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
        new_pipegraph.new_pipegraph()
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

    def test_invariant_dumping_on_graph_exception(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()

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
        new_pipegraph.new_pipegraph()
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

    def test_job_not_setting_invalidated_after_was_invalidated_raises(self):
        class BadJob(ppg.FileGeneratingJob):
            def invalidated(self, reason):
                pass

        BadJob("out/A", lambda: write("out/A", "a"))
        with pytest.raises(ppg.JobContractError):
            ppg.run_pipegraph()

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

    def test_invariant_loading_issues_on_value_catastrophic(self):
        a = ppg.DataLoadingJob("a", lambda: 5)
        b = ppg.FileGeneratingJob("out/b", lambda: write("out/b", "b"))
        b.ignore_code_changes()
        b.depends_on(a)
        write("out/b", "a")
        import pickle

        with open(ppg.util.global_pipegraph.invariant_status_filename, "wb") as op:
            pickle.dump(a.job_id, op, pickle.HIGHEST_PROTOCOL)
            op.write(b"This breaks")
        with pytest.raises(ppg.PyPipeGraphError):
            ppg.run_pipegraph()
        assert read("out/b") == "a"  # job was not run

    def test_invariant_loading_issues_on_value_undepickableclass(self):
        import tempfile
        import pickle

        ppg.util.global_pipegraph.quiet = False

        # make sure Undepickable is Undepickable
        with tempfile.TemporaryFile("wb+") as tf:
            o = Undepickable()
            pickle.dump(o, tf, pickle.HIGHEST_PROTOCOL)
            with pytest.raises(pickle.UnpicklingError):
                tf.seek(0, 0)
                pickle.load(tf)

        a = ppg.ParameterInvariant("a", 5)
        b = ppg.FileGeneratingJob("out/b", lambda: write("out/b", "b"))
        b.ignore_code_changes()
        c = ppg.ParameterInvariant("c", 23)
        b.depends_on(a)
        write("out/b", "a")

        with open(ppg.util.global_pipegraph.invariant_status_filename, "wb") as op:
            pickle.dump(a.job_id, op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(Undepickable(), op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(c.job_id, op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(23, op, pickle.HIGHEST_PROTOCOL)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()
        assert read("out/b") == "b"  # job was run
        print(ppg.util.global_pipegraph.invariant_status)
        assert a.job_id in ppg.util.global_pipegraph.invariant_loading_issues
        assert ppg.util.global_pipegraph.invariant_status["PIc"] == 23

    def test_invariant_loading_issues_on_key(self):
        a = ppg.DataLoadingJob("a", lambda: 5)
        b = ppg.FileGeneratingJob("out/b", lambda: write("out/b", "b"))
        b.ignore_code_changes()
        b.depends_on(a)
        write("out/b", "a")

        with open(ppg.util.global_pipegraph.invariant_status_filename, "wb") as op:
            op.write(b"key breaks already")
            op.write(b"This breaks")
        with pytest.raises(ppg.PyPipeGraphError):
            ppg.run_pipegraph()
        assert read("out/b") == "a"  # job was not run


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

        assertRaises(TypeError, inner)

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
        with pytest.raises(ppg.JobContractError):
            ppg.FunctionInvariant("A", lambda: "b")  # raises ValueError

        def sha():
            def shu():
                return "b"

            return shu

        ppg.FunctionInvariant("B", sha())
        ppg.FunctionInvariant("B", sha())

    def test_instance_functions_ok(self):
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
        j1 = y.get_job()
        j2 = y.get_job()
        assert j1 is j2
        assert j1.callback is j2.callback

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
            __doc__ = "File:stat.py starting at line 22)\nHello world"
            im_func = "cyfunction shu"
            im_class = MockImClass

            def __call__(self):
                pass

            def __repr__(self):
                return "cyfunction mockup"

        class MockCythonWithoutFileInfo:
            __doc__ = "Hello world"
            im_func = "cyfunction shu"
            im_class = MockImClass

            def __call__(self):
                pass

            def __repr__(self):
                return "cyfunction mockup"

        c = MockCython()
        c2 = MockCythonWithoutFileInfo
        mi = MockImClass()
        stat.MockIM = mi
        c.im_class = mi

        assert len(ppg.util.global_pipegraph.func_hashes) == 0
        a = ppg.FunctionInvariant("test", c)
        a._get_invariant(None, [])
        assert len(ppg.util.global_pipegraph.func_hashes) == 0

        b = ppg.FunctionInvariant("test2", c2)
        with pytest.raises(ValueError):
            b._get_invariant(None, [])
        assert len(ppg.util.global_pipegraph.func_hashes) == 0

        assert ppg.job.function_to_str(c).endswith("stat.py 22")
        assert ppg.FunctionInvariant.get_cython_source(c) == "return mode & 0o7777"

    def test_buildin_function(self):
        a = ppg.FunctionInvariant("a", open)
        assert "<built-in" in str(a)

    def test_function_invariant_non_function(self):
        class CallMe:
            def __call__(self):
                raise ValueError()

        a = ppg.FunctionInvariant("a", CallMe)
        with pytest.raises(ValueError):
            a._get_invariant([], None)

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

    def test_function_to_str_builtin(self):
        assert ppg.job.function_to_str(open) == "<built-in function open>"

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
    def test_input_checking(self):
        with pytest.raises(TypeError):
            ppg.MultiFileInvariant("out/A", lambda: write("out/A", "A"))
        with pytest.raises(TypeError):
            ppg.MultiFileInvariant(34, lambda: write("out/A", "A"))
        with pytest.raises(TypeError):
            alist = ["out/A", "out/B"]
            ppg.MultiFileInvariant((x for x in alist), lambda: write("out/A", "A"))
        with pytest.raises(ValueError):
            ppg.MultiFileInvariant(["out/A", "out/A"], lambda: write("out/A", "A"))
        with pytest.raises(ValueError):
            ppg.MultiFileInvariant([], lambda: write("out/A", "A"))

    def test_new_raises_unchanged(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])

        def inner():
            jobA.get_invariant(False, {})

        assertRaises(ppg.NothingChanged, inner)

    def test_no_raise_on_no_change(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs

    def test_filetime_changed_contents_the_same(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs = e.new_value
        subprocess.check_call(["touch", "--date=2004-02-29", "out/b"])
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
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
        except ppg.NothingChanged as e:
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
        except ppg.NothingChanged as e:
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
        except ppg.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        write("out2/b", "world")
        jobB = ppg.MultiFileInvariant(["out2/a", "out2/b"])

        def inner():
            jobB.get_invariant(False, {jobA.job_id: cs})

        assertRaises(ppg.NothingChanged, inner)

    def test_rehome_and_change(self):
        write("out/a", "hello")
        write("out/b", "world")
        jobA = ppg.MultiFileInvariant(["out/a", "out/b"])
        try:
            jobA.get_invariant(False, {})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
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
        except ppg.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
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
        except ppg.NothingChanged as e:
            cs = e.new_value
        try:
            jobA.get_invariant(cs, {jobA.job_id: cs})
            self.fail("should not be reached")
        except ppg.NothingChanged as e:
            cs2 = e.new_value
        assert cs2 == cs
        os.makedirs("out2")
        write("out2/a", "hello")
        jobB = ppg.MultiFileInvariant(["out2/a"])
        cs3 = jobB.get_invariant(False, {jobA.job_id: cs})
        assert not ([x[3] for x in cs2] == [x[2] for x in cs3])  # checksums changed

    def test_rehome_same_filenames_gives_up(self, new_pipegraph):
        from pathlib import Path

        write("out/counter", "0")
        Path("out/A").mkdir()
        Path("out/B").mkdir()
        Path("out/C").mkdir()
        Path("out/D").mkdir()
        write("out/A/A", "hello")
        write("out/B/A", "world")
        jobA = ppg.MultiFileInvariant(["out/A/A", "out/B/A"])

        def of():
            append("out/counter", "x")
            write("out/x", "ok")

        jobB = ppg.FileGeneratingJob("out/x", of)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/counter") == "0x"
        new_pipegraph.new_pipegraph()
        shutil.move("out/A/A", "out/C/A")
        shutil.move("out/B/A", "out/D/A")
        jobA = ppg.MultiFileInvariant(["out/C/A", "out/D/A"])
        jobB = ppg.FileGeneratingJob("out/x", of)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        assert read("out/counter") == "0xx"


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

    def test_failed_job_kills_those_after(self, new_pipegraph):
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
        new_pipegraph.new_pipegraph()
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

    def test_invariant_violation_redoes_deps_but_not_nondeps(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()
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
        jobC.depends_on(None)
        jobC.depends_on()
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
        jobB = ppg.FileGeneratingJob("out/b", lambda: write("out/B", "b"))
        jobB.depends_on(jobA)

        with pytest.raises(ppg.CycleError):
            jobA.depends_on(jobA)

        with pytest.raises(ppg.CycleError):
            jobA.depends_on(jobB)

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

    def test_invariant_job_depends_on_raises(self):
        from pypipegraph.job import _InvariantJob

        with pytest.raises(ppg.JobContractError):
            _InvariantJob("A").depends_on(ppg.Job("B"))
        with pytest.raises(ppg.JobContractError):
            ppg.FinalJob("A").depends_on(ppg.Job("B"))

    def test_cached_job_depends_on(self):
        class Dummy:
            pass

        o = Dummy()
        jobA = ppg.CachedAttributeLoadingJob("cache/A", o, "a", lambda: 23)
        jobB = ppg.Job("B")
        jobC = ppg.Job("C")
        jobD = ppg.Job("D")
        jobA.depends_on([jobB], jobC, jobD)
        assert jobB not in jobA.prerequisites
        assert jobC not in jobA.prerequisites
        assert jobD not in jobA.prerequisites
        assert jobB in jobA.lfg.prerequisites
        assert jobC in jobA.lfg.prerequisites
        assert jobD in jobA.lfg.prerequisites

    def test_dependency_placeholder(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: write("out/A", "A" + read("out/B"))
        )
        jobB = ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))

        def gen_deps():
            print("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        ppg.run_pipegraph()
        assert read("out/A") == "AB"

    def test_dependency_placeholder2(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: write("out/A", "A" + read("out/B"))
        )

        def gen_deps():
            return ppg.FileGeneratingJob("out/B", lambda: write("out/B", "B"))

        jobA.depends_on(gen_deps)
        ppg.run_pipegraph()
        assert read("out/A") == "AB"

    def test_dependency_placeholder_nested(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: write("out/A", "A" + read("out/B") + read("out/C"))
        )

        def gen_deps2():
            return ppg.FileGeneratingJob("out/C", lambda: write("out/C", "C"))

        def gen_deps():
            return ppg.FileGeneratingJob(
                "out/B", lambda: write("out/B", "B")
            ).depends_on(gen_deps2)

        jobA.depends_on(gen_deps)
        ppg.run_pipegraph()
        assert read("out/A") == "ABC"

    def test_dependency_placeholder_dynamic_auto_invariants(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: write("out/A", "A" + read("out/B"))
        )

        def check_function_invariant():
            write("out/B", "B")
            preqs = ppg.util.global_pipegraph.jobs["out/B"].prerequisites
            for x in preqs:
                if isinstance(x, ppg.FunctionInvariant):
                    break
            else:
                raise ValueError("no function invariant for gen_deps created job")

        def gen_deps():
            jobB = ppg.FileGeneratingJob("out/B", check_function_invariant)
            print("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        ppg.run_pipegraph()
        assert read("out/A") == "AB"


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
