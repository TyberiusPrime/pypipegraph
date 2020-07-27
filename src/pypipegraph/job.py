from __future__ import print_function

"""
"""

License = """
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

import pathlib
import re
import os
import stat
import sys
import dis
import inspect
import shutil
import hashlib
import traceback
import platform
import time
import six
import pickle
from io import StringIO
from pathlib import Path
from . import ppg_exceptions
from . import util

is_pypy = platform.python_implementation() == "PyPy"
module_type = type(sys)
checksum_file = util.checksum_file

register_tags = False


class JobList(object):
    """For when you want to return a list of jobs that mostly behaves like a single Job.
    (Ie. when it must have a depends_on() method. Otherwise, a regular list will do fine).
    """

    def __init__(self, jobs):
        jobs = list(jobs)
        for job in jobs:
            if not isinstance(job, Job):
                raise ValueError("%s was not a job object" % (job,))
        self.jobs = set(jobs)

    def __iter__(self):
        for job in self.jobs:
            yield job

    def __add__(self, other_job):
        if isinstance(other_job, list):
            other_job = JobList(other_job)

        def iter():
            for job in self.jobs:
                yield job
            if isinstance(other_job, Job):
                yield other_job
            else:
                for job in other_job:
                    yield job

        return JobList(iter())

    def __len__(self):
        return len(self.jobs)

    def depends_on(self, *other_job):
        for job in self.jobs:
            job.depends_on(*other_job)

    def __str__(self):
        return "JobList of %i jobs: %s" % (
            len(self),
            ", ".join(str(x) for x in self.jobs),
        )


def was_inited_before(obj, cls):
    if type(obj) is cls:
        res = hasattr(obj, "_init_done")
        obj._init_done = True
        return res
    else:
        return False


def functions_equal(a, b):
    if a is None and b is None:
        return True
    elif a is None or b is None:
        return False
    elif hasattr(a, "__code__") and hasattr(a, "__closure__"):
        if hasattr(b, "__code__") and hasattr(b, "__closure__"):
            return (a.__code__ == b.__code__) and (a.__closure__ == b.__closure__)
        else:
            return False
    else:
        return ~(hasattr(b, "__code__") and hasattr(b, "__closure__"))


class Job(object):
    """Base class for all Jobs - never instanciated itself.

    This class also provides the pipegraph-lifetime singletonizing of Jobs - ie.
    jobs with the same job_id (=name) will be the same object as long as no new pipegraph
    is generated via new_pipegraph()
    """

    @classmethod
    def verify_job_id(cls, job_id):
        if not isinstance(job_id, str):
            if isinstance(job_id, pathlib.Path):
                job_id = str(job_id)
            else:
                raise TypeError(
                    "Job_id must be a string or Path, was %s %s"
                    % (job_id, type(job_id))
                )
        # if '..' in job_id:

        if (
            util.global_pipegraph is None
            or job_id not in util.global_pipegraph.job_id_cache
        ):
            if ".." in job_id or job_id.startswith("/"):
                job_id = Path(job_id).resolve()
                new_job_id = str(os.path.relpath(job_id))  # keep them relative
            else:
                new_job_id = job_id

            if util.global_pipegraph is not None:
                util.global_pipegraph.job_id_cache[job_id] = new_job_id
            else:
                return new_job_id
        return util.global_pipegraph.job_id_cache[job_id]

    def __new__(cls, job_id, *args, **kwargs):
        """Handles the singletonization on the job_id"""
        if util.global_pipegraph is None:
            raise ValueError("Must instanciate a pipegraph before creating any Jobs")
        job_id = cls.verify_job_id(job_id)
        if job_id not in util.global_pipegraph.job_uniquifier:
            util.global_pipegraph.job_uniquifier[job_id] = object.__new__(cls)
            util.global_pipegraph.job_uniquifier[job_id].job_id = job_id
            # doing it later will fail because hash apperantly might be called before init has run?
        else:
            if util.global_pipegraph.job_uniquifier[job_id].__class__ != cls:
                if args and hasattr(args[0], "__code__"):
                    x = (args[0].__code__.co_filename, args[0].__code__.co_firstlineno)
                else:
                    x = ""
                raise ppg_exceptions.JobContractError(
                    "Same job id, different job classes for %s - was %s and %s.\nOld job: %s\n My args: %s %s\n%s"
                    % (
                        job_id,
                        util.global_pipegraph.job_uniquifier[job_id].__class__,
                        cls,
                        str(util.global_pipegraph.job_uniquifier[job_id]),
                        args,
                        kwargs,
                        x,
                    )
                )
        if util.global_pipegraph is None:
            raise ValueError(
                "You must first instanciate a pypipegraph before creating jobs" ""
            )
        return util.global_pipegraph.job_uniquifier[job_id]

    # def __getnewargs__(self):
    # """Provides unpickeling support"""
    # return (self.job_id, )

    def __init__(self, job_id):
        if was_inited_before(self, Job):
            return
        if not hasattr(self, "dependants"):  # test any of the following
            # else: this job was inited before, and __new__ returned an existing instance
            self.job_no = -1
            self._cores_needed = 1
            self.memory_needed = -1
            self.dependants = set()
            self.prerequisites = set()
            self.dependency_callbacks = []  # called by graph.fill_dependency_callbacks
            self.failed = None
            self.error_reason = "no error"
            self.stdout = None
            self.stderr = None
            self.exception = None
            self.was_run = False
            self.was_done_on = set()  # on which worker(s) was this job run?
            self.was_loaded = False
            self.was_invalidated = False
            self.invalidation_count = (
                0  # used to save some time in graph.distribute_invariant_changes
            )
            self.was_cleaned_up = False
            self.always_runs = False
            self.start_time = None
            self.stop_time = None
            self.is_final_job = False
            self.do_cleanup_if_was_never_run = False
            self.invariant_cache = None
            self.is_temp_job = False
            self._is_done = None
            self.do_cache = False
            self._pruned = False
        util.global_pipegraph.add_job(util.global_pipegraph.job_uniquifier[self.job_id])

    # def __call__(self):
    # return self.dependants

    @property
    def cores_needed(self):
        return self._cores_needed

    @cores_needed.setter
    def cores_needed(self, value):
        ok = True
        if not isinstance(value, int):
            ok = False
        else:
            if value < -2 or value == 0:
                ok = False
        if not ok:
            raise ValueError(
                "cores_needed must be a positive integer, or -1 (allow one other job to run) or -2 (use all cores"
            )
        self._cores_needed = value

    def use_cores(self, cores_needed):
        """Set .cores_needed and return self for chaining"""
        self.cores_needed = cores_needed
        return self

    def depends_on(self, *job_joblist_or_list_of_jobs):
        """Declare that this job depends on the ones passed in (which must be Jobs, JobLists or iterables of such).
        This means that this job can only run, if all previous ones have been done sucessfully.
        """
        # if isinstance(job_joblist_or_list_of_jobs, Job):
        # job_joblist_or_list_of_jobs = [job_joblist_or_list_of_jobs]
        if not job_joblist_or_list_of_jobs:  # nothing to do
            return
        if job_joblist_or_list_of_jobs[0] is self:
            raise ppg_exceptions.CycleError(
                "job.depends_on(self) would create a cycle: %s" % (self.job_id)
            )

        for job in job_joblist_or_list_of_jobs:
            if not isinstance(job, Job):
                if hasattr(job, "__iter__") and not isinstance(
                    job, str
                ):  # a nested list
                    self.depends_on(*job)
                    pass
                elif job is None:
                    continue
                elif hasattr(job, "__call__"):
                    self.dependency_callbacks.append(job)
                else:
                    raise ValueError(
                        "Can only depend on Job objects, was: %s" % type(job)
                    )

            else:
                if job.prerequisites is None:  # this happens during a run
                    raise ppg_exceptions.PyPipeGraphError(
                        "Mixing jobs from different piepgraphs not supported.\n%s\n%s"
                        % (self, job)
                    )
                if self in job.prerequisites:
                    raise ppg_exceptions.CycleError(
                        "Cycle adding %s to %s" % (self.job_id, job.job_id)
                    )
                if isinstance(job, FinalJob):
                    raise ppg_exceptions.JobContractError(
                        "No jobs can depend on FinalJobs"
                    )
        for job in job_joblist_or_list_of_jobs:
            if isinstance(
                job, Job
            ):  # skip the lists here, they will be delegated to further calls during the checking...
                if self.prerequisites is None:
                    raise ppg_exceptions.PyPipeGraphError(
                        "Mixing jobs from different piepgraphs not supported"
                    )
                self.prerequisites.add(job)
        return self

    def depends_on_params(self, params):
        """Create a ParameterInvariant(job_id, params) and have this job
        depend on it"""
        p = ParameterInvariant(self.job_id, params)
        self.depends_on(p)
        return p

    def depends_on_func(self, name, func):
        job = FunctionInvariant(self.job_id + "_" + name, func)
        self.depends_on(job)
        return job

    def depends_on_file(self, filename):
        job = FileInvariant(filename)
        self.depends_on(job)
        return job

    def prune(self):
        """Pruns this job (and all that will eventually depend on it) from the pipegraph
        just before execution)"""
        self._pruned = True

    def unprune(self):
        """Revert this job to unpruned state. See prune()"""
        self._pruned = False

    def is_in_dependency_chain(self, other_job, max_depth):
        """check wether the other job is in this job's dependency chain.
        We check at most @max_depth levels, starting with this job (ie.
        max_depth = 2 means this job and it's children).
        Use a -1 for 'unlimited' (up to the maximum recursion depth of python ;))
        """
        if max_depth == 0:
            return False
        if other_job in self.prerequisites:
            return True
        else:
            for preq in self.prerequisites:
                if preq.is_in_dependency_chain(other_job, max_depth - 1):
                    return True
        return False

    def ignore_code_changes(self):
        """Tell the job not to autogenerate a FunctionInvariant for it's callback(s)"""
        pass

    def inject_auto_invariants(self):
        """Create the automagically generated FunctionInvariants if applicable"""
        pass

    def get_invariant(self, old, all_invariant_stati):
        """Retrieve the invariant 'magic cookie' we should store for this Job.
        The job (and it's descendands) will be invalidated if you return anything
        but @old. You may escape this and raise a NothingChanged(new_value) exception,
        then the new_value will be stored, but no invalidation will occur.
        (Example: FileChecksumInvariant jobs test the filetime first. Only if that differs,
        they check the checksum. If that stayed the same, raising NothingChanged((new_filentime, checksum))
        allows us to not check the file again next time

        Invariant return values are cached by this function, please overwrite
        _get_invariant(old) in subclasses.
        """
        if self.invariant_cache is None or self.invariant_cache[0] != old:
            self.invariant_cache = (old, self._get_invariant(old, all_invariant_stati))
        return self.invariant_cache[1]

    def _get_invariant(self, old, all_invariant_stati):
        """The actual workhorse/sub class specific function for get_invariant
        """
        return False

    def is_done(self, depth=0):
        if not self.do_cache or self._is_done is None:
            self._is_done = self.calc_is_done(depth)
        return self._is_done

    def calc_is_done(self, depth=0):
        """Is this Job done ( ie. does it need to be in the execution order)"""
        return True

    def _reset_is_done_cache(self):
        self._is_done = None
        for child in self.dependants:
            child._reset_is_done_cache()

    def is_loadable(self):
        """Is this a job that's modifies the in memory data of our process?"""
        return False

    def load(self):
        """Actually modify the in memory data"""
        if not self.is_loadable():
            raise ValueError("Called load() on a job that was not loadable")
        raise ValueError(  # pragma: no cover
            "Called load() on a j'ob that had is_loadable, but did not overwrite load() as it should"
        )

    def runs_in_worker(self):
        """Is this a job that runs in our worker, ie. in a spawned job"""
        return True

    def modifies_jobgraph(self):
        """Is this a job that can modify the jobgraph at runtime?
        """
        return False

    def invalidated(self, reason=""):
        """This job was invalidated - throw away any existing output for recalculation"""
        util.global_pipegraph.logger.info(
            "%s invalidated called, reason: %s" % (self, reason)
        )
        self.was_invalidated = True
        self.distribute_invalidation()

    def distribute_invalidation(self):
        """Depth first descend to pass invalidated into all Jobs that dependend on this one"""
        for dep in self.dependants:
            if not dep.was_invalidated:
                dep.invalidated(reason="preq invalidated %s" % self)

    def can_run_now(self):
        """Can this job run right now?
        """
        for preq in self.prerequisites:
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable():
                    # was_run is necessary, a filegen job might have already created the file (and written a bit to it),
                    # but that does not mean that it's done enough to start the next one. Was_run means it has returned.
                    # On the other hand, it might have been a job that didn't need to run, then was_invalidated should be false.
                    # or it was a loadable job anyhow, then it doesn't matter.
                    return False  # pragma: no cover - there is a test case, it triggers, but coverage misses it apperantly
                # else:
                # continue  # go and check the next one
            elif preq._pruned:
                pass
            else:
                return False
        return True

    def list_blocks(self):  # pragma: no cover
        """A helper to list what blocked this job from running - debug function"""
        res = []
        for preq in self.prerequisites:
            if preq.is_done():
                if (
                    preq.was_invalidated and not preq.was_run and not preq.is_loadable()
                ):  # see can_run_now for why
                    res.append((str(preq), "not run"))
                else:
                    pass
            else:
                if preq.was_run:
                    if preq.was_cleaned_up:
                        res.append(
                            (str(preq), "not done - but was run! - after cleanup")
                        )
                    else:
                        res.append((str(preq), "not done - but was run! - no cleanup"))
                else:
                    res.append((str(preq), "not done"))
                break
                # return False
        return res

    def run(self):  # pragma: no cover
        """Do the actual work"""
        pass

    def check_prerequisites_for_cleanup(self):
        """If for one of our prerequisites, all dependands have run, we can
        call it's cleanup function (unload data, remove tempfile...)
        """
        for preq in self.prerequisites:
            all_done = True
            for dep in preq.dependants:
                if dep._pruned:
                    continue
                elif dep.failed or (not dep.was_run) or not preq.is_done():
                    all_done = False
                    break
            if all_done:
                preq.cleanup()
                preq.was_cleaned_up = True

    def cleanup(self):
        """Cleanup after all your direct dependands have finished running"""
        pass

    def __eq__(self, other):
        """Jobs are only equal if they are the same object"""
        return other is self

    def __hash__(self):
        """We can simply hash on our job_id"""
        return hash(self.job_id)

    def __add__(self, other_job):
        """Creates JobLists from two jobs
        """

        def iter():
            yield self
            for job in other_job:
                yield job

        return JobList(iter())

    def __iter__(self):
        yield self

    def __str__(self):
        if hasattr(self, "callback"):
            return "%s (%s\n%s:%s\n%i)" % (
                self.__class__.__name__,
                self.job_id,
                # id(self),
                self.callback.__code__.co_filename,
                self.callback.__code__.co_firstlineno,
                self.cores_needed,
            )
        else:
            return "%s (job_id=%s,id=%s)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
            )

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.job_id)


class _InvariantJob(Job):
    """common code for all invariant jobs"""

    def __init__(self, *args, **kwargs):
        if was_inited_before(self, _InvariantJob):
            return
        super().__init__(*args, **kwargs)

    def depends_on(self, *job_joblist_or_list_of_jobs):
        raise ppg_exceptions.JobContractError("Invariants can't have dependencies")

    def runs_in_worker(self):
        return False


def get_cython_filename_and_line_no(cython_func):
    first_doc_line = cython_func.__doc__.split("\n")[0]
    if not first_doc_line.startswith("File:"):
        raise ValueError(
            "No file/line information in doc string. Make sure your cython is compiled with -p (or #embed_pos_in_docstring=True atop your pyx"
        )
    line_no = int(
        first_doc_line[
            first_doc_line.find("starting at line ")
            + len("starting at line ") : first_doc_line.find(")")
        ]
    )

    # find the right module
    module_name = cython_func.im_class.__module__
    found = False
    for name in sorted(sys.modules):
        if name == module_name or name.endswith("." + module_name):
            try:
                if (
                    getattr(sys.modules[name], cython_func.im_class.__name__)
                    == cython_func.im_class
                ):
                    found = sys.modules[name]
                    break
            except AttributeError:  # pragma: no cover
                continue
        elif hasattr(sys.modules[name], module_name):
            sub_module = getattr(sys.modules[name], module_name)
            try:  # pragma: no cover
                if (
                    getattr(sub_module, cython_func.im_class.__name__)
                    == cython_func.im_class
                ):
                    found = sys.moduls[name].sub_module
                    break
            except AttributeError:
                continue
    if not found:  # pragma: no cover
        raise ValueError("Could not find module for %s" % cython_func)
    filename = found.__file__.replace(".so", ".pyx").replace(
        ".pyc", ".py"
    )  # pyc replacement is for mock testing
    return filename, line_no


def function_to_str(func):
    if str(func).startswith("<built-in function"):
        return "%s" % func
    elif hasattr(func, "im_func") and (
        "cyfunction" in repr(func.im_func)
        or ("<built-in function" in repr(func.im_func))
    ):
        return "%s %i" % get_cython_filename_and_line_no(func)
    else:
        return "%s %i" % (
            func.__code__.co_filename if func else "None",
            func.__code__.co_firstlineno if func else 0,
        )


class FunctionInvariant(_InvariantJob):
    """FunctionInvariant detects (bytecode) changes in a python function,
    currently via disassembly"""

    def __init__(self, job_id, function, absolute_path=None):
        self.verify_arguments(self.job_id, function)
        if was_inited_before(self, FunctionInvariant):
            return
        super().__init__(job_id)
        self.function = function
        self.absolute_path = absolute_path

    def verify_arguments(self, job_id, function):
        if not hasattr(function, "__call__") and function is not None:
            raise ValueError("%s function was not a callable (or None)" % job_id)
        if hasattr(self, "function") and not functions_equal(function, self.function):
            raise ppg_exceptions.JobContractError(
                "FunctionInvariant %s created twice with different functions: \n%s\n%s"
                % (job_id, function_to_str(function), function_to_str(self.function))
            )

    def __str__(self):
        if (
            hasattr(self, "function")
            and self.function
            and hasattr(self.function, "__code__")
        ):  # during creating, __str__ migth be called by a debug function before function is set...
            return "%s (job_id=%s,id=%s\n Function: %s:%s)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
                self.function.__code__.co_filename,
                self.function.__code__.co_firstlineno,
            )
        elif hasattr(self, "function") and str(self.function).startswith(
            "<built-in function"
        ):
            return "%s (job_id=%s,id=%s, Function: %s)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
                self.function,
            )
        else:
            return "%s (job_id=%s,id=%s, Function: None)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
            )

    @classmethod
    def _get_invariant_from_non_python_function(cls, function):
        if str(function).startswith("<built-in function"):
            return str(function)
        elif hasattr(function, "im_func") and (
            "cyfunction" in repr(function.im_func)
            or repr(function.im_func).startswith("<built-in function")
        ):
            return cls.get_cython_source(function)
        else:
            # print(repr(function))
            # print(repr(function.im_func))
            raise ValueError("Can't handle this object %s" % function)

    @classmethod
    def _get_func_hash(cls, key, function):
        if not util.global_pipegraph or key not in util.global_pipegraph.func_hashes:
            source = inspect.getsource(function).strip()
            # cut off function definition / name, but keep parameters
            if source.startswith("def"):
                source = source[source.find("(") :]
            # filter doc string
            if function.__doc__:
                for prefix in ['"""', "'''", '"', "'"]:
                    if prefix + function.__doc__ + prefix in source:
                        source = source.replace(prefix + function.__doc__ + prefix, "",)
            value = (
                source,
                cls.dis_code(function.__code__, function),
            )
            if not util.global_pipegraph:
                return value
            util.global_pipegraph.func_hashes[key] = value

        return util.global_pipegraph.func_hashes[key]

    @classmethod
    def _hash_function(cls, function):
        if not hasattr(function, "__code__"):
            return cls._get_invariant_from_non_python_function(function)
        key = id(function.__code__)
        new_source, new_funchash = cls._get_func_hash(key, function)
        new_closure = cls.extract_closure(function)
        return new_source, new_funchash, new_closure

    def _get_invariant(self, old, all_invariant_stati, version_info=sys.version_info):
        if self.function is None:
            # since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
            return None
        if not hasattr(self.function, "__code__"):
            return self._get_invariant_from_non_python_function(self.function)
        new_source, new_funchash, new_closure = self._hash_function(self.function)
        return self._compare_new_and_old(new_source, new_funchash, new_closure, old)

    @staticmethod
    def _compare_new_and_old(new_source, new_funchash, new_closure, old):
        new = {
            "source": new_source,
            str(sys.version_info[:2]): (new_funchash, new_closure),
        }

        if isinstance(old, dict):
            pass  # the current style
        elif isinstance(old, tuple):
            # the previous style.
            old_funchash = old[2]
            old_closure = old[3]
            old = {
                # if you change python version and pypipegraph at the same time, you're out of luck and will possibly rebuild
                str(sys.version_info[:2]): (old_funchash, old_closure,)
            }
        elif isinstance(old, str):
            # the old old style, just concatenated.
            old = {"old": old}
            new["old"] = new_funchash + new_closure
        elif old is False:  # never ran before
            return new
        elif old is None:  # if you provided a None type instead of a function, you will run into this
            return new
        else:  # pragma: no cover
            raise ValueError(
                "Could not understand old FunctionInvariant invariant. Was Type(%s): %s"
                % (type(old), old)
            )
        unchanged = False
        for k in set(new.keys()).intersection(old.keys()):
            if k != "_version" and new[k] == old[k]:
                unchanged = True
        out = old.copy()
        out.update(new)
        out[
            "_version"
        ] = 3  # future proof, since this is *at least* the third way we're doing this
        if "old" in out:
            del out["old"]
        if unchanged:
            raise ppg_exceptions.NothingChanged(out)
        return out

    @staticmethod
    def extract_closure(function):
        """extract the bound variables from a function into a string representation"""
        try:
            closure = function.func_closure
        except AttributeError:
            closure = function.__closure__
        output = ""
        if closure:
            for name, cell in zip(function.__code__.co_freevars, closure):
                # we ignore references to self - in that use case you're expected
                # to make your own ParameterInvariants, and we could not detect
                # self.parameter anyhow (only self would be bound)
                # we also ignore bound functions - their address changes
                # every run.
                # IDEA: Make this recursive (might get to be too expensive)
                try:
                    if (
                        name != "self"
                        and not hasattr(cell.cell_contents, "__code__")
                        and not isinstance(cell.cell_contents, module_type)
                    ):
                        if isinstance(cell.cell_contents, dict):
                            x = repr(sorted(list(cell.cell_contents.items())))
                        elif isinstance(cell.cell_contents, set) or isinstance(
                            cell.cell_contents, frozenset
                        ):
                            x = repr(sorted(list(cell.cell_contents)))
                        else:
                            x = repr(cell.cell_contents)
                        if (
                            "at 0x" in x
                        ):  # if you don't have a sensible str(), we'll default to the class path. This takes things like <chipseq.quality_control.AlignedLaneQualityControl at 0x73246234>.
                            x = x[: x.find("at 0x")]
                        if "id=" in x:  # pragma: no cover - defensive
                            print(x)
                            raise ValueError("Still an issue, %s", repr(x))
                        output += "\n" + x
                except ValueError as e:  # pragma: no cover - defensive
                    if str(e) == "Cell is empty":
                        pass
                    else:
                        raise
        return output

    inner_code_object_re = re.compile(
        r"(<code\sobject\s<?[^>]+>?\sat\s0x[a-f0-9]+[^>]+)"
        + "|"
        + "(<code\tobject\t<[^>]+>,\tfile\t'[^']+',\tline\t[0-9]+)"  # that's the cpython way  # that's how they look like in pypy. More sensibly, actually
    )

    @classmethod
    def dis_code(cls, code, function, version_info=sys.version_info):
        """'dissassemble' python code.
        Strips lambdas (they change address every execution otherwise),
        but beginning with 3.7 these are actually included
        """

        out = StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = out
            dis.dis(code)
        finally:
            sys.stdout = old_stdout
        discode = out.getvalue().split("\n")
        # now, eat of the line nos, if there are any
        res = []
        for row in discode:
            row = row.split()
            res.append("\t".join(row[1:]))
        res = "\n".join(res)
        res = cls.inner_code_object_re.sub("lambda", res)
        if function and hasattr(function, "__qualname__"):
            res = res.replace(function.__qualname__, "<func name ommited>")
        # beginning with  version 3.7, this piece of code is obsolete,
        # since dis does depth descend by itself way.
        if version_info < (3, 7):
            for ii, constant in enumerate(code.co_consts):
                if hasattr(constant, "co_code"):
                    res += "inner no %i" % ii
                    res += cls.dis_code(constant, None)
        return res

    @staticmethod
    def get_cython_source(cython_func):
        """Attemp to get the cython source for a function.
        Requires cython code to be compiled with -p or #embed_pos_in_docstring=True in the source file

        Unfortunatly, finding the right module (to get an absolute file path) is not straight forward,
        we inspect all modules in sys.module, and their children, but we might be missing sub-sublevel modules,
        in which case we'll need to increase search depth
        """

        # check there's actually the file and line no documentation
        filename, line_no = get_cython_filename_and_line_no(cython_func)

        # load the source code
        op = open(filename, "rb")
        d = op.read().decode("utf-8").split("\n")
        op.close()

        # extract the function at hand, minus doc string
        remaining_lines = d[line_no - 1 :]  # lines start couting at 1
        first_line = remaining_lines[0]
        first_line_indent = len(first_line) - len(first_line.lstrip())
        start_tags = '"""', "'''"
        start_tag = False
        for st in start_tags:
            if first_line.strip().startswith(st):
                start_tag = st
                break
        if start_tag:  # there is a docstring
            text = "\n".join(remaining_lines).strip()
            text = text[3:]  # cut of initial ###
            text = text[text.find(start_tag) + 3 :]
            remaining_lines = text.split("\n")
        last_line = len(remaining_lines)
        for ii, line in enumerate(remaining_lines):
            line_strip = line.strip()
            if line_strip:
                indent = len(line) - len(line_strip)
                if indent < first_line_indent:
                    last_line = ii
                    break
        return "\n".join(remaining_lines[:last_line]).strip()


class ParameterInvariant(_InvariantJob):
    """ParameterInvariants encapsulate smalling parameters, thresholds etc. that your work-jobs
    depend on. They prefix their job_id with 'PI' so given
    a = FileGeneratingJob("A")
    you can simply say
    a.depends_on(pypipegraph.ParameterInvariant('A', (my_threshold_value)))

    In the special case that you need to extend a parameter, but the (new) default is the old behaviour,
    so no recalc is necessary, you can pass @accept_as_unchanged_func
    accept_as_unchanged_func will be called with the invariant from the last run,
    and you need to return True if you want to accept it.
    """

    def __new__(cls, job_id, *parameters, **kwargs):
        job_id = "PI" + cls.verify_job_id(job_id)
        return Job.__new__(cls, job_id)

    def __init__(self, job_id, parameters, accept_as_unchanged_func=None):
        self.verify_arguments(self.job_id, parameters, accept_as_unchanged_func)
        if was_inited_before(self, ParameterInvariant):
            return
        super().__init__(self.job_id)
        self.parameters = parameters
        self.accept_as_unchanged_func = accept_as_unchanged_func

    def verify_arguments(self, job_id, parameters, accept_as_unchanged_func=None):
        if hasattr(self, "parameters"):
            if self.parameters != parameters:
                raise ValueError(
                    "ParameterInvariant %s defined twice with different parameters"
                    % job_id
                )
            if self.accept_as_unchanged_func != accept_as_unchanged_func:
                raise ValueError(
                    "ParameterInvariant %s defined twice with different accept_as_unchanged_func"
                )

    def _get_invariant(self, old, all_invariant_stati):
        if self.accept_as_unchanged_func is not None:
            if self.accept_as_unchanged_func(old):
                raise ppg_exceptions.NothingChanged(self.parameters)
        return self.parameters


class RobustFileChecksumInvariant(_InvariantJob):
    """ Invalidates when the (md5) checksum of a file changed.
    Checksum only get's recalculated if the file modification time changed.
    RobustFileChecksumInvariant can find a file again if it was moved
    (but not if it was renamed)
    """

    def __init__(self, filename):
        if was_inited_before(self, RobustFileChecksumInvariant):
            return
        super().__init__(filename)
        if len(self.job_id) < 3:
            raise ValueError(
                "This is probably not the filename you intend to use: {}".format(
                    filename
                )
            )
        self.input_file = self.job_id
        self.filenames = [filename]

    def _get_invariant(self, old, all_invariant_stati):
        if (
            old
        ):  # if we have something stored, this acts like a normal FileChecksumInvariant
            return self._get_invariant_basic(old, all_invariant_stati)
        else:
            basename = os.path.basename(self.input_file)
            st = util.stat(self.input_file)
            filetime = st[stat.ST_MTIME]
            filesize = st[stat.ST_SIZE]
            checksum = self.checksum()
            for job_id in all_invariant_stati:
                if os.path.basename(job_id) == basename:  # could be a moved file...
                    old = all_invariant_stati[job_id]
                    if isinstance(old, tuple):
                        if len(old) == 2:
                            (
                                old_filesize,
                                old_chksum,
                            ) = old  # pragma: no cover - upgrade from older pipegraph and move at the same time
                        else:
                            dummy_old_filetime, old_filesize, old_chksum = old
                        if old_filesize == filesize:
                            if (
                                old_chksum == checksum
                            ):  # don't check filetime, if the file has moved it will have changed
                                # print("checksum hit %s" % self.input_file)
                                raise ppg_exceptions.NothingChanged(
                                    (filetime, filesize, checksum)
                                )
            # no suitable old job found.
            return (filetime, filesize, checksum)

    def _get_invariant_basic(self, old, all_invariant_stati):
        st = util.stat(self.input_file)
        filetime = st[stat.ST_MTIME]
        filesize = st[stat.ST_SIZE]
        try:
            if not old or old == filetime or old[1] != filesize or old[0] != filetime:
                chksum = self.checksum()
                if old == filetime:  # we converted from a filetimeinvariant
                    raise ppg_exceptions.NothingChanged(
                        (filetime, filesize, chksum)
                    )  # pragma: no cover
                elif old and old[2] == chksum:
                    raise ppg_exceptions.NothingChanged((filetime, filesize, chksum))
                else:
                    return filetime, filesize, chksum
            else:
                return old
        except TypeError:  # pragma: no cover could not parse old tuple... possibly was an FileTimeInvariant before...
            chksum = self.checksum()
            # print ('type error', self.job_id)
            return filetime, filesize, chksum

    def checksum(self):
        md5_file = self.input_file + ".md5sum"
        if os.path.exists(md5_file):
            st = util.stat(self.input_file)
            st_md5 = util.stat(md5_file)
            if st[stat.ST_MTIME] == st_md5[stat.ST_MTIME]:
                with open(md5_file, "rb") as op:
                    return op.read()
            else:
                checksum = self._calc_checksum()
                with open(md5_file, "wb") as op:
                    op.write(checksum.encode("utf-8"))
                os.utime(md5_file, (st[stat.ST_MTIME], st[stat.ST_MTIME]))
                return checksum
        else:
            return self._calc_checksum()

    def _calc_checksum(self):
        return checksum_file(self.job_id)


FileChecksumInvariant = RobustFileChecksumInvariant
FileTimeInvariant = RobustFileChecksumInvariant
FileInvariant = RobustFileChecksumInvariant


class MultiFileInvariant(Job):
    """A (robust) FileChecksumInvariant that depends
    on a list of files.
    Triggers when files are added or removed,
    or one of the files changes.
    """

    def __new__(cls, filenames, *args, **kwargs):
        if isinstance(filenames, six.string_types):
            raise TypeError(
                "Filenames must be a list (or at least an iterable), not a single string"
            )
        if not hasattr(filenames, "__iter__"):
            raise TypeError("filenames was not iterable")

        if len(set(filenames)) != len(filenames):
            raise ValueError("Duplicated filenames")
        if not filenames:
            raise ValueError("filenames was empty")
        job_id = "_MFC_" + ":".join(sorted(str(x) for x in filenames))
        return Job.__new__(cls, job_id)

    # def __getnewargs__(self):  # so that unpickling works
    # return (self.filenames,)

    def __init__(self, filenames):
        sorted_filenames = list(sorted(str(x) for x in filenames))
        job_id = "_MFC_" + ":".join(sorted_filenames)
        self.verify_arguments(sorted_filenames)
        if was_inited_before(self, MultiFileInvariant):
            return
        Job.__init__(self, job_id)
        self.filenames = sorted_filenames

    def verify_arguments(self, filenames):
        # no need to check for filenames changing - it's a new job at that point
        if not hasattr(self, "filenames"):
            for fn in filenames:
                if not os.path.exists(fn):
                    raise ValueError("File did not exist: %s" % fn)

    def _get_invariant(self, old, all_invariant_stati):
        if not old:
            old = self.find_matching_renamed(all_invariant_stati)
        checksums = self.calc_checksums(old)
        if old is False:
            raise ppg_exceptions.NothingChanged(checksums)
        elif old is None:
            return checksums
        else:
            old_d = {x[0]: x[1:] for x in old}
            checksums_d = {x[0]: x[1:] for x in checksums}
            for fn in self.filenames:
                if old_d[fn][2] != checksums_d[fn][2]:  # checksum mismatch!
                    return checksums
            raise ppg_exceptions.NothingChanged(checksums)

    def find_matching_renamed(self, all_invariant_stati):
        def to_basenames(job_id):
            fp = job_id[len("_MFC_") :].split(":")
            return [os.path.basename(f) for f in fp]

        def to_by_filename(job_id):
            fp = job_id[len("_MFC_") :].split(":")
            return {os.path.basename(f): f for f in fp}

        my_basenames = to_basenames(self.job_id)
        if len(my_basenames) != len(
            set(my_basenames)
        ):  # can't mach if the file names are not distinct.
            return None
        for job_id in all_invariant_stati:
            if job_id.startswith("_MFC_"):
                their_basenames = to_basenames(job_id)
                if my_basenames == their_basenames:
                    mine_by_filename = to_by_filename(self.job_id)
                    old = all_invariant_stati[job_id]
                    new = []
                    if old is not False:
                        for tup in old:
                            fn = tup[0]
                            new_fn = mine_by_filename[os.path.basename(fn)]
                            new_tup = (new_fn,) + tup[1:]
                            new.append(new_tup)
                        return new
        # ok, no perfect match - how about a subset?
        for job_id in all_invariant_stati:
            if job_id.startswith("_MFC_"):
                their_basenames = to_basenames(job_id)
                if (
                    len(set(their_basenames).difference(my_basenames)) == 0
                ) and their_basenames:
                    # less filenames, but otherwise same set...
                    return None
                elif len(set(my_basenames).difference(their_basenames)) == 0:
                    return None

        return False

    def calc_checksums(self, old):
        """return a list of tuples
        (filename, filetime, filesize, checksum)"""
        result = []
        if old:
            old_d = {x[0]: x[1:] for x in old}
        else:
            old_d = {}
        for fn in self.filenames:
            st = os.stat(fn)
            filetime = st[stat.ST_MTIME]
            filesize = st[stat.ST_SIZE]
            if (
                fn in old_d
                and (old_d[fn][0] == filetime)
                and (old_d[fn][1] == filesize)
            ):  # we can reuse the checksum
                result.append((fn, filetime, filesize, old_d[fn][2]))
            else:
                result.append((fn, filetime, filesize, checksum_file(fn)))
        return result


class FileGeneratingJob(Job):
    """Create a single output file of more than 0 bytes."""

    def __init__(self, output_filename, function, rename_broken=False, empty_ok=False):
        """If @rename_broken is set, any eventual outputfile that exists
        when the job crashes will be renamed to output_filename + '.broken'
        (overwriting whatever was there before)
        """
        if was_inited_before(self, FileGeneratingJob):
            return
        output_filename = self.job_id  # was verifyied by job.__new__
        super().__init__(output_filename)
        if not hasattr(function, "__call__"):
            raise ValueError("function was not a callable")
        self.empty_ok = empty_ok
        self.filenames = [
            self.job_id
        ]  # so the downstream can treat this one and MultiFileGeneratingJob identically
        self._check_for_filename_collisions()
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False
        self._is_done_cache = None
        self._was_run = None

    def _check_for_filename_collisions(self):
        for x in self.filenames:
            if (
                x in util.global_pipegraph.filename_collider_check
                and util.global_pipegraph.filename_collider_check[x] is not self
            ):
                raise ValueError(
                    "Two jobs generating the same file: %s %s - %s"
                    % (self, util.global_pipegraph.filename_collider_check[x], x)
                )
            else:
                util.global_pipegraph.filename_collider_check[x] = self

    # the motivation for this chaching is that we do a lot of stat calls. Tens of thousands - and the answer can basically only change
    # when you either run or invalidate the job. This apperantly cuts down about 9/10 of all stat calls
    def get_was_run(self):
        return self._was_run

    def set_was_run(self, value):
        self._was_run = value
        self._is_done_cache = None

    was_run = property(get_was_run, set_was_run)

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + "_func", self.callback))
        else:
            pass

    def calc_is_done(self, depth=0):
        if self._is_done_cache is None:
            if self.empty_ok:
                self._is_done_cache = util.file_exists(self.job_id)
            else:
                self._is_done_cache = util.output_file_exists(self.job_id)
        return self._is_done_cache

    def invalidated(self, reason=""):
        util.global_pipegraph.logger.info(
            "%s invalidated called, reason: %s" % (self, reason)
        )
        try:
            util.global_pipegraph.logger.debug("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
            self._is_done_cache = False
        except OSError:
            pass
        Job.invalidated(self, reason)

    def run(self):
        try:
            try:
                self.callback()
            except TypeError as e:
                if "takes exactly 1 argument (0 given)" in str(
                    e
                ) or " missing 1 required positional argument:" in str(  # python2
                    e
                ):  # python3
                    self.callback(self.job_id)
                else:
                    raise
        except Exception:
            exc_info = sys.exc_info()
            tb = traceback.format_exc()
            sys.stderr.write(tb)
            try:
                if self.rename_broken:
                    shutil.move(self.job_id, self.job_id + ".broken")
                else:
                    util.global_pipegraph.logger.debug("unlinking %s" % self.job_id)
                    os.unlink(self.job_id)
            except (OSError, IOError):
                pass
            six.reraise(*exc_info)
        if self.empty_ok:
            filecheck = util.file_exists
        else:
            filecheck = util.output_file_exists
        if not filecheck(self.job_id):
            raise ppg_exceptions.JobContractError(
                "%s did not create its file %s %s\n.Cwd: %s"
                % (
                    self,
                    self.callback.__code__.co_filename,
                    self.callback.__code__.co_firstlineno,
                    os.path.abspath(os.getcwd()),
                )
            )


class MultiFileGeneratingJob(FileGeneratingJob):
    """Create multiple files - recreate all of them if at least one is missing.
    """

    def __new__(cls, filenames, *args, **kwargs):
        if isinstance(filenames, str):
            raise ValueError(
                "Filenames must be a list (or at least an iterable), not a single string"
            )
        if not hasattr(filenames, "__iter__"):
            raise TypeError("filenames was not iterable")
        filenames = [cls.verify_job_id(f) for f in filenames]

        job_id = ":".join(sorted(filenames))
        res = Job.__new__(cls, job_id)
        res.filenames = filenames
        return res

    # def __getnewargs__(self):  # so that unpickling works
    # return (self.filenames,)

    def __init__(self, filenames, function, rename_broken=False, empty_ok=False):
        """If @rename_broken is set, any eventual outputfile that exists
        when the job crashes will be renamed to output_filename + '.broken'
        (overwriting whatever was there before)
        """
        # filenames = sorted([verify_job_id(f) for f in filenames])
        self.verify_arguments(function, rename_broken, empty_ok)
        if was_inited_before(self, MultiFileGeneratingJob):
            return
        Job.__init__(self, self.job_id)
        self._check_for_filename_collisions()

        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False
        self.empty_ok = empty_ok

    def verify_arguments(self, function, rename_broken, empty_ok):
        if not hasattr(function, "__call__"):
            raise ValueError("function was not a callable")
        if hasattr(self, "callback"):
            if not functions_equal(self.callback, function):
                raise ValueError(
                    "MultiFileGeneratingJob with two different functions"
                )  # todo: test
            if self.rename_broken != rename_broken:
                raise ValueError(
                    "MultiFileGeneratingJob with two different rename_brokens"
                )  # todo: test
            if self.empty_ok != empty_ok:
                raise ValueError(
                    "MultiFileGeneratingJob with two different rename_brokens"
                )  # todo: test

    def calc_is_done(self, depth=0):
        for fn in self.filenames:
            if self.empty_ok:
                if not util.file_exists(fn):
                    return False
            else:
                if not util.output_file_exists(fn):
                    return False
        return True

    def invalidated(self, reason=""):
        util.global_pipegraph.logger.info(
            "%s invalidated called, reason: %s" % (self, reason)
        )
        for fn in self.filenames:
            try:
                util.global_pipegraph.logger.debug("unlinking %s" % self.job_id)
                os.unlink(fn)
            except OSError:
                pass
        Job.invalidated(self, reason)

    def run(self):
        try:
            self.callback()
        except Exception:
            exc_info = sys.exc_info()
            if self.rename_broken:
                for fn in self.filenames:
                    try:
                        shutil.move(fn, str(fn) + ".broken")
                    except IOError:  # pragma: no cover
                        pass
            else:
                for fn in self.filenames:
                    try:
                        util.global_pipegraph.logger.debug("unlinking %s" % fn)
                        os.unlink(fn)
                    except OSError:
                        pass
            six.reraise(*exc_info)
        self._is_done = None
        missing_files = []
        if self.empty_ok:
            filecheck = util.file_exists
        else:
            filecheck = util.output_file_exists
        for f in self.filenames:
            if not filecheck(f):
                missing_files.append(f)
        if missing_files:
            raise ppg_exceptions.JobContractError(
                "%s did not create all of its files.\nMissing were:\n %s"
                % (self.job_id, "\n".join([str(x) for x in missing_files]))
            )

    def runs_in_worker(self):
        return True


class TempFileGeneratingJob(FileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully"""

    def __init__(self, output_filename, function, rename_broken=False):
        if was_inited_before(self, TempFileGeneratingJob):
            return

        FileGeneratingJob.__init__(self, output_filename, function, rename_broken)
        self.is_temp_job = True

    def cleanup(self):
        try:
            # the renaming will already have been done when FileGeneratingJob.run(self) was called...
            # if self.rename_broken:
            # shutil.move(self.job_id, self.job_id + '.broken')
            # else:
            util.global_pipegraph.logger.debug("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except (OSError, IOError):  # pragma: no cover
            pass

    def runs_in_worker(self):
        return True

    def calc_is_done(self, depth=0):
        if os.path.exists(
            self.job_id
        ):  # explicitly not using util.output_file_exists, since there the stat has a race condition - reports 0 on recently closed files
            return True
        else:
            for dep in self.dependants:
                if not dep._pruned:
                    if (not dep.is_done()) and (not dep.is_loadable()):
                        return False
            return True


class MultiTempFileGeneratingJob(MultiFileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully"""

    def __init__(self, filenames, function, rename_broken=False, empty_ok=False):
        if was_inited_before(self, MultiTempFileGeneratingJob):
            return
        super().__init__(filenames, function, rename_broken, empty_ok)
        self.is_temp_job = True

    # def __getnewargs__(self):  # so that unpickling works
    # return (self.filenames,)

    def cleanup(self):
        try:
            # the renaming will already have been done when FileGeneratingJob.run(self) was called...
            # if self.rename_broken:
            # shutil.move(self.job_id, self.job_id + '.broken')
            # else:
            for fn in self.filenames:
                util.global_pipegraph.logger.debug("unlinking (cleanup) %s" % fn)
                os.unlink(fn)
        except (OSError, IOError):  # pragma: no cover
            pass

    def calc_is_done(self, depth=0):
        all_files_exist = True
        for fn in self.filenames:
            all_files_exist = all_files_exist and os.path.exists(fn)
        if (
            all_files_exist
        ):  # explicitly not using util.output_file_exists, since there the stat has a race condition - reports 0 on recently closed files
            return True
        else:
            for dep in self.dependants:
                if (not dep.is_done()) and (not dep.is_loadable()):
                    return False
            return True


class TempFilePlusGeneratingJob(TempFileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully,
    but keep a log file (and rerun if the log file is not there)
    """

    def __init__(self, output_filename, log_filename, function, rename_broken=False):
        if was_inited_before(self, TempFilePlusGeneratingJob):
            return
        if output_filename == log_filename:
            raise ValueError("output_filename and log_filename must be different")
        super().__init__(output_filename, function)
        self.output_filename = output_filename
        self.log_file = log_filename
        self.is_temp_job = True

    def calc_is_done(self, depth=0):
        if not os.path.exists(self.log_file):
            return None
        return super().calc_is_done()

    def run(self):
        super().run()
        if not os.path.exists(self.log_file):
            raise ppg_exceptions.JobContractError(
                "%s did not create it's log file" % self.job_id
            )


class DataLoadingJob(Job):
    """Modify the current (system local) master process with a callback function.
    No cleanup is performed - use AttributeLoadingJob if you want your data to be unloaded"""

    def __init__(self, job_id, callback):
        DataLoadingJob.verify_arguments(self, job_id, callback)
        if was_inited_before(self, DataLoadingJob):
            return

        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def verify_arguments(self, job_id, callback):
        if not hasattr(callback, "__call__"):
            raise ValueError("callback was not a callable")
        if hasattr(self, "callback") and not functions_equal(self.callback, callback):
            raise ValueError(
                "Same DataLoadingJob d,ifferent callbacks?\n%s\n%s\n%s\n%s\n%s\n%s\n"
                % (
                    self.callback,
                    callback,
                    self.callback.__code__,
                    self.__code__,
                    self.callback.__closure__,
                    callback.__closure__,
                )
            )  # todo: test this

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + "_func", self.callback))

    def is_loadable(self):
        return True

    def load(self):
        if self.was_loaded:
            return
        for preq in self.prerequisites:  # load whatever is necessary...
            if preq.is_loadable():
                preq.load()

        start = time.time()
        self.callback()
        end = time.time()
        util.global_pipegraph.logger.debug(
            "Loading time for %s - %.3f" % (self.job_id, end - start)
        )
        self.was_loaded = True

    def calc_is_done(
        self, depth=0
    ):  # delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done(depth=depth + 1):
                return False
        return True


class AttributeLoadingJob(DataLoadingJob):
    """Modify the current master process by loading the return value of a callback
    into an object attribute.

    On cleanup, the attribute is deleted via del
    """

    def __init__(self, job_id, object, attribute_name, callback):
        self.verify_arguments(job_id, object, attribute_name, callback)
        if was_inited_before(self, AttributeLoadingJob):
            return
        self.object = object
        self.attribute_name = attribute_name
        DataLoadingJob.__init__(self, job_id, callback)

    def verify_arguments(self, job_id, object, attribute_name, callback):
        if not hasattr(callback, "__call__"):
            raise ValueError("callback was not a callable")
        if not isinstance(attribute_name, str):
            raise ValueError("attribute_name was not a string")
        if hasattr(self, "object"):
            if self.object is not object:
                raise ppg_exceptions.JobContractError(
                    "Creating AttributeLoadingJob twice with different target objects"
                )
            if not self.attribute_name == attribute_name:
                raise ppg_exceptions.JobContractError(
                    "Creating AttributeLoadingJob twice with different target attributes"
                )

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + "_func", self.callback))

    def load(self):
        if self.was_loaded:
            return
        for preq in self.prerequisites:  # load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        setattr(self.object, self.attribute_name, self.callback())
        self.was_loaded = True

    def is_loadable(self):
        return True

    def calc_is_done(
        self, depth=0
    ):  # delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

    def cleanup(self):
        try:
            delattr(self.object, self.attribute_name)
        except AttributeError:  # pragma: no cover
            # this can happen if you have a messed up DependencyInjectionJob,
            # but it would block the messed up reporting...
            # so we ignore it
            pass

    def __str__(self):
        return "AttributeLoadingJob (job_id=%s,id=%i,target=%i)" % (
            self.job_id,
            id(self),
            id(self.object),
        )


class _GraphModifyingJob(Job):
    """Baseclass for jobs that modify the pipegraph during runtime"""

    def modifies_jobgraph(self):
        return True

    def calc_is_done(self, depth=0):
        return self.was_run


class DependencyInjectionJob(_GraphModifyingJob):
    """Inject additional dependencies into a Job (B) that depends on the DependencyInjectionJob (A).
    B can not run before A, and once A has run, B has additional dependencies.
    For example if you have an aggregation job, but the generating jobs are not known until you have
    queried a webservice. Then your aggregation job would be B, and A would create all the generating
    jobs.
    The callback should report back the jobs it has created - B will automagically depend on those
    after A has run (you don't need to do this yourself).

    The DependencyInjectionJob does it's very best to check wheter you're doing something stupid and
    will raise JobContractErrors if you do.
    """

    def __init__(self, job_id, callback, check_for_dependency_injections=True):
        """@check_for_dependency_injections -
        by default, we check whether you injected correctly,
        but some of these checks are costly so you might wish to optimize
        by setting check_for_dependency_injections=False,
        but injecting into already run jobs and so on might
        create invisible (non exception raising) bugs.
        """
        if not hasattr(callback, "__call__"):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True
        self.check_for_dependency_injections = check_for_dependency_injections

    def run(self):
        # this is different form JobGeneratingJob.run in it's checking of the contract
        util.global_pipegraph.new_jobs = {}
        reported_jobs = self.callback()
        for new_job in list(util.global_pipegraph.new_jobs.values()):
            new_job.inject_auto_invariants()
        if reported_jobs:
            for new_job in reported_jobs:
                for my_dependand in self.dependants:
                    my_dependand.depends_on(new_job)
        # we now need to fill new_jobs.dependants
        # these implementations are much better than the old for loop based ones
        # but still could use some improvements
        # but at least for the first one, I don't see how to remove the remaining loops.
        new_job_set = set(util.global_pipegraph.new_jobs.keys())
        for job in util.global_pipegraph.jobs.values():
            for nw_jobid in new_job_set.intersection(
                [x.job_id for x in job.prerequisites]
            ):
                nw = util.global_pipegraph.new_jobs[nw_jobid]
                if job not in self.dependants:
                    raise ppg_exceptions.JobContractError(
                        "DependencyInjectionJob %s tried to inject %s into %s, "
                        "but %s was not dependand on the DependencyInjectionJob."
                        " It was dependand on %s though (case 0)"
                        % (self, nw, job, job, nw.prerequisites)
                    )
                nw.dependants.add(job)
        # I need to check: All new jobs are now prereqs of my dependands

        # I also need to check that none of the jobs that ain't dependand on me have been injected
        if self.check_for_dependency_injections:
            for job in util.global_pipegraph.jobs.values():
                if job in self.dependants:
                    for new_job in util.global_pipegraph.new_jobs.values():
                        if not job.is_in_dependency_chain(
                            new_job, 5
                        ):  # 1 for the job, 2 for auto dependencies, 3 for load jobs, 4 for the dependencies of load jobs... 5 seems to work in pratice.
                            raise ppg_exceptions.JobContractError(
                                "DependencyInjectionJob %s created a job %s that was not added to the prerequisites of %s (case 1)"
                                % (self.job_id, new_job.job_id, job.job_id)
                            )
                else:  # pragma: no cover - I could not come up with a test case triggering this (2019-01-11)
                    preq_intersection = set(job.prerequisites).intersection(new_job_set)
                    if preq_intersection:
                        raise ppg_exceptions.JobContractError(
                            "DependencyInjectionJob %s created a job %s that was added to the prerequisites of %s, "
                            "but was not dependant on the DependencyInjectionJob (case 2)"
                            % (self.job_id, preq_intersection, job.job_id)
                        )
                    dep_intersection = set(job.prerequisites).intersection(new_job_set)
                    if dep_intersection:
                        raise ppg_exceptions.JobContractError(
                            "DependencyInjectionJob %s created a job %s that was added to the dependants of %s, but was not dependant on the DependencyInjectionJob (case 3)"
                            % (self.job_id, dep_intersection, job.job_id)
                        )

        res = util.global_pipegraph.new_jobs
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        return res


class JobGeneratingJob(_GraphModifyingJob):
    """A Job generating new jobs. The new jobs must be leaves in the sense that no job that existed
    before may depend on them. If that's what you want, see L{DependencyInjectionJob}.
    """

    def __init__(self, job_id, callback):
        if was_inited_before(self, JobGeneratingJob):
            return
        if not hasattr(callback, "__call__"):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True

    def run(self):
        util.global_pipegraph.new_jobs = {}
        self.callback()
        for new_job in list(util.global_pipegraph.new_jobs.values()):
            new_job.inject_auto_invariants()
        # I need to check: All new jobs are now prereqs of my dependands
        # I also need to check that none of the jobs that ain't dependand on me have been injected
        new_job_set = set(util.global_pipegraph.new_jobs.values())
        for job in util.global_pipegraph.jobs.values():
            if new_job_set.intersection(job.prerequisites):
                raise ppg_exceptions.JobContractError(
                    "JobGeneratingJob %s created a job that was added to the prerequisites of %s, which is invalid. Use a DependencyInjectionJob instead, this one might only create 'leaf' nodes"
                    % (self.job_id, job.job_id)
                )
        res = util.global_pipegraph.new_jobs
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        return res


class FinalJob(Job):
    """A final job runs after all other (non final) jobs have run.
    Use these sparringly - they really only make sense for things where you really want to hook
    'after the pipeline has run', everything else realy is better of if you depend on the appropriate job

    FinalJobs are also run on each run - but only if no other job died.
    """

    def __init__(self, job_id, callback):
        if was_inited_before(self, FinalJob):
            return

        Job.__init__(self, self.job_id)
        self.callback = callback
        self.is_final_job = True
        self.do_ignore_code_changes = False
        self.always_runs = True

    def calc_is_done(self, depth=0):
        return self.was_run

    def depends_on(self, *args):
        raise ppg_exceptions.JobContractError(
            "Final jobs can not have explicit dependencies - they run in random order after all other jobs"
        )

    def run(self):
        self.callback()


class PlotJob(FileGeneratingJob):
    """Calculate some data for plotting, cache it in cache/output_filename, and plot from there.
    creates two jobs, a plot_job (this one) and a cache_job (FileGeneratingJob, in self.cache_job),

    To use these jobs, you need to have pyggplot available.
    """

    def __init__(  # noqa:C901
        self,
        output_filename,
        calc_function,
        plot_function,
        render_args=None,
        skip_table=False,
        skip_caching=False,
    ):
        if was_inited_before(self, PlotJob):
            if (
                skip_caching != self.skip_caching
                or skip_table != self.skip_table
                or render_args != self.render_args
            ):
                raise ValueError(
                    "PlotJob(%s) called twice with different parameters" % self.job_id
                )
            return
        output_filename = self.job_id
        if not (
            output_filename.endswith(".png")
            or output_filename.endswith(".pdf")
            or output_filename.endswith(".svg")
        ):
            raise ValueError(
                "Don't know how to create this file %s, must end on .png or .pdf or .svg"
                % output_filename
            )

        self.output_filename = output_filename
        self.filenames = [output_filename]
        self._check_for_filename_collisions()
        self.table_filename = self.output_filename + ".tsv"
        self.calc_function = calc_function
        self.plot_function = plot_function
        self.skip_caching = skip_caching
        self.skip_table = skip_table
        if render_args is None:
            render_args = {}
        self.render_args = render_args
        self._fiddle = None

        import pandas as pd

        if not self.skip_caching:
            self.cache_filename = (
                Path(util.global_pipegraph.cache_folder) / output_filename
            )
            self.cache_filename.parent.mkdir(exist_ok=True, parents=True)

            def run_calc():
                df = calc_function()
                if not isinstance(df, pd.DataFrame):
                    do_raise = True
                    if isinstance(df, dict):  # might be a list dfs...
                        do_raise = False
                        for x in df.values():
                            if not isinstance(x, pd.DataFrame):
                                do_raise = True
                                break
                    if do_raise:
                        raise ppg_exceptions.JobContractError(
                            "%s.calc_function did not return a DataFrame (or dict of such), was %s "
                            % (output_filename, str(df.__class__))
                        )
                try:
                    os.makedirs(os.path.dirname(self.cache_filename))
                except OSError:
                    pass
                of = open(self.cache_filename, "wb")
                pickle.dump(df, of, pickle.HIGHEST_PROTOCOL)
                of.close()

        def run_plot():
            df = self.get_data()
            plot = plot_function(df)
            if not hasattr(plot, "render") and not hasattr(plot, "save"):
                raise ppg_exceptions.JobContractError(
                    "%s.plot_function did not return a plot object (needs to have as render or save function"
                    % (output_filename)
                )
            if hasattr(plot, "pd"):
                plot = plot.pd
            render_args = {}
            if "width" not in render_args and hasattr(plot, "width"):
                render_args["width"] = plot.width
            if "height" not in render_args and hasattr(plot, "height"):
                render_args["height"] = plot.height
            render_args.update(getattr(plot, "render_args", {}))
            render_args.update(self.render_args)
            if self._fiddle:
                self._fiddle(plot)
            if hasattr(plot, "render"):
                plot.render(output_filename, **render_args)
            elif hasattr(plot, "save"):
                plot.save(output_filename, **render_args)
            else:
                raise NotImplementedError("Don't know how to handle this plotjob")

        FileGeneratingJob.__init__(self, output_filename, run_plot)
        Job.depends_on(
            self, ParameterInvariant(self.output_filename + "_params", render_args)
        )

        if not self.skip_caching:
            cache_job = FileGeneratingJob(self.cache_filename, run_calc)
            Job.depends_on(self, cache_job)
            self.cache_job = cache_job

        if not skip_table:

            def dump_table():
                import pandas as pd

                df = self.get_data()
                if isinstance(df, pd.DataFrame):
                    df.to_csv(self.table_filename, sep="\t")
                else:
                    with open(self.table_filename, "w") as op:
                        for key, dframe in df.items():
                            op.write("#%s\n" % key)
                            dframe.to_csv(op, sep="\t")

            table_gen_job = FileGeneratingJob(self.table_filename, dump_table)
            if not self.skip_caching:
                table_gen_job.depends_on(cache_job)
            self.table_job = table_gen_job
        else:
            self.table_job = None

    def add_another_plot(self, output_filename, plot_function, render_args=None):
        """Add another plot job that runs on the same data as the original one (calc only done once)"""

        if render_args is None:
            render_args = {}

        def run_plot():
            df = self.get_data()
            plot = plot_function(df)
            if not hasattr(plot, "render"):
                raise ppg_exceptions.JobContractError(
                    "%s.plot_function did not return a plot with a render function"
                    % (output_filename)
                )
            if "width" not in render_args and hasattr(plot, "width"):
                render_args["width"] = plot.width
            if "height" not in render_args and hasattr(plot, "height"):
                render_args["height"] = plot.height
            plot.render(output_filename, **render_args)

        job = FileGeneratingJob(output_filename, run_plot)
        job.depends_on(
            ParameterInvariant(self.output_filename + "_params", render_args)
        )
        job.depends_on(FunctionInvariant(self.output_filename + "_func", plot_function))
        job.depends_on(self.cache_job)
        return job

    def add_fiddle(self, fiddle_function):
        """Add another function that is called right before the plot is
        rendered with a pyggplot.Plot as the only argument in order to be able
        to 'fiddle' with the plot.
        Please note: if you want to remove an add_fiddle, the plot is only redone if you
        call add_fiddle(None) instead of removing the call altogether
        """
        self._fiddle = fiddle_function
        Job.depends_on(
            self, FunctionInvariant(self.output_filename + "_fiddle", fiddle_function)
        )

    def depends_on(self, *other_jobs):
        # FileGeneratingJob.depends_on(self, other_job)  # just like the cached jobs, the plotting does not depend on the loading of prerequisites
        if self.skip_caching:
            Job.depends_on(self, *other_jobs)
            if self.table_job:
                self.table_job.depends_on(*other_jobs)
        elif (
            hasattr(self, "cache_job") and other_jobs[0] is not self.cache_job
        ):  # activate this after we have added the invariants...
            self.cache_job.depends_on(*other_jobs)
        return self

    def prune(self):
        """Pruns this job (and all that will eventually depend on it) from the pipegraph
        just before execution)"""
        if not self.skip_caching:
            self.cache_job.prune()
        super().prune()

    def unprune(self):
        """Revert this job to unpruned state. See prune()"""
        if not self.skip_caching:
            self.cache_job.unprune()
        super().unprune()

    def use_cores(self, cores_needed):
        if self.skip_caching:
            Job.use_cores(self, cores_needed)
        else:
            self.cache_job.use_cores(cores_needed)
        return self

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            if not self.skip_caching:
                self.cache_job.depends_on(
                    FunctionInvariant(self.job_id + ".calcfunc", self.calc_function)
                )
            else:
                FileGeneratingJob.depends_on(
                    self,
                    FunctionInvariant(self.job_id + ".calcfunc", self.calc_function),
                )
            FileGeneratingJob.depends_on(
                self, FunctionInvariant(self.job_id + ".plotfunc", self.plot_function)
            )
            FileGeneratingJob.depends_on(
                self, FunctionInvariant(self.job_id + ".run_plot", self.callback)
            )  # depend on the run_plot func

    def get_data(self):
        if self.skip_caching:
            return self.calc_function()
        else:
            try:
                of = open(self.cache_filename, "rb")
                df = pickle.load(of)
                of.close()
            except Exception as e:
                raise ValueError(
                    "Unpickling error in file %s - original error was %s"
                    % (self.cache_filename, str(e))
                )
            return df

    def __str__(self):
        return "%s (job_id=%s,id=%s\n Calc function: %s:%s\nPlot function: %s:%s)" % (
            self.__class__.__name__,
            self.job_id,
            id(self),
            self.calc_function.__code__.co_filename,
            self.calc_function.__code__.co_firstlineno,
            self.plot_function.__code__.co_filename,
            self.plot_function.__code__.co_firstlineno,
        )


def CombinedPlotJob(
    output_filename, plot_jobs, facet_arguments, render_args=None, fiddle=None
):
    """Combine multiple PlotJobs into a common (faceted) output plot.
    An empty list means 'no facetting'

    To use these jobs, you need to have pyggplot available.
    """
    output_filename = Job.verify_job_id(output_filename)
    if not (output_filename.endswith(".png") or output_filename.endswith(".pdf")):
        raise ValueError(
            "Don't know how to create this file %s, must end on .png or .pdf"
            % output_filename
        )
    if not plot_jobs:
        raise ValueError("plot_jobs must not be empty")
    for x in plot_jobs:
        if not isinstance(x, PlotJob):
            raise ValueError("all plot_jobs must be PlotJob instances")
    if not isinstance(facet_arguments, (list, dict)):
        raise ValueError(
            "Facet arguments must be a list or a dict to be passed to plot.facet"
        )

    if render_args is None:
        render_args = {"width": 10, "height": 10}

    def plot():
        import pandas as pd

        data = pd.concat([plot_job.get_data() for plot_job in plot_jobs], axis=0)
        plot = plot_jobs[0].plot_function(data)
        if isinstance(facet_arguments, list):
            if facet_arguments:  # empty lists mean no faceting
                plot.facet_wrap(*facet_arguments)
        elif isinstance(facet_arguments, dict):
            plot.facet_wrap(**facet_arguments)
        else:
            raise ValueError("Should not be reached")  # pragma: no cover

        # no need to check plot objects for being plot - the prerequisite
        # PlotJobs would have failed already
        if fiddle:
            fiddle(plot)
        plot.render(output_filename, **render_args)

    job = FileGeneratingJob(output_filename, plot)
    job.depends_on(
        ParameterInvariant(
            output_filename + "_params",
            (
                list(
                    sorted([plot_job.output_filename for plot_job in plot_jobs])
                ),  # so to detect new plot_jobs...
                render_args,
                facet_arguments,
            ),
        )
    )
    job.depends_on(FunctionInvariant(output_filename + "_fiddle", fiddle))
    job.depends_on([plot_job.cache_job for plot_job in plot_jobs])
    job.depends_on(
        FunctionInvariant(
            output_filename + "_plot_combined", plot_jobs[0].plot_function
        )
    )
    return job


class _CacheFileGeneratingJob(FileGeneratingJob):
    """A job that takes the results from it's callback and pickles it.
    data_loading_job is dependend on somewhere"""

    def __init__(self, job_id, calc_function, dl_job, empty_ok=False):
        if was_inited_before(self, _CacheFileGeneratingJob):
            return

        self.empty_ok = empty_ok
        if not hasattr(calc_function, "__call__"):
            raise ValueError("calc_function was not a callable")
        Job.__init__(self, job_id)  # FileGeneratingJob has no benefits for us
        if not hasattr(self, "data_loading_job"):  # only do this the first time...
            self.cache_filename = job_id
            self.callback = calc_function
            self.data_loading_job = dl_job
            self.do_ignore_code_changes = False

    def invalidated(self, reason=""):
        util.global_pipegraph.logger.info(
            "%s invalidated called, reason: %s" % (self, reason)
        )
        try:
            util.global_pipegraph.logger.debug("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except OSError:
            pass
        self.was_invalidated = True
        if not self.data_loading_job.was_invalidated:
            self.data_loading_job.invalidated(reason)
        self._is_done_cache = False
        # Job.invalidated(self)  # no going back up the dependants... the dataloading job takes care of that

    def run(self):
        data = self.callback()
        op = open(self.cache_filename, "wb")
        pickle.dump(data, op, pickle.HIGHEST_PROTOCOL)
        op.close()


class _CachingJobMixin:
    def depends_on(self, *other_jobs):
        self.lfg.depends_on(*other_jobs)
        return self
        # The loading job itself should not depend on the preqs
        # because then the preqs would even have to be loaded if
        # the lfg had run already in another job
        # and dataloadingpreqs could not be unloaded right away
        # and anyhow, the loading job is so simple it doesn't need
        # anything but the lfg output file
        # return Job.depends_on(self, jobs)

    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()
        self.do_ignore_code_changes = True

    def __del__(self):
        self.lfg = None

    def invalidated(self, reason=""):
        util.global_pipegraph.logger.info(
            "%s invalidated called, reason: %s" % (self, reason)
        )
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)


class CachedAttributeLoadingJob(_CachingJobMixin, AttributeLoadingJob):
    """Like an AttributeLoadingJob, except that the callback value is pickled into
    a file called job_id and reread on the next run"""

    def __new__(cls, job_id, *args, **kwargs):
        return Job.__new__(cls, str(job_id) + "_load")

    def __init__(
        self, cache_filename, target_object, target_attribute, calculating_function
    ):
        cache_filename = self.verify_job_id(cache_filename)
        self.verify_arguments(
            cache_filename, target_object, target_attribute, calculating_function
        )
        if was_inited_before(self, CachedAttributeLoadingJob):
            return

        abs_cache_filename = os.path.abspath(cache_filename)

        def do_load(cache_filename=abs_cache_filename):
            op = open(cache_filename, "rb")
            data = pickle.load(op)
            op.close()
            return data

        AttributeLoadingJob.__init__(
            self, cache_filename + "_load", target_object, target_attribute, do_load
        )
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)

    def verify_arguments(
        self, cache_filename, target_object, target_attribute, calculating_function
    ):
        if not isinstance(target_attribute, str):
            raise ValueError("attribute_name was not a string")
        super().verify_arguments(
            cache_filename, target_object, target_attribute, calculating_function
        )

    def use_cores(self, n):
        self.lfg.use_cores(n)
        return self


class CachedDataLoadingJob(_CachingJobMixin, DataLoadingJob):
    """Like a DataLoadingJob, except that the callback value is pickled into
    a file called job_id and reread on the next run"""

    def __new__(cls, job_id, *args, **kwargs):
        return Job.__new__(
            cls, str(job_id) + "_load"
        )  # plus load, so that the cached data goes into the cache_filename passed to the constructor...

    def __init__(self, cache_filename, calculating_function, loading_function):
        cache_filename = self.verify_job_id(cache_filename)
        self.verify_arguments(cache_filename, calculating_function, loading_function)
        if was_inited_before(self, CachedDataLoadingJob):
            return

        abs_cache_filename = os.path.abspath(cache_filename)

        def do_load(cache_filename=abs_cache_filename):
            op = open(cache_filename, "rb")
            try:
                data = pickle.load(op)
            except Exception as e:
                raise ValueError(
                    "Unpickling error in file %s - original error was %s"
                    % (cache_filename, str(e))
                )
            op.close()
            loading_function(data)

        DataLoadingJob.__init__(
            self, cache_filename + "_load", do_load
        )  # todo: adjust functioninvariant injection
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)
        self.calculating_function = calculating_function
        self.loading_function = loading_function

    def verify_arguments(self, cache_filename, calculating_function, loading_function):
        # super().verify_arguments(self, cache_filename, calculating_function, loading_function)
        if not hasattr(loading_function, "__call__"):
            raise ValueError("loading_function was not a callable")

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            # this job should depend on that, not the lazy filegenerating one...
            Job.depends_on(
                self, FunctionInvariant(self.job_id + "_func", self.loading_function)
            )  # we don't want to depend on 'callback', that's our tiny wrapper, but on the loading_function instead.

    def __str__(self):
        try:
            return (
                "%s (job_id=%s,id=%s\n Calc calcback: %s:%s\nLoad callback: %s:%s)"
                % (
                    self.__class__.__name__,
                    self.job_id,
                    id(self),
                    self.calculating_function.__code__.co_filename,
                    self.calculating_function.__code__.co_firstlineno,
                    self.loading_function.__code__.co_filename,
                    self.loading_function.__code__.co_firstlineno,
                )
            )
        except AttributeError:  # pragma: no cover
            return "%s(job_id=%s, callbacks unset" % (
                self.__class__.__name__,
                self.job_id,
            )

    def use_cores(self, n):
        self.lfg.use_cores(n)
        return self


class MemMappedDataLoadingJob(_CachingJobMixin, DataLoadingJob):
    """Like a DataLoadingJob that returns a numpy array. That array get's stored to a file, and memmapped back in later on.
    Note that it's your job to del your memmapped reference to get it garbage collectable...
    """

    def __new__(cls, job_id, *args, **kwargs):
        if is_pypy:  # pragma: no cover
            raise NotImplementedError(
                "Numpypy currently does not support memmap(), there is no support for MemMappedDataLoadingJob using pypy."
            )
        if isinstance(job_id, pathlib.Path):
            job_id = str(job_id)
        elif not isinstance(job_id, six.string_types):
            raise TypeError("cache_filename/job_id was not a string object")
        return Job.__new__(
            cls, job_id + "_load"
        )  # plus load, so that the cached data goes into the cache_filename passed to the constructor...

    def __init__(self, cache_filename, calculating_function, loading_function, dtype):
        cache_filename = self.verify_job_id(cache_filename)
        self.verify_arguments(
            cache_filename, calculating_function, loading_function, dtype
        )
        if was_inited_before(self, MemMappedDataLoadingJob):
            return
        abs_cache_filename = os.path.abspath(cache_filename)
        self.dtype = dtype

        def do_load(cache_filename=abs_cache_filename):
            import numpy

            data = numpy.memmap(cache_filename, self.dtype, mode="r")
            loading_function(data)

        DataLoadingJob.__init__(self, cache_filename + "_load", do_load)  #

        def do_calc(cache_filename=abs_cache_filename):
            import numpy

            data = calculating_function()
            if not isinstance(data, numpy.ndarray):
                raise ppg_exceptions.JobContractError("Data must be a numpy array")
            if data.dtype != self.dtype:
                raise ppg_exceptions.JobContractError(
                    "Data had wrong dtype. Expected %s, was %s"
                    % (self.dtype, data.dtype)
                )
            mmap = numpy.memmap(cache_filename, self.dtype, "w+", shape=data.shape)
            mmap[:] = data
            mmap.flush()
            del data
            del mmap

        lfg = FileGeneratingJob(cache_filename, do_calc)
        self.lfg = lfg
        Job.depends_on(self, lfg)
        self.calculating_function = calculating_function
        self.loading_function = loading_function

    def verify_arguments(
        self, cache_filename, calculating_function, loading_function, dtype
    ):
        if not hasattr(calculating_function, "__call__"):
            raise ValueError("calculating_function was not a callable")
        if not hasattr(loading_function, "__call__"):
            raise ValueError("loading_function was not a callable")
        # todo: check input against last init

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(
                FunctionInvariant(self.job_id + "_func", self.loading_function)
            )  # we don't want to depend on 'callback', that's our tiny wrapper, but on the loading_function instead.
            self.lfg.depends_on(
                FunctionInvariant(self.job_id + "_calc_func", self.calculating_function)
            )

    def __str__(self):
        return "%s (job_id=%s,id=%s\n Calc calcback: %s:%s\nLoad callback: %s:%s)" % (
            self.__class__.__name__,
            self.job_id,
            id(self),
            self.calculating_function.__code__.co_filename,
            self.calculating_function.__code__.co_firstlineno,
            self.loading_function.__code__.co_filename,
            self.loading_function.__code__.co_firstlineno,
        )


def NotebookJob(notebook_filename, auto_detect_dependencies=True):
    """Run a jupyter notebook.

    Invalidates (if not ignore_code_changes()) if
        - notebook_filename's contents change
        - any file mentioned in the notebook for which we have
            a Job changes / needs to be build

    """
    notebook_name = os.path.basename(notebook_filename)
    if not os.path.exists("cache/notebooks"):
        os.mkdir("cache/notebooks")
    nb_fn_hash = hashlib.md5(notebook_filename.encode("utf-8")).hexdigest()
    sentinel_file = os.path.join(
        "cache", "notebooks", nb_fn_hash + " " + notebook_name + ".html"
    )
    ipy_cache_file = os.path.join("cache", "notebooks", nb_fn_hash + ".ipynb")
    return _NotebookJob(
        [sentinel_file, ipy_cache_file], notebook_filename, auto_detect_dependencies
    )


class _NotebookJob(MultiFileGeneratingJob):
    def __init__(self, files, notebook_filename, auto_detect_dependencies):
        if was_inited_before(self, _NotebookJob):
            return
        sentinel_file, ipy_cache_file = files

        def run_notebook():
            import subprocess

            shutil.copy(notebook_filename, ipy_cache_file)
            p = subprocess.Popen(
                ["runipy", "-o", os.path.abspath(ipy_cache_file), "--no-chdir"],
                cwd=os.path.abspath("."),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = p.communicate()
            if p.returncode != 0:  # pragma: no cover
                raise ValueError(
                    "Ipython notebook %s error return.\nstdout:\n%s\n\nstderr:\n%s"
                    % (notebook_filename, stdout, stderr)
                )
            output_file = open(sentinel_file, "wb")
            p = subprocess.Popen(
                [
                    "ipython",
                    "nbconvert",
                    os.path.abspath(ipy_cache_file),
                    "--to",
                    "html",
                    "--stdout",
                ],
                stdout=output_file,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = p.communicate()
            if p.returncode != 0:  # pragma: no cover
                raise ValueError("Ipython nbconvert error. stderr: %s" % (stderr,))
            output_file.close()

        self.auto_detect_dependencies = auto_detect_dependencies
        self.notebook_filename = notebook_filename
        MultiFileGeneratingJob.__init__(self, files, run_notebook)

    def inject_auto_invariants(self):
        deps = [FileChecksumInvariant(self.notebook_filename)]
        if self.auto_detect_dependencies:
            with open(self.notebook_filename, "r") as op:
                raw_text = op.read()
            for job_name, job in util.global_pipegraph.jobs.items():
                if hasattr(job, "filenames"):
                    for fn in job.filenames:
                        if fn in raw_text:
                            deps.append(job)
        self.depends_on(deps)
