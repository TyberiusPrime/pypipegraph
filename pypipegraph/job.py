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

from . import ppg_exceptions
from . import util
logger = util.start_logging('job')
import re
try:
    import cStringIO
    io = cStringIO
except ImportError:
    import io
import os
import stat
from . import util
import sys
import dis
import shutil
import hashlib
try:
    import cPickle
    pickle = cPickle
except ImportError:
    import pickle
import traceback
import platform;
import time
import six

is_pypy = platform.python_implementation() == 'PyPy'
module_type = type(sys)

register_tags = False


class JobList(object):
    """For when you want to return a list of jobs that mostly behaves like a single Job.
    (Ie. when it must have a depends_on() method. Otherwise, a regular list will do fine).
    """
    def __init__(self, jobs):
        jobs = list(jobs)
        for job in jobs:
            if not isinstance(job, Job):
                raise ppg_exceptions.ValueError("%s was not a job object" % job)
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

    def depends_on(self, other_job):
        for job in self.jobs:
            job.depends_on(other_job)

    def __str__(self):
        return "JobList of %i jobs: %s" % (len(self), ", ".join(str(x) for x in self.jobs))


class Job(object):
    """Base class for all Jobs - never instanciated itself.

    This class also provides the pipegraph-lifetime singletonizing of Jobs - ie.
    jobs with the same job_id (=name) will be the same object as long as no new pipegraph
    is generated via new_pipegraph()
    """

    def __new__(cls, job_id, *args, **kwargs):
        """Handles the singletonization on the job_id"""
        #logger.info("New for %s %s" % (cls, job_id))
        if not isinstance(job_id, str):
            raise ValueError("Job_id must be a string, was %s %s" % (job_id, type(job_id)))
        if not job_id in util.job_uniquifier:
            util.job_uniquifier[job_id] = object.__new__(cls)
            util.job_uniquifier[job_id].job_id = job_id  # doing it later will fail because hash apperantly might be called before init has run?
        else:
            if util.job_uniquifier[job_id].__class__ != cls:
                import types
                if args and hasattr(args[0], '__code__'):
                    x = ( args[0].__code__.co_filename, args[0].__code__.co_firstlineno)
                else:
                    x=''
                raise ppg_exceptions.JobContractError("Same job id, different job classes for %s - was %s and %s.\nOld job: %s\n My args: %s %s\n%s" % (job_id, util.job_uniquifier[job_id].__class__, cls,
                    str(util.job_uniquifier[job_id]),
                    args, kwargs,x
                    ))
        if util.global_pipegraph is None:
            raise ValueError("You must first instanciate a pypipegraph before creating jobs""")
        return util.job_uniquifier[job_id]

    #def __getnewargs__(self):
        #"""Provides unpickeling support"""
        #return (self.job_id, )

    def __init__(self, job_id):
        #logger.info("init for %s" % job_id)
        if not hasattr(self, 'dependants'):  # test any of the following
            #else: this job was inited before, and __new__ returned an existing instance
            self.job_id = job_id
            self.cores_needed = 1
            self.memory_needed = -1
            self.dependants = set()
            self.prerequisites = set()
            self.failed = None
            self.error_reason = "no error"
            self.stdout = None
            self.stderr = None
            self.exception = None
            self.was_run = False
            self.was_done_on = set()  # on which slave(s) was this job run?
            self.was_loaded = False
            self.was_invalidated = False
            self.invalidation_count = 0  # used to save some time in graph.distribute_invariant_changes
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
        #logger.info("adding self %s to %s" % (job_id, id(util.global_pipegraph)))
        util.global_pipegraph.add_job(util.job_uniquifier[job_id])

    def depends_on(self, job_joblist_or_list_of_jobs):
        """Declare that this job depends on the ones passed in (which must be Jobs, JobLists or iterables of tsuch).
        This means that this job can only run, if all previous ones have been done sucessfully.
        """
        #if isinstance(job_joblist_or_list_of_jobs, Job):
            #job_joblist_or_list_of_jobs = [job_joblist_or_list_of_jobs]
        if job_joblist_or_list_of_jobs is self:
            raise ppg_exceptions.CycleError("job.depends_on(self) would create a cycle: %s" % (self.job_id))

        for job in job_joblist_or_list_of_jobs:
            if not isinstance(job, Job):
                if hasattr(job, '__iter__') and not isinstance(job, str):  # a nested list
                    self.depends_on(job)
                    pass
                else:
                    raise ValueError("Can only depend on Job objects, was: %s" % type(job))
            else:
                if self in job.prerequisites:
                    raise ppg_exceptions.CycleError("Cycle adding %s to %s" % (self.job_id, job.job_id))
                if isinstance(job, FinalJob):
                    raise ppg_exceptions.JobContractError("No jobs can depend on FinalJobs")
        for job in job_joblist_or_list_of_jobs:
            if isinstance(job, Job):  # skip the lists here, they will be delegated to further calls during the checking...
                self.prerequisites.add(job)
        return self

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
        raise ValueError("This job does not support ignore_code_changes")

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

    def is_done(self, depth = 0):
        if not self.do_cache or self._is_done is None:
            logger.info("recalc is_done %s" % self)
            self._is_done = self.calc_is_done(depth)
        logger.info("called %s.is_done - result %s" % (self, self._is_done))
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
        raise ValueError("Called load() on a j'ob that had is_loadable, but did not overwrite load() as it should")

    def runs_in_slave(self):
        """Is this a job that runs in our slave, ie. in a spawned job"""
        return True

    def modifies_jobgraph(self):
        """Is this a job that can modify the jobgraph at runtime?
        """
        return False

    def invalidated(self, reason=''):
        """This job was invalidated - throw away any existing output for recalculation"""
        logger.info("%s invalidated called, reason: %s" % (self, reason))
        self.was_invalidated = True
        self.distribute_invalidation()

    def distribute_invalidation(self):
        """Depth first descend to pass invalidated into all Jobs that dependend on this one"""
        for dep in self.dependants:
            if not dep.was_invalidated:
                dep.invalidated(reason='preq invalidated %s' % self)

    def can_run_now(self):
        """Can this job run right now?
        """
        #logger.info("can_run_now %s" % self)
        for preq in self.prerequisites:
            #logger.info("checking preq %s" % preq)
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable():
                     # was_run is necessary, a filegen job might have already created the file (and written a bit to it), but that does not mean that it's done enough to start the next one. Was_run means it has returned.
                     # On the other hand, it might have been a job that didn't need to run, then was_invalidated should be false.
                     # or it was a loadable job anyhow, then it doesn't matter.
                    #logger.info("case 1 - false %s" % preq)
                    return False  # false means no way
                else:  # pragma: no cover
                    #logger.info("case 2 - delay") #but we still need to try the other preqs if it was ok
                    pass
            else:
                #logger.info("case 3 - not done")
                return False
        #logger.info("case 4 - true")
        return True

    def list_blocks(self):  # pragma: no cover
        """A helper to list what blocked this job from running - debug function"""
        res = []
        for preq in self.prerequisites:
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable():   # see can_run_now for why
                    res.append((str(preq), 'not run'))
                else:
                    #logger.info("case 2 - delay") #but we still need to try the other preqs if it was ok
                    pass
            else:
                #logger.info("case 3 - not done")
                if preq.was_run:
                    if preq.was_cleaned_up:
                        res.append((str(preq), 'not done - but was run! - after cleanup'))
                    else:
                        res.append((str(preq), 'not done - but was run! - no cleanup'))
                else:
                    res.append((str(preq), 'not done'))
                break
                #return False
        return res

    def run(self):  #pragma: no cover
        """Do the actual work"""
        pass

    def check_prerequisites_for_cleanup(self):
        """If for one of our prerequisites, all dependands have run, we can
        call it's cleanup function (unload data, remove tempfile...)
        """
        for preq in self.prerequisites:
            logger.info("check_prerequisites_for_cleanup %s" % preq)
            all_done = True
            for dep in preq.dependants:
                logger.info('checking %s, failed %s, was_run: %s' % (dep, dep.failed, dep.was_run))
                if dep.failed or (not dep.was_run) or not preq.is_done():
                    all_done = False
                    break
            if all_done:
                logger.info("Calling %s cleanup" % preq)
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
        if hasattr(self, 'callback'):
            return "%s (job_id=%s,id=%s\n Callback: %s:%s)" % (self.__class__.__name__, self.job_id, id(self), self.callback.__code__.co_filename, self.callback.__code__.co_firstlineno)
        else:
            return "%s (job_id=%s,id=%s)" % (self.__class__.__name__, self.job_id, id(self))

    def __str__(self):
        return '%s("%s")' % (self.__class__.__name__, self.job_id)


class _InvariantJob(Job):
    """common code for all invariant jobs"""

    def depends_on(self, job_joblist_or_list_of_jobs):
        raise ppg_exceptions.JobContractError("Invariants can't have dependencies")

    def runs_in_slave(self):
        return False


def get_cython_filename_and_line_no(cython_func):
    first_doc_line = cython_func.__doc__.split("\n")[0]
    if not first_doc_line.startswith('File:'):
        raise ValueError("No file/line information in doc string. Make sure your cython is compiled with -p (or #embed_pos_in_docstring=True atop your pyx")
    line_no = int(first_doc_line[first_doc_line.find('starting at line ') + len('starting at line '):first_doc_line.find(')')])

    #find the right module
    module_name = cython_func.im_class.__module__
    found = False
    for name in sorted(sys.modules):
        if name == module_name or name.endswith("." + module_name):
            try:
                if getattr(sys.modules[name], cython_func.im_class.__name__) == cython_func.im_class:
                    found = sys.modules[name]
                    break
            except AttributeError:
                continue
        elif hasattr(sys.modules[name], module_name):
            sub_module = getattr(sys.modules[name], module_name)
            try:
                if getattr(sub_module, cython_func.im_class.__name__) == cython_func.im_class:
                    found = sys.moduls[name].sub_module
                    break
            except AttributeError:
                continue
    if not found:
        raise ValueError("Could not find module for %s" % cython_func)
    filename = found.__file__.replace('.so', '.pyx').replace('.pyc','.py') #  pyc replacement is for mock testing
    return filename, line_no


def function_to_str(func):
    if str(func).startswith('<built-in function'):
        return "%s" % func
    elif hasattr(func, 'im_func') and ('cyfunction' in repr(func.im_func) or ('<built-in function' in repr(func.im_func))):
        return "%s %i" % get_cython_filename_and_line_no(func)
    else:
        return "%s %i" % (
            func.__code__.co_filename if func else 'None', func.__code__.co_firstlineno if func else 0,
        )


class FunctionInvariant(_InvariantJob):
    """FunctionInvariant detects (bytecode) changes in a python function,
    currently via disassembly"""
    def __init__(self, job_id, function):
        if not hasattr(function, '__call__') and function is not None:
            raise ValueError("%s function was not a callable (or None)" % job_id)
        Job.__init__(self, job_id)
        if hasattr(self, 'function') and function != self.function:
            raise ppg_exceptions.JobContractError("FunctionInvariant %s created twice with different functions: \n%s\n%s" % (
                job_id,
                function_to_str(function),
                function_to_str(self.function)
            ))
        self.function = function

    def __str__(self):
        if hasattr(self, 'function') and self.function and hasattr(self.function, '__code__'): # during creating, __str__ migth be called by a debug function before function is set...
            return "%s (job_id=%s,id=%s\n Function: %s:%s)" % (self.__class__.__name__, self.job_id, id(self), self.function.__code__.co_filename, self.function.__code__.co_firstlineno)
        elif hasattr(self, 'function') and str(self.function).startswith('<built-in function'):
            return "%s (job_id=%s,id=%s, Function: %s)" % (self.__class__.__name__, self.job_id, id(self), self.function)
        else:
            return "%s (job_id=%s,id=%s, Function: None)" % (self.__class__.__name__, self.job_id, id(self))

    def _get_invariant(self, old, all_invariant_stati):
        if self.function is None:
            return None  # since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
        if not hasattr(self.function, '__code__'):
            if str(self.function).startswith('<built-in function'):
                return str(self.function)
            elif hasattr(self.function, 'im_func') and ('cyfunction' in repr(self.function.im_func) or repr(self.function.im_func).startswith('<built-in function')):
                return self.get_cython_source(self.function)
            else:
                print (repr(self.function))
                print (repr(self.function.im_func))
                raise ValueError("Can't handle this object %s" % self.function)
        try:
            closure = self.function.func_closure
        except AttributeError:
            closure = self.function.__closure__
        key = (id(self.function.__code__), id(closure))
        if not key in util.func_hashes:
            if hasattr(self.function, 'im_func') and 'cyfunction' in repr(self.function.im_func):
                invariant = self.get_cython_source(self.function)
            else:
                invariant = self.dis_code(self.function.__code__)
                if closure:
                    for name, cell in zip(self.function.__code__.co_freevars, closure):
                        # we ignore references to self - in that use case you're expected to make your own ParameterInvariants, and we could not detect self.parameter anyhow (only self would be bound)
                        # we also ignore bound functions - their address changes all the time. IDEA: Make this recursive (might get to be too expensive)
                        try:
                            if (
                                name != 'self' and
                                not hasattr(cell.cell_contents, '__code__') and
                                not isinstance(cell.cell_contents, module_type)
                            ):
                                if isinstance(cell.cell_contents, dict):
                                    x = str(sorted(list(cell.cell_contents.items())))
                                else:
                                    x = str(cell.cell_contents)
                                if 'at 0x' in x:  # if you don't have a sensible str(), we'll default to the class path. This takes things like <chipseq.quality_control.AlignedLaneQualityControl at 0x73246234>.
                                    x = x[:x.find('at 0x')]
                                if 'id=' in x:
                                    print(x)
                                    raise ValueError("Still an issue")
                                invariant += "\n" + x
                        except ValueError as e:
                            if str(e) == 'Cell is empty':
                                pass
                            else:
                                raise

            util.func_hashes[id(self.function.__code__)] = invariant
        return util.func_hashes[id(self.function.__code__)]

    inner_code_object_re = re.compile(
                            r'(<code\sobject\s<?[^>]+>?\sat\s0x[a-f0-9]+[^>]+)' + '|' +  #that's the cpython way
                            '(<code\tobject\t<[^>]+>,\tfile\t\'[^\']+\',\tline\t[0-9]+)' #that's how they look like in pypy. More sensibly, actually
            )

    def dis_code(self, code):
        """'dissassemble' python code.
        Strips lambdas (they change address every execution otherwise)"""
        # TODO: replace with bytecode based smarter variant
        out = io.StringIO()
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
        res = self.inner_code_object_re.sub('lambda', res)
        for ii, constant in enumerate(code.co_consts):
            if hasattr(constant, 'co_code'):
                res += 'inner no %i' % ii
                res += self.dis_code(constant)
        return res

    def get_cython_source(self, cython_func):
        """Attemp to get the cython source for a function.
        Requires cython code to be compiled with -p or #embed_pos_in_docstring=True in the source file

        Unfortunatly, finding the right module (to get an absolute file path) is not straight forward,
        we inspect all modules in sys.module, and their children, but we might be missing sub-sublevel modules,
        in which case we'll need to increase search depth
        """

        #check there's actually the file and line no documentation
        filename, line_no = get_cython_filename_and_line_no(cython_func)

        #load the source code
        op = open(filename, 'rb')
        d = op.read().split("\n")
        op.close()

        #extract the function at hand, minus doc string
        remaining_lines = d[line_no:]
        first_line = remaining_lines[0]
        first_line_indent = len(first_line) - len(first_line.lstrip())
        starts_with_double_quote = first_line.strip().startswith('"""')
        starts_with_single_quote = first_line.strip().startswith("'''")
        if starts_with_single_quote or starts_with_double_quote:  # there is a docstring
            text = "\n".join(remaining_lines).strip()
            text = text[3:]  # cut of initial ###
            if starts_with_single_quote:
                text = text[text.find("'''") + 3:]
            else:
                text = text[text.find('"""') + 3:]
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
        job_id = 'PI' + job_id
        return Job.__new__(cls, job_id)

    def __init__(self, job_id, parameters, accept_as_unchanged_func = None):
        job_id = 'PI' + job_id
        self.parameters = parameters
        self.accept_as_unchanged_func = accept_as_unchanged_func
        Job.__init__(self, job_id)

    def _get_invariant(self, old, all_invariant_stati):
        if self.accept_as_unchanged_func is not None:
            if self.accept_as_unchanged_func(old):
                logger.info("Nothing Changed for %s" % self)
                raise util.NothingChanged(self.parameters)
        return self.parameters


class FileChecksumInvariant(_InvariantJob):
    """Invalidates when the (md5) checksum of a file changed.
    Checksum only get's recalculated if the file modification time changed.
    """

    def __init__(self, filename):
        Job.__init__(self, filename)
        self.input_file = filename

    def _get_invariant(self, old, all_invariant_stati):
        st = util.stat(self.input_file)
        filetime = st[stat.ST_MTIME]
        filesize = st[stat.ST_SIZE]
        try:
            if not old or old == filetime or old[1] != filesize or old[0] != filetime:
                #print 'triggered checksum', self.input_file
                #print 'old', old
                #print 'new', filetime, filesize
                chksum = self.checksum()
                if old == filetime:  # we converted from a filetimeinvariant
                    #print ('nothingchanged', self.job_id)
                    raise util.NothingChanged((filetime, filesize, chksum))
                elif old and old[2] == chksum:
                    raise util.NothingChanged((filetime, filesize, chksum))
                else:
                    #print ('returning new', self.job_id)
                    return filetime, filesize, chksum
            else:
                return old
        except TypeError:  # could not parse old tuple... possibly was an FileTimeInvariant before...
            chksum = self.checksum()
            #print ('type error', self.job_id)
            return filetime, filesize, chksum

    def checksum(self):
        md5_file = self.input_file + '.md5sum'
        if os.path.exists(md5_file):
            st = util.stat(self.input_file)
            st_md5 = util.stat(md5_file)
            if st[stat.ST_MTIME] == st_md5[stat.ST_MTIME]:
                with open(md5_file, 'rb') as op:
                    return op.read()
            else:
                checksum = self._calc_checksum()
                with open(md5_file, 'wb') as op:
                    op.write(checksum)
                os.utime(md5_file, (st[stat.ST_MTIME], st[stat.ST_MTIME]))
                return checksum
        else:
            return self._calc_checksum()
        
    def _calc_checksum(self):
        file_size = os.stat(self.job_id)[stat.ST_SIZE]
        if file_size > 200 * 1024 * 1024:
            print ('Taking md5 of large file', self.job_id)
        with open(self.job_id, 'rb') as op:
            block_size = 1024**2 * 10
            block = op.read(block_size)
            _hash = hashlib.md5()
            while block:
                _hash.update(block)
                block = op.read(block_size)
            res = _hash.hexdigest()
        return res

FileTimeInvariant = FileChecksumInvariant


class RobustFileChecksumInvariant(FileChecksumInvariant):
    """A file checksum invariant that is robust against file moves (but not against renames!"""

    def _get_invariant(self, old, all_invariant_stati):
        if old: # if we have something stored, this acts like a normal FileChecksumInvariant
            return FileChecksumInvariant._get_invariant(self, old, all_invariant_stati)
        else:
            basename = os.path.basename(self.input_file)
            st = util.stat(self.input_file)
            filetime = st[stat.ST_MTIME]
            filesize = st[stat.ST_SIZE]
            checksum = self.checksum()
            for job_id in all_invariant_stati:
                if os.path.basename(job_id) == basename: # could be a moved file...
                    old = all_invariant_stati[job_id]
                    if isinstance(old, tuple):
                        if len(old) == 2:
                            old_filesize, old_chksum = old
                        else:
                            dummy_old_filetime, old_filesize, old_chksum = old
                        if old_filesize == filesize:
                            if old_chksum == checksum:  # don't check filetime, if the file has moved it will have changed
                                #print("checksum hit %s" % self.input_file)
                                raise util.NothingChanged((filetime, filesize, checksum))
            # no suitable old job found.
            return (filetime, filesize, checksum)


class FileGeneratingJob(Job):
    """Create a single output file of more than 0 bytes."""

    def __init__(self, output_filename, function, rename_broken=False, empty_file_allowed = False):
        """If @rename_broken is set, any eventual outputfile that exists
        when the job crashes will be renamed to output_filename + '.broken'
        (overwriting whatever was there before)
        """
        if not hasattr(function, '__call__'):
            raise ValueError("function was not a callable")
        if output_filename in util.filename_collider_check and util.filename_collider_check[output_filename] is not self:
            raise ValueError("Two jobs generating the same file: %s %s%" % (self, util.filename_collider_check[output_filename]))
        else:
            util.filename_collider_check[output_filename] = self
        self.empty_file_allowed = empty_file_allowed
        Job.__init__(self, output_filename)
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False
        self._is_done_cache = None
        self._was_run = None

    #the motivation for this chaching is that we do a lot of stat calls. Tens of thousands - and the answer can basically only change
    #when you either run or invalidate the job. This apperantly cuts down about 9/10 of all stat calls
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
            #logger.info("Injecting outa invariants %s" % self)
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))
        else:
            pass
            #logger.info("not Injecting outa invariants %s" % self)

    def calc_is_done(self, depth=0):
        if self._is_done_cache is None:
            if self.empty_file_allowed:
                self._is_done_cache = util.file_exists(self.job_id)
            else:
                self._is_done_cache = util.output_file_exists(self.job_id)
        return self._is_done_cache

    def invalidated(self, reason=''):
        try:
            logger.info("unlinking %s" % self.job_id)
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
                if (
                'takes exactly 1 argument (0 given)' in str(e) or # python2
                ' missing 1 required positional argument:' in str(e) # python3
                ):
                    self.callback(self.job_id)
                else:
                    raise
        except Exception as e:
            exc_info = sys.exc_info()
            tb = traceback.format_exc()
            sys.stderr.write(tb)
            try:
                if self.rename_broken:
                    shutil.move(self.job_id, self.job_id + '.broken')
                else:
                    logger.info("unlinking %s" % self.job_id)
                    os.unlink(self.job_id)
            except (OSError, IOError):
                pass
            util.reraise(exc_info[1], None, exc_info[2])
        if self.empty_file_allowed:
            filecheck = util.file_exists
        else:
            filecheck = util.output_file_exists
        if not filecheck(self.job_id):
            raise ppg_exceptions.JobContractError("%s did not create its file %s %s" % (self,self.callback.__code__.co_filename, self.callback.__code__.co_firstlineno) )


class MultiFileGeneratingJob(FileGeneratingJob):
    """Create multiple files - recreate all of them if at least one is missing.
    """

    def __new__(cls, filenames, *args, **kwargs):
        if isinstance(filenames, str):
            raise ValueError("Filenames must be a list (or at least an iterable), not a single string")
        if not hasattr(filenames, '__iter__'):
            raise TypeError("filenames was not iterable")

        job_id = ":".join(sorted(str(x) for x in filenames))
        return Job.__new__(cls, job_id)

    def __getnewargs__(self):   # so that unpickling works
        return (self.filenames, )

    def __init__(self, filenames, function, rename_broken=False, empty_files_ok = False):
        """If @rename_broken is set, any eventual outputfile that exists
        when the job crashes will be renamed to output_filename + '.broken'
        (overwriting whatever was there before)
        """
        if not hasattr(function, '__call__'):
            raise ValueError("function was not a callable")
        sorted_filenames = list(sorted(x for x in filenames))
        for x in sorted_filenames:
            if not isinstance(x, six.string_types):
                raise ValueError("Not all filenames passed to MultiFileGeneratingJob were string objects")
            if x in util.filename_collider_check and util.filename_collider_check[x] is not self:
                raise ValueError("Two jobs generating the same file: %s %s - %s" % (self, util.filename_collider_check[x], x))
            else:
                util.filename_collider_check[x] = self

        job_id = ":".join(sorted_filenames)
        Job.__init__(self, job_id)
        self.filenames = filenames
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False
        self.empty_files_ok = empty_files_ok

    def calc_is_done(self, depth=0):
        for fn in self.filenames:
            if self.empty_files_ok:
                if not util.file_exists(fn):
                    return False
            else:
                if not util.output_file_exists(fn):
                    return False
        return True

    def invalidated(self, reason=''):
        for fn in self.filenames:
            try:
                logger.info("unlinking %s" % self.job_id)
                os.unlink(fn)
            except OSError:
                pass
        Job.invalidated(self, reason)

    def run(self):
        try:
            self.callback()
        except Exception as e:
            exc_info = sys.exc_info()
            if self.rename_broken:
                for fn in self.filenames:
                    try:
                        shutil.move(fn, fn + '.broken')
                    except IOError:
                        pass
            else:
                for fn in self.filenames:
                    try:
                        logger.info("unlinking %s" % fn)
                        os.unlink(fn)
                    except OSError:
                        pass
            util.reraise(exc_info[1], None, exc_info[2])
        self._is_done = None
        missing_files = []
        if self.empty_files_ok:
            filecheck = util.file_exists
        else:
            filecheck = util.output_file_exists
        for f in self.filenames:
            if not filecheck(f):
                missing_files.append(f)
        if missing_files:
            raise ppg_exceptions.JobContractError("%s did not create all of its files.\nMissing were:\n %s" % (self.job_id, "\n".join(missing_files)))

    def runs_in_slave(self):
        return True


class TempFileGeneratingJob(FileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully"""

    def __init__(self, output_filename, function, rename_broken=False):
        FileGeneratingJob.__init__(self, output_filename, function, rename_broken)
        self.is_temp_job = True

    def cleanup(self):
        logger.info("%s cleanup" % self)
        try:
             # the renaming will already have been done when FileGeneratingJob.run(self) was called...
            #if self.rename_broken:
                #shutil.move(self.job_id, self.job_id + '.broken')
            #else:
            logger.info("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except (OSError, IOError):
            pass

    def runs_in_slave(self):
        return True

    def calc_is_done(self, depth=0):
        logger.info("calc is done %s" % self)
        if os.path.exists(self.job_id):  # explicitly not using util.output_file_exists, since there the stat has a race condition - reports 0 on recently closed files
            logger.info("calc is done %s - file existed" % self)
            return True
        else:
            for dep in self.dependants:
                if (not dep.is_done()) and (not dep.is_loadable()):
                    return False
            return True

class MultiTempFileGeneratingJob(FileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully"""

    def __new__(cls, filenames, *args, **kwargs):
        if isinstance(filenames, str):
            raise ValueError("Filenames must be a list (or at least an iterable), not a single string")
        if not hasattr(filenames, '__iter__'):
            raise TypeError("filenames was not iterable")

        job_id = ":".join(sorted(str(x) for x in filenames))
        return Job.__new__(cls, job_id)

    def __getnewargs__(self):   # so that unpickling works
        return (self.filenames, )

    def __init__(self, filenames, function, rename_broken=False):
        """If @rename_broken is set, any eventual outputfile that exists
        when the job crashes will be renamed to output_filename + '.broken'
        (overwriting whatever was there before)
        """
        self.is_temp_job = True
        if not hasattr(function, '__call__'):
            raise ValueError("function was not a callable")
        sorted_filenames = list(sorted(x for x in filenames))
        for x in sorted_filenames:
            if not isinstance(x, six.string_types):
                raise ValueError("Not all filenames passed to MultiTempFileGeneratingJob were string objects")
            if x in util.filename_collider_check and util.filename_collider_check[x] is not self:
                raise ValueError("Two jobs generating the same file: %s %s - %s" % (self, util.filename_collider_check[x], x))
            else:
                util.filename_collider_check[x] = self

        job_id = ":".join(sorted_filenames)
        Job.__init__(self, job_id)
        self.filenames = filenames
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False


    def cleanup(self):
        logger.info("%s cleanup" % self)
        try:
             # the renaming will already have been done when FileGeneratingJob.run(self) was called...
            #if self.rename_broken:
                #shutil.move(self.job_id, self.job_id + '.broken')
            #else:
            for fn in self.filenames:
                logger.info("unlinking (cleanup) %s" % fn)
                os.unlink(fn)
        except (OSError, IOError):
            pass

    def invalidated(self, reason=''):
        for fn in self.filenames:
            try:
                logger.info("unlinking (invalidated) %s" % self.job_id)
                os.unlink(fn)
            except OSError:
                pass
        Job.invalidated(self, reason)

    def run(self):
        try:
            self.callback()
        except Exception as e:
            exc_info = sys.exc_info()
            if self.rename_broken:
                for fn in self.filenames:
                    try:
                        shutil.move(fn, fn + '.broken')
                    except IOError:
                        pass
            else:
                for fn in self.filenames:
                    try:
                        logger.info("unlinking %s" % fn)
                        os.unlink(fn)
                    except OSError:
                        pass
            util.reraise(exc_info[1], None, exc_info[2])
        self._is_done = None
        missing_files = []
        for f in self.filenames:
            if not util.output_file_exists(f):
                missing_files.append(f)
        if missing_files:
            raise ppg_exceptions.JobContractError("%s did not create all of its files.\nMissing were:\n %s" % (self.job_id, "\n".join(missing_files)))

    def runs_in_slave(self):
        return True
    def calc_is_done(self, depth=0):
        logger.info("calc is done %s" % self)
        all_files_exist = True
        for fn in self.filenames:
            all_files_exist = all_files_exist and os.path.exists(fn)
        if all_files_exist:  # explicitly not using util.output_file_exists, since there the stat has a race condition - reports 0 on recently closed files
            logger.info("calc is done %s - file existed" % self)
            return True
        else:
            for dep in self.dependants:
                if (not dep.is_done()) and (not dep.is_loadable()):
                    return False
            return True


class TempFilePlusGeneratingJob(FileGeneratingJob):
    """Create a temporary file that is removed once all direct dependands have
    been executed sucessfully,
    but keep a log file (and rerun if the log file is not there)
    """

    def __init__(self, output_filename, log_filename, function, rename_broken=False):
        if output_filename == log_filename:
            raise ValueError("output_filename and log_filename must be different")
        FileGeneratingJob.__init__(self, output_filename, function)
        self.output_filename = output_filename
        self.log_file = log_filename
        self.is_temp_job = True

    def cleanup(self):
        logger.info("%s cleanup" % self)
        try:
             # the renaming will already have been done when FileGeneratingJob.run(self) was called...
            #if self.rename_broken:
                #shutil.move(self.job_id, self.job_id + '.broken')
            #else:
            logger.info("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except (OSError, IOError):
            pass

    def runs_in_slave(self):
        return True

    def calc_is_done(self, depth=0):
        if not os.path.exists(self.log_file):
            return None
        if os.path.exists(self.job_id):  # explicitly not using util.output_file_exists, since there the stat has a race condition - reports 0 on recently closed files
            return True
        else:
            for dep in self.dependants:
                if (not dep.is_done()) and (not dep.is_loadable()):
                    return False
            return True

    def run(self):
        try:
            self.callback()
        except Exception as e:
            exc_info = sys.exc_info()
            try:
                logger.info("unlinking %s" % self.output_filename)
                os.unlink(self.output_filename)
            except OSError:
                pass
            util.reraise(exc_info[1], None, exc_info[2])
        self._is_done = None
        if not os.path.exists(self.output_filename):
            raise ppg_exceptions.JobContractError("%s did not create it's output file")
        if not os.path.exists(self.log_file):
            raise ppg_exceptions.JobContractError("%s did not create it's log file")

class DataLoadingJob(Job):
    """Modify the current (system local) master process with a callback function.
    No cleanup is performed - use AttributeLoadingJob if you want your data to be unloaded"""
    def __init__(self, job_id, callback):
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")

        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def is_loadable(self):
        return True

    def load(self):
        if self.was_loaded:
            logger.info("%s.load (repeat)" % self)
            return
        logger.info("%s.load" % self)
        for preq in self.prerequisites:  # load whatever is necessary...
            if preq.is_loadable():
                preq.load()

        start = time.time()
        if hasattr(self, 'profile'):
            import cProfile
            cProfile.runctx('self.callback()', globals(), locals(), "%s.prof" % id(self))
        else:
            self.callback()
        end = time.time()
        logger.info("Loading time for %s - %.3f" % (self.job_id, end - start))
        self.was_loaded = True

    def calc_is_done(self, depth=0):  # delegate to preqs... passthrough of 'not yet done'
        #logger.info("\t" * depth + "Checking is done on %s" % self)
        for preq in self.prerequisites:
            if not preq.is_done(depth=depth + 1):
                #logger.info("\t" * depth + "failed on %s" % preq)
                return False
        #logger.info("\t" * depth + "Passed")
        return True


class AttributeLoadingJob(DataLoadingJob):
    """Modify the current master process by loading the return value of a callback
    into an object attribute.

    On cleanup, the attribute is deleted via del
    """

    def __init__(self, job_id, object, attribute_name, callback):
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")
        if not isinstance(attribute_name, str):
            raise ValueError("attribute_name was not a string")
        if not hasattr(self, 'object'):
            self.object = object
            self.attribute_name = attribute_name
        else:
            if not self.object is object:
                raise ppg_exceptions.JobContractError("Creating AttributeLoadingJob twice with different target objects")
            if not self.attribute_name == attribute_name:
                raise ppg_exceptions.JobContractError("Creating AttributeLoadingJob twice with different target attributes")
        if not hasattr(callback, '__call__'):
            raise ValueError("Callback for %s was not callable (missed __call__ attribute)" % job_id)
        DataLoadingJob.__init__(self, job_id, callback)

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def load(self):
        #logger.info("%s load" % self)
        if self.was_loaded:
        #    logger.info('Was loaded')
            return
        for preq in self.prerequisites:  # load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        #logger.info("setting %s on id %i in pid %i" % (self.attribute_name, id(self.object), os.getpid()))
        setattr(self.object, self.attribute_name, self.callback())
        self.was_loaded = True

    def is_loadable(self):
        return True

    def calc_is_done(self, depth=0):  # delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

    def cleanup(self):
        logger.info("Cleanup on %s" % self.attribute_name)
        try:
            delattr(self.object, self.attribute_name)
        except AttributeError:  # this can happen if you have a messed up DependencyInjectionJob, but it would block the messed up reporting...
            pass

    def __str__(self):
        return "AttributeLoadingJob (job_id=%s,id=%i,target=%i)" % (self.job_id, id(self), id(self.object))


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
        """@check_for_dependency_injections - by default, we check whether you injected correctly,
        but some of these checks are costly so you might wish to optimize by setting check_for_dependency_injections=False, but injecting into already run jobs and so on might create invisible (non exception raising) bugs.
        """
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True
        self.check_for_dependency_injections = check_for_dependency_injections

    def ignore_code_changes(self):
        pass

    def inject_auto_invariants(self):
         # if not self.do_ignore_code_changes:
            #self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))
        pass

    def run(self):
         # this is different form JobGeneratingJob.run in it's checking of the contract
        util.global_pipegraph.new_jobs = {}
        logger.info("DependencyInjectionJob.dependants = %s %s" % (", ".join(str(x) for x in self.dependants), id(self.dependants)))
        reported_jobs = self.callback()
        logger.info("DependencyInjectionJob.dependants after callback = %s %s" % (", ".join(str(x) for x in self.dependants), id(self.dependants)))
        logger.info("new_jobs count: %i, id %s" % (len(util.global_pipegraph.new_jobs), id(util.global_pipegraph.new_jobs)))
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
        logger.info("Now checking first step for dependency injection violations")
        new_job_set = set(util.global_pipegraph.new_jobs.keys())
        if True:
            for job in util.global_pipegraph.jobs.values():
                for nw_jobid in new_job_set.intersection([x.job_id for x in job.prerequisites]):
                    #logger.info("Checking %s against %s - %s" % (nw, job, job in self.dependants))
                    nw = util.global_pipegraph.new_jobs[nw_jobid]
                    if not job in self.dependants:
                        raise ppg_exceptions.JobContractError("DependencyInjectionJob %s tried to inject %s into %s, but %s was not dependand on the DependencyInjectionJob. It was dependand on %s though" % (self, nw, job, job, nw.prerequisites))
                    nw.dependants.add(job)
         # I need to check: All new jobs are now prereqs of my dependands

         # I also need to check that none of the jobs that ain't dependand on me have been injected
        if not self.check_for_dependency_injections:
            logger.info("Skipping check for dependency injection violations")
        else:
            logger.info("Checking for dependency injection violations")
            for job in util.global_pipegraph.jobs.values():
                if job in self.dependants:
                    for new_job in util.global_pipegraph.new_jobs.values():
                        if not job.is_in_dependency_chain(new_job, 5):  # 1 for the job, 2 for auto dependencies, 3 for load jobs, 4 for the dependencies of load jobs... 5 seems to work in pratice.
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was not added to the prerequisites of %s" % (self.job_id, new_job.job_id, job.job_id))
                else:
                    preq_intersection = set(job.prerequisites).intersection(new_job_set)
                    if preq_intersection:
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the prerequisites of %s, but was not dependant on the DependencyInjectionJob" % (self.job_id, preq_intersection, job.job_id))
                    dep_intersection = set(job.prerequisites).intersection(new_job_set)
                    if dep_intersection:
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the dependants of %s, but was not dependant on the DependencyInjectionJob" % (self.job_id, dep_intersection, job.job_id))

        res = util.global_pipegraph.new_jobs
        logger.info('returning %i new jobs' % len(res))
        logger.info('%s' % ",".join(res.keys()))
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        return res


class JobGeneratingJob(_GraphModifyingJob):
    """A Job generating new jobs. The new jobs must be leaves in the sense that no job that existed
    before may depend on them. If that's what you want, see L{DependencyInjectionJob}.
    """
    def __init__(self, job_id, callback):
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True

    def ignore_code_changes(self):
        pass

    def inject_auto_invariants(self):
        pass

    def run(self):
        logger.info("Storing new jobs in %s" % id(util.global_pipegraph))
        util.global_pipegraph.new_jobs = {}
        self.callback()
        for new_job in list(util.global_pipegraph.new_jobs.values()):
            new_job.inject_auto_invariants()
         # I need to check: All new jobs are now prereqs of my dependands
         # I also need to check that none of the jobs that ain't dependand on me have been injected
        new_job_set = set(util.global_pipegraph.new_jobs.values())
        for job in util.global_pipegraph.jobs.values():
            if new_job_set.intersection(job.prerequisites):
                    raise ppg_exceptions.JobContractError("JobGeneratingJob %s created a job that was added to the prerequisites of %s, which is invalid. Use a DependencyInjectionJob instead, this one might only create 'leave' nodes" % (self.job_id, job.job_id))
        res = util.global_pipegraph.new_jobs
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        logger.info("Returning from %s" % self)
        return res


class FinalJob(Job):
    """A final job runs after all other (non final) jobs have run.
    Use these sparringly - they really only make sense for things where you really want to hook
    'after the pipeline has run', everything else realy is better of if you depend on the appropriate job

    FinalJobs are also run on each run - but only if no other job died.
    """

    def __init__(self, jobid, callback):
        Job.__init__(self, jobid)
        self.callback = callback
        self.is_final_job = True
        self.do_ignore_code_changes = False
        self.always_runs = True

    def calc_is_done(self, depth=0):
        return self.was_run

    def depends_on(self, *args):
        raise ppg_exceptions.JobContractError("Final jobs can not have explicit dependencies - they run in random order after all other jobs")

    def ignore_code_changes(self):
        pass

    def inject_auto_invariants(self):
        pass

    def run(self):
        self.callback()


class PlotJob(FileGeneratingJob):
    """Calculate some data for plotting, cache it in cache/output_filename, and plot from there.
    creates two jobs, a plot_job (this one) and a cache_job (FileGeneratingJob, in self.cache_job),

    To use these jobs, you need to have pyggplot available.
    """
    def __init__(self, output_filename, calc_function, plot_function, render_args=None, skip_table=False, skip_caching=False):
        if not isinstance(output_filename, six.string_types):
            raise ValueError("output_filename was not a string type")
        if not (output_filename.endswith('.png') or output_filename.endswith('.pdf') or output_filename.endswith('.svg')):
            raise ValueError("Don't know how to create this file %s, must end on .png or .pdf or .svg" % output_filename)

        self.output_filename = output_filename
        self.table_filename = self.output_filename + '.tsv'
        self.calc_function = calc_function
        self.plot_function = plot_function
        self.skip_caching = skip_caching
        if render_args is None:
            render_args = {}
        self.render_args = render_args
        self._fiddle = None

        import pandas as pd
        import pyggplot
        if not self.skip_caching:
            self.cache_filename = os.path.join('cache', output_filename)

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
                        raise ppg_exceptions.JobContractError("%s.calc_function did not return a DataFrame (or dict of such), was %s " % (output_filename, str(df.__class__)))
                try:
                    os.makedirs(os.path.dirname(self.cache_filename))
                except OSError:
                    pass
                of = open(self.cache_filename, 'wb')
                pickle.dump(df, of, pickle.HIGHEST_PROTOCOL)
                of.close()

        def run_plot():
            df = self.get_data()
            plot = plot_function(df)
            if not isinstance(plot, pyggplot.Plot):
                raise ppg_exceptions.JobContractError("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
            if not 'width' in render_args and hasattr(plot, 'width'):
                render_args['width'] = plot.width
            if not 'height' in render_args and hasattr(plot, 'height'):
                render_args['height'] = plot.height
            if self._fiddle:
                self._fiddle(plot)
            plot.render(output_filename, **render_args)

        FileGeneratingJob.__init__(self, output_filename, run_plot)
        Job.depends_on(self, ParameterInvariant(self.output_filename + '_params', render_args))

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
                    from pandas import ExcelWriter
                    writer = pd.ExcelWriter(self.table_filename)
                    for key, dframe in df.items():
                        dframe.to_excel(writer, key)
                    writer = pd.ExcelWriter(self.table_filename)
                    for key in df:
                        df[key].to_excel(writer, key)
                    writer.save()
            table_gen_job = FileGeneratingJob(self.table_filename, dump_table)
            if not self.skip_caching:
                table_gen_job.depends_on(cache_job)
            self.table_job = table_gen_job
        else:
            self.table_job = None

    def add_another_plot(self, output_filename, plot_function, render_args=None):
        """Add another plot job that runs on the same data as the original one (calc only done once)"""
        import pyggplot
        if render_args is None:
            render_args = {}
        def run_plot():
            df = self.get_data()
            plot = plot_function(df)
            if not isinstance(plot, pyggplot.Plot):
                raise ppg_exceptions.JobContractError("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
            if not 'width' in render_args and hasattr(plot, 'width'):
                render_args['width'] = plot.width
            if not 'height' in render_args and hasattr(plot, 'height'):
                render_args['height'] = plot.height
            plot.render(output_filename, **render_args)
        job = FileGeneratingJob(output_filename, run_plot)
        job.depends_on(ParameterInvariant(self.output_filename + '_params', render_args))
        job.depends_on(FunctionInvariant(self.output_filename + '_func', plot_function))
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
        Job.depends_on(self, FunctionInvariant(self.output_filename + '_fiddle', fiddle_function))

    def depends_on(self, other_job):
        #FileGeneratingJob.depends_on(self, other_job)  # just like the cached jobs, the plotting does not depend on the loading of prerequisites
        if self.skip_caching:
            Job.depends_on(self, other_job)
            if self.table_job:
                self.table_job.depends_on(other_job)
        elif hasattr(self, 'cache_job') and not other_job is self.cache_job:  # activate this after we have added the invariants...
            self.cache_job.depends_on(other_job)
        return self

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            if not self.skip_caching:
                self.cache_job.depends_on(FunctionInvariant(self.job_id + '.calcfunc', self.calc_function))
            else:
                FileGeneratingJob.depends_on(self, FunctionInvariant(self.job_id + '.calcfunc', self.calc_function))
            FileGeneratingJob.depends_on(self, FunctionInvariant(self.job_id + '.plotfunc', self.plot_function))
            FileGeneratingJob.depends_on(self, FunctionInvariant(self.job_id + '.run_plot', self.callback)) # depend on the run_plot func

    def get_data(self):
        if self.skip_caching:
            return self.calc_function()
        else:
            try:
                of = open(self.cache_filename, 'rb')
                df = pickle.load(of)
                of.close()
            except:
                print ('could not load', self.cache_filename)
                raise
            return df

    def __str__(self):
        return "%s (job_id=%s,id=%s\n Calc function: %s:%s\nPlot function: %s:%s)" % (self.__class__.__name__, self.job_id, id(self), self.calc_function.__code__.co_filename, self.calc_function.__code__.co_firstlineno,
                self.plot_function.__code__.co_filename,self.plot_function.__code__.co_firstlineno)


def CombinedPlotJob(output_filename, plot_jobs, facet_arguments, render_args=None, fiddle = None):
    """Combine multiple PlotJobs into a common (faceted) output plot.
    An empty list means 'no facetting'

    To use these jobs, you need to have pyggplot available.
    """
    if not isinstance(output_filename, six.string_types):
        raise ValueError("output_filename was not a string type")
    if not (output_filename.endswith('.png') or output_filename.endswith('.pdf')):
        raise ValueError("Don't know how to create this file %s, must end on .png or .pdf" % output_filename)

    if render_args is None:
        render_args = {'width': 10, 'height': 10}

    def plot():
        import pandas as pd
        import pyggplot
        data = pd.concat([plot_job.get_data() for plot_job in plot_jobs], axis=0)
        plot = plot_jobs[0].plot_function(data)
        if isinstance(facet_arguments, list):
            if facet_arguments:  # empty lists mean no faceting
                plot.facet(*facet_arguments)
        elif isinstance(facet_arguments, dict):
            plot.facet(**facet_arguments)
        else:
            raise ValueError("Don't know how to pass object of type %s to a function, needs to be a list or a dict. Was: %s" % (type(facet_arguments), facet_arguments))
        if not isinstance(plot, pyggplot.Plot):
            raise ppg_exceptions.JobContractError("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
        path = os.path.dirname(output_filename)
        if not os.path.exists(path):
            os.makedirs(path)
        if fiddle:
            fiddle(plot)
        plot.render(output_filename, **render_args)

    job = FileGeneratingJob(output_filename, plot)
    job.depends_on(ParameterInvariant(output_filename + '_params',
        (
            list(sorted([plot_job.output_filename for plot_job in plot_jobs])),  # so to detect new plot_jobs...
            render_args,
            facet_arguments
        )))
    job.depends_on(FunctionInvariant(output_filename + '_fiddle', fiddle))
    job.depends_on([plot_job.cache_job for plot_job in plot_jobs])
    job.depends_on(FunctionInvariant(output_filename + '_plot_combined', plot_jobs[0].plot_function))
    return job


class _CacheFileGeneratingJob(FileGeneratingJob):
    """A job that takes the results from it's callback and pickles it.
    data_loading_job is dependend on somewhere"""

    def __init__(self, job_id, calc_function, dl_job, emtpy_file_allowed = False):
        self.empty_file_allowed = emtpy_file_allowed
        if not hasattr(calc_function, '__call__'):
            raise ValueError("calc_function was not a callable")
        Job.__init__(self, job_id)  # FileGeneratingJob has no benefits for us
        if not hasattr(self, 'data_loading_job'):  # only do this the first time...
            self.cache_filename = job_id
            self.callback = calc_function
            self.data_loading_job = dl_job
            self.do_ignore_code_changes = False

    def invalidated(self, reason=''):
        logger.info("%s invalidated called, reason: %s" % (self, reason))
        try:
            logger.info("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except OSError:
            pass
        self.was_invalidated = True
        if (not self.data_loading_job.was_invalidated):
            self.data_loading_job.invalidated(reason)
        self._is_done_cache = False
        #Job.invalidated(self)  # no going back up the dependants... the dataloading job takes care of that

    def run(self):
        data = self.callback()
        op = open(self.cache_filename, 'wb')
        pickle.dump(data, op, pickle.HIGHEST_PROTOCOL)
        op.close()


class CachedAttributeLoadingJob(AttributeLoadingJob):
    """Like an AttributeLoadingJob, except that the callback value is pickled into
    a file called job_id and reread on the next run"""

    def __new__(cls, job_id, *args, **kwargs):
        if not isinstance(job_id, six.string_types):
            raise ValueError("cache_filename/job_id was not a string i jobect")

        return Job.__new__(cls, job_id + '_load')

    def __init__(self, cache_filename, target_object, target_attribute, calculating_function):
        if not isinstance(cache_filename, six.string_types):
            raise ValueError("cache_filename/job_id was not a string jobect")
        if not hasattr(calculating_function, '__call__'):
            raise ValueError("calculating_function was not a callable")
        if not isinstance(target_attribute, str):
            raise ValueError("attribute_name was not a string")
        abs_cache_filename = os.path.abspath(cache_filename)

        def do_load(cache_filename=abs_cache_filename):
            op = open(cache_filename, 'rb')
            data = pickle.load(op)
            op.close()
            return data

        AttributeLoadingJob.__init__(self, cache_filename + '_load', target_object, target_attribute, do_load)
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)

    def depends_on(self, jobs):
        self.lfg.depends_on(jobs)
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

    def invalidated(self, reason=''):
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)


class CachedDataLoadingJob(DataLoadingJob):
    """Like a DataLoadingJob, except that the callback value is pickled into
    a file called job_id and reread on the next run"""

    def __new__(cls, job_id, *args, **kwargs):
        if not isinstance(job_id, six.string_types):
            raise ValueError("cache_filename/job_id was not a string object")
        return Job.__new__(cls, job_id + '_load')  # plus load, so that the cached data goes into the cache_filename passed to the constructor...

    def __init__(self, cache_filename, calculating_function, loading_function):
        if not isinstance(cache_filename, six.string_types):
            raise ValueError("cache_filename/job_id was not a string object")
        if not hasattr(calculating_function, '__call__'):
            raise ValueError("calculating_function was not a callable")
        if not hasattr(loading_function, '__call__'):
            raise ValueError("loading_function was not a callable")
        abs_cache_filename = os.path.abspath(cache_filename)

        def do_load(cache_filename=abs_cache_filename):
            op = open(cache_filename, 'rb')
            try:
                data = pickle.load(op)
            except Exception as e:
                raise ValueError("Unpickling error in file %s - original error was %s" % (cache_filename, str(e)))
            op.close()
            loading_function(data)
        DataLoadingJob.__init__(self, cache_filename + '_load', do_load)  # todo: adjust functioninvariant injection
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)
        self.calculating_function = calculating_function
        self.loading_function = loading_function

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            # this job should depend on that, not the lazy filegenerating one...
            Job.depends_on(self, FunctionInvariant(self.job_id + '_func', self.loading_function)) # we don't want to depend on 'callback', that's our tiny wrapper, but on the loading_function instead.

    def __str__(self):
        try:
            return "%s (job_id=%s,id=%s\n Calc calcback: %s:%s\nLoad callback: %s:%s)" % (self.__class__.__name__, self.job_id, id(self), self.calculating_function.__code__.co_filename, self.calculating_function.__code__.co_firstlineno, self.loading_function.__code__.co_filename, self.loading_function.__code__.co_firstlineno)
        except AttributeError:
            return "%s(job_id=%s, callbacks unset"% ( self.__class__.__name__, self.job_id)

    def depends_on(self, jobs):
        self.lfg.depends_on(jobs)
        return self
         # The loading job itself should not depend on the preqs
         # because then the preqs would even have to be loaded if
         # the lfg had run already in another job
         # and dataloadingpreqs could not be unloaded right away
         # Now, if you need to have a more complex loading function,
         # that also requires further jobs being loaded (integrating, etc)
         # either add in another DataLoadingJob dependand on this CachedDataLoadingJob
         # or call Job.depends_on(this_job, jobs) yourself.
        #return Job.depends_on(self, jobs)

    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()
        self.do_ignore_code_changes = True

    def __del__(self):
        self.lfg = None

    def invalidated(self, reason=''):
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)

class MemMappedDataLoadingJob(DataLoadingJob):
    """Like a DataLoadingJob that returns a numpy array. That array get's stored to a file, and memmapped back in later on.
    Note that it's your job to del your memmapped reference to get it garbage collectable...
    """
    def __new__(cls, job_id, *args, **kwargs):
        if is_pypy:
            raise NotImplementedError("Numpypy currently does not support memmap(), there is no support for MemMappedDataLoadingJob using pypy.")
        if not isinstance(job_id, six.string_types):
            raise ValueError("cache_filename/job_id was not a string object")
        return Job.__new__(cls, job_id + '_load')  # plus load, so that the cached data goes into the cache_filename passed to the constructor...

    def __init__(self, cache_filename, calculating_function, loading_function, dtype):
        if not isinstance(cache_filename, six.string_types):
            raise ValueError("cache_filename/job_id was not a string object")
        if not hasattr(calculating_function, '__call__'):
            raise ValueError("calculating_function was not a callable")
        if not hasattr(loading_function, '__call__'):
            raise ValueError("loading_function was not a callable")
        abs_cache_filename = os.path.abspath(cache_filename)
        self.dtype = dtype
        def do_load(cache_filename=abs_cache_filename):
            import numpy
            data = numpy.memmap(cache_filename, self.dtype, mode='r')
            loading_function(data)
        DataLoadingJob.__init__(self, cache_filename + '_load', do_load)  #
        def do_calc(cache_filename=abs_cache_filename):
            import numpy
            data = calculating_function()
            if not isinstance(data, numpy.ndarray):
                raise ppg_exceptions.JobContractError("Data must be a numpy array")
            if data.dtype != self.dtype:
                raise ppg_exceptions.JobContractError("Data had wrong dtype. Expected %s, was %s" % (self.dtype, data.dtype))
            mmap = numpy.memmap(cache_filename, self.dtype, 'w+', shape=data.shape)
            mmap[:] = data
            mmap.flush()
            del data
            del mmap
        lfg = FileGeneratingJob(cache_filename, do_calc)
        self.lfg = lfg
        Job.depends_on(self, lfg)
        self.calculating_function = calculating_function
        self.loading_function = loading_function

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.loading_function)) # we don't want to depend on 'callback', that's our tiny wrapper, but on the loading_function instead.
            self.lfg.depends_on(FunctionInvariant(self.job_id + '_calc_func', self.calculating_function))

    def __str__(self):
        return "%s (job_id=%s,id=%s\n Calc calcback: %s:%s\nLoad callback: %s:%s)" % (self.__class__.__name__, self.job_id, id(self), self.calculating_function.__code__.co_filename, self.calculating_function.__code__.co_firstlineno, self.loading_function.__code__.co_filename, self.loading_function.__code__.co_firstlineno)

    def depends_on(self, jobs):
        self.lfg.depends_on(jobs)
        return self
    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()
        self.do_ignore_code_changes = True

    def __del__(self):
        self.lfg = None

    def invalidated(self, reason=''):
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)

def NotebookJob(notebook_filename, auto_detect_dependencies = True):
    """Run an ipython notebook if it changed, or any of the jobs for filenames it references
    changed"""
    notebook_name = notebook_filename
    if '/' in notebook_name:
        notebook_name = notebook_name[notebook_name.rfind('/') + 1:]
    if not os.path.exists('cache/notebooks'):
        os.mkdir('cache/notebooks')
    sentinel_file = os.path.join('cache','notebooks', hashlib.md5(notebook_filename).hexdigest() + ' ' + notebook_name +  '.html')
    ipy_cache_file = os.path.join('cache','notebooks', hashlib.md5(notebook_filename).hexdigest() + '.ipynb')
    return _NotebookJob([sentinel_file, ipy_cache_file], notebook_filename, auto_detect_dependencies)

class _NotebookJob(MultiFileGeneratingJob):

    def __init__(self, files, notebook_filename, auto_detect_dependencies):
        sentinel_file, ipy_cache_file = files
        def run_notebook():
            import subprocess
            shutil.copy(notebook_filename, ipy_cache_file)
            p = subprocess.Popen(['runipy', '-o', os.path.abspath(ipy_cache_file), '--no-chdir'], cwd=os.path.abspath('.'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != 0:
                raise ValueError("Ipython notebook %s error return.\nstdout:\n%s\n\nstderr:\n%s" % (notebook_filename, stdout, stderr))
            output_file = open(sentinel_file, 'wb')
            p = subprocess.Popen(['ipython', 'nbconvert', os.path.abspath(ipy_cache_file), '--to', 'html', '--stdout'], stdout=output_file, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != 0:
                raise ValueError("Ipython nbconvert error. stderr: %s" % (stderr,))
            output_file.close()
        self.auto_detect_dependencies = auto_detect_dependencies
        self.notebook_filename = notebook_filename
        MultiFileGeneratingJob.__init__(self, files, run_notebook)

    def inject_auto_invariants(self):
       deps = [
               FileChecksumInvariant(self.notebook_filename)
               ]
       if self.auto_detect_dependencies:
           with open(self.notebook_filename, 'rb') as op:
               raw_text = op.read()
       for job_name, job  in  util.global_pipegraph.jobs.items():
           if isinstance(job, MultiFileGeneratingJob):
               for fn in job.filenames:
                   if fn in raw_text:
                       deps.append(job)
           elif isinstance(job, FileGeneratingJob):
                if job.job_id in raw_text:
                       deps.append(job)
