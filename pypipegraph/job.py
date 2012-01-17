import ppg_exceptions
import util
logger = util.start_logging('job')
import re
import cStringIO
import os
import stat
import util
import sys
import dis
import shutil
import hashlib
import cPickle
import traceback


class JobList(object):
    """For when you want to return a list of jobs that mostly behaves like a single Job
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

class Job(object):

    def __new__(cls, job_id, *args, **kwargs):
        #logger.info("New for %s %s" % (cls, job_id))
        if not isinstance(job_id, str):
            raise ValueError("Job_id must be a string")
        if not job_id in util.job_uniquifier:
            util.job_uniquifier[job_id] = object.__new__(cls)
            util.job_uniquifier[job_id].job_id = job_id #doing it later will fail because hash apperantly might be called before init has run?
        else:
            if util.job_uniquifier[job_id].__class__ != cls:
                raise ppg_exceptions.JobContractError("Same job id, different job classes for %s" % job_id)

        if util.global_pipegraph is None:
            raise ValueError("You must first instanciate a pypipegraph before creating jobs""")


        return util.job_uniquifier[job_id]

    def __getnewargs__(self):  #so that unpickling works
        return (self.job_id, )

    def __init__(self, job_id):
        #logger.info("init for %s" % job_id)
        if not hasattr(self, 'dependants'): #test any of the following
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
            self.was_done_on = set() #on which slave(s) was this job run?
            self.was_loaded = False
            self.was_invalidated = False
            self.always_runs = False
            self.start_time = None
            self.stop_time = None
        #logger.info("adding self %s to %s" % (job_id, id(util.global_pipegraph)))
        util.global_pipegraph.add_job(util.job_uniquifier[job_id])

    def depends_on(self, job_joblist_or_list_of_jobs):
        #if isinstance(job_joblist_or_list_of_jobs, Job):
            #job_joblist_or_list_of_jobs = [job_joblist_or_list_of_jobs]
        if job_joblist_or_list_of_jobs is self:
            raise ppg_exceptions.CycleError("job.depends_on(self) would create a cycle: %s" % (self.job_id))

        for job in job_joblist_or_list_of_jobs:
            if not isinstance(job, Job):
                if hasattr(job, '__iter__'): #a nested list
                    self.depends_on(job) 
                    pass
                else:
                    raise ValueError("Can only depend on Job objects")
            else:
                if self in job.prerequisites:
                    raise ppg_exceptions.CycleError("Cycle adding %s to %s" % (self.job_id, job.job_id))
        for job in job_joblist_or_list_of_jobs:
            if isinstance(job, Job): #skip the lists here, they will be delegated to further calls during the checking... 
                self.prerequisites.add(job)
        return self
    
    def is_in_dependency_chain(self, other_job, max_depth):
        """check wether the other job is in this job's dependency chain.
        We check at most @max_depth levels, starting with this job (ie.
        max_depth = 2 means this job and it's children).
        Use a -1 for 'unlimited' (up to the maximum recursion depth) ;))
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
        raise ValueError("This job does not support ignore_code_changes")

    def inject_auto_invariants(self):
        pass

    def get_invariant(self, old):
        return False

    def is_done(self, depth = 0):
        return True

    def is_loadable(self):
        return False

    def load(self):
        if not self.is_loadable():
            raise ValueError("Called load() on a job that was not loadable")
        raise ValueError("Called load() on a j'ob that had is_loadable, but did not overwrite load() as it should")

        pass

    def runs_in_slave(self):
        return True

    def invalidated(self, reason = ''):
        logger.info("%s invalidated called, reason: %s" % (self, reason))
        self.was_invalidated = True
        for dep in self.dependants:
            dep.invalidated(reason = 'preq invalidated %s' % self)

    def can_run_now(self):
        #logger.info("can_run_now %s" % self)
        for preq in self.prerequisites:
            #logger.info("checking preq %s" % preq)
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable(): 
                    #was_run is necessary, a filegen job might have already created the file (and written a bit to it), but that does not mean that it's done enough to start the next one. Was_run means it has returned.
                    #On the other hand, it might have been a job that didn't need to run, then was_invalidated should be false.
                    #or it was a loadable job anyhow, then it doesn't matter.
                    #logger.info("case 1 - false %s" % preq)
                    return False #false means no way
                else:
                    #logger.info("case 2 - delay") #but we still need to try the other preqs if it was ok
                    pass
            else:
                #logger.info("case 3 - not done")
                return False
        #logger.info("case 4 - true")
        return True

    def list_blocks(self):
        """A helper to list what blocked this job from running - debug function"""
        res = []
        for preq in self.prerequisites:
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable():  #see can_run_now for why
                    res.append((preq, 'not run'))
                else:
                    #logger.info("case 2 - delay") #but we still need to try the other preqs if it was ok
                    pass
            else:
                #logger.info("case 3 - not done")
                res.append((preq,'not done'))
                break
                #return False
        return res

    def run(self):
        pass

    def check_prerequisites_for_cleanup(self):
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

    def cleanup(self):
        pass

    def modifies_jobgraph(self):
        return False

    def __eq__(self, other):
        return other is self

    def __hash__(self):
        return hash(self.job_id)
    
    def __add__(self, other_job):
        def iter():
            yield self
            for job in other_job:
                yield job
        return JobList(iter())

    def __iter__(self):
        yield self

    def __str__(self):
        return "%s (job_id=%s,id=%s)" % (self.__class__.__name__, self.job_id, id(self))

    def __repr__(self):
        return str(self)

class _InvariantJob(Job):
    #common code for all invariant jobs

    def depends_on(self, job_joblist_or_list_of_jobs):
        raise ppg_exceptions.JobContractError("Invariants can't have dependencies")

    def runs_in_slave(self):
        return False


inner_code_object_re = re.compile('<code	object	<[^>]+>	at	0x[a-f0-9]+[^>]+')
class FunctionInvariant(_InvariantJob):
    def __init__(self, job_id, function):
        if not hasattr(function, '__call__') and function is not None:
            raise ValueError("function was not a callable (or None)")
        Job.__init__(self, job_id)
        if hasattr(self, 'function') and function != self.function:
            raise ppg_exceptions.JobContractError("FunctionInvariant %s created twice with different functions: \n%s %i\n%s %i" % (
                job_id,
                self.function.func_code.co_filename, self.function.func_code.co_firstlineno,
                function.func_code.co_filename, function.func_code.co_firstlineno,
                ))
        self.function = function


    def get_invariant(self, old):
        if self.function is None:
            return None #since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
        if not id(self.function.func_code) in util.func_hashes:
            util.func_hashes[id(self.function.func_code)] = self.dis_code(self.function.__code__)
        return util.func_hashes[id(self.function.func_code)] 


    def dis_code(self, code): #TODO: replace with bytecode based smarter variant
        """'dissassemble' python code.
        
        Strips lambdas (they change address every execution otherwise)"""
        out = cStringIO.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = out
            dis.dis(code)
        finally:
            sys.stdout = old_stdout
        discode = out.getvalue().split("\n") 
        #now, eat of the line nos, if there are any
        res = []
        for row in discode:
            row = row.split()
            res.append("\t".join(row[1:]))
        res = "\n".join(res)
        res = inner_code_object_re.sub('lambda', res)
        for ii, constant in enumerate(code.co_consts):
            if hasattr(constant, 'co_code'):
                res += 'inner no %i' % ii
                res += self.dis_code(constant)
        return res


class ParameterInvariant(_InvariantJob):

    def __new__(cls, job_id, *parameters, **kwargs):
        job_id = 'PI' + job_id
        return Job.__new__(cls, job_id)

    def __init__(self, job_id, parameters):
        job_id = 'PI' + job_id
        self.parameters = parameters
        Job.__init__(self, job_id)

    def get_invariant(self, old):
        return self.parameters

class FileTimeInvariant(_InvariantJob):

    def __init__(self, filename):
        Job.__init__(self, filename)
        self.input_file = filename


    def get_invariant(self, old):
        st = os.stat(self.input_file)
        return st[stat.ST_MTIME]


class FileChecksumInvariant(_InvariantJob):

    def __init__(self, filename):
        Job.__init__(self, filename)
        self.input_file = filename


    def get_invariant(self, old):
        st = os.stat(self.input_file)
        filetime = st[stat.ST_MTIME]
        filesize = st[stat.ST_SIZE]
        if not old or old[1] != filesize or old[0] != filetime:
            chksum = self.checksum()
            if old and old[2] == chksum:
                raise util.NothingChanged((filetime, filesize, chksum))
            else:
                return filetime, filesize, chksum
        else:
            return old

    def checksum(self):
        op = open(self.job_id, 'rb')
        res = hashlib.md5(op.read()).hexdigest()
        op.close()
        return res




class FileGeneratingJob(Job):

    def __init__(self, output_filename, function, rename_broken = False):
        if not hasattr(function, '__call__'):
            raise ValueError("function was not a callable")
        Job.__init__(self, output_filename)
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            #logger.info("Injecting outa invariants %s" % self)
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))
        else:
            pass
            #logger.info("not Injecting outa invariants %s" % self)

    def is_done(self, depth = 0):
        return util.output_file_exists(self.job_id)

    def invalidated(self, reason = ''):
        try:
            logger.info("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except OSError:
            pass
        Job.invalidated(self, reason)

    def run(self):
        try:
            self.callback()
        except Exception, e:
            exc_info = sys.exc_info()
            sys.stderr.write(traceback.format_exc())
            try:
                if self.rename_broken:
                    shutil.move(self.job_id, self.job_id + '.broken')
                else:
                    logger.info("unlinking %s" % self.job_id)
                    os.unlink(self.job_id)
            except (OSError, IOError):
                pass
            raise exc_info[1], None, exc_info[2] #so we reraise as if in the original place
        if not util.output_file_exists(self.job_id):
            raise ppg_exceptions.JobContractError("%s did not create its file" % (self.job_id,))


class MultiFileGeneratingJob(FileGeneratingJob):

    def __new__(cls, filenames, *args, **kwargs):
        job_id = ":".join(sorted(str(x) for x in filenames))
        return Job.__new__(cls, job_id)

    def __getnewargs__(self):  #so that unpickling works
        return (self.filenames,)

    def __init__(self, filenames, function, rename_broken = False):
        if not hasattr(function, '__call__'):
            raise ValueError("function was not a callable")
        if not hasattr(filenames, '__iter__'):
            raise ValueError("filenames was not iterable")
        sorted_filenames = list(sorted(x for x in filenames))
        for x in sorted_filenames:
            if not isinstance(x, str) and not isinstance(x, unicode):
                raise ValueError("Not all filenames passed to MultiFileGeneratingJob were str or unicode objects")
        job_id = ":".join(sorted_filenames)
        Job.__init__(self, job_id)
        self.filenames = filenames
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False

    def is_done(self, depth = 0):
        for fn in self.filenames:
            if not util.output_file_exists(fn):
                return False
        return True

    def invalidated(self, reason = ''):
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
        except Exception, e:
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
                        logger.info("Removing %s" % fn)
                        os.unlink(fn)
                    except OSError:
                        pass
            raise exc_info[1], None, exc_info[2] #so we reraise as if in the original place
        if not self.is_done():
            missing_files = []
            for f in self.filenames:
                if not os.path.exists(f):
                    missing_files.append(f)
            raise ppg_exceptions.JobContractError("%s did not create all of its files.\nMissing were:\n %s" % (self.job_id,"\n".join(missing_files)))
    def runs_in_slave(self):
        return True

class TempFileGeneratingJob(FileGeneratingJob):

    def cleanup(self):
        logger.info("%s cleanup" % self)
        try:
            #the renaming will already have been done when FileGeneratingJob.run(self) was called...
            #if self.rename_broken:
                #shutil.move(self.job_id, self.job_id + '.broken')
            #else:
            logger.info("unlinking %s" % self.job_id)
            os.unlink(self.job_id)
        except (OSError, IOError):
            pass

    def runs_in_slave(self):
        return True

    def is_done(self, depth = 0):
        if util.output_file_exists(self.job_id):
            return True
        else:
            for dep in self.dependants:
                if (not dep.is_done()) and (not dep.is_loadable()):
                    return False
            return True

class DataLoadingJob(Job):
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
        for preq in self.prerequisites: #load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        self.callback()
        self.was_loaded = True

    def is_done(self, depth = 0): #delegate to preqs... passthrough of 'not yet done'
        logger.info("\t" * depth + "Checking is done on %s" % self)
        for preq in self.prerequisites:
            if not preq.is_done(depth = depth + 1):
                logger.info("\t" * depth + "failed on %s" % preq)
                return False
        logger.info("\t" * depth + "Passed")
        return True

class AttributeLoadingJob(DataLoadingJob):

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
        for preq in self.prerequisites: #load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        #logger.info("setting %s on id %i in pid %i" % (self.attribute_name, id(self.object), os.getpid()))
        setattr(self.object, self.attribute_name, self.callback())
        self.was_loaded = True

    def is_loadable(self):
        return True

    def is_done(self, depth = 0): #delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

    def cleanup(self):
        logger.info("Cleanup on %s" % self.attribute_name)
        try:
            delattr(self.object, self.attribute_name)
        except AttributeError: #this can happen if you have a messed up DependencyInjectionJob, but it would block the messed up reporting...
            pass

    def __str__(self):
        return "AttributeLoadingJob (job_id=%s,id=%i,target=%i)" % (self.job_id, id(self), id(self.object))

class _GraphModifyingJob(Job):

    def modifies_jobgraph(self):
        return True

    def is_done(self, depth = 0):
        return self.was_run




class DependencyInjectionJob(_GraphModifyingJob):
    def __init__(self, job_id, callback):
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def run(self):
        #this is different form JobGeneratingJob.run in it's checking of the contract
        util.global_pipegraph.new_jobs = {}
        logger.info("DependencyInjectionJob.dependants = %s %s" % (", ".join(str(x) for x in self.dependants), id(self.dependants)))
        self.callback()
        logger.info("DependencyInjectionJob.dependants after callback = %s %s" % (", ".join(str(x) for x in self.dependants), id(self.dependants)))
        logger.info("new_jobs count: %i, id %s"  % ( len(util.global_pipegraph.new_jobs), id(util.global_pipegraph.new_jobs)))
        for new_job in util.global_pipegraph.new_jobs.values():
            new_job.inject_auto_invariants()
        #we now need to fill new_jobs.dependants
        #these implementations are much better than the old for loop based ones
        #but still could use some improvements
        #but at least for the first one, I don't see how to remove the remaining loops.
        logger.info("Now checking first step for dependency injection violations") 
        new_job_set = set(util.global_pipegraph.new_jobs.values())
        if True:
            for job in util.global_pipegraph.jobs.values():
                for nw in new_job_set.intersection(job.prerequisites):
                    #logger.info("Checking %s against %s - %s" % (nw, job, job in self.dependants))
                    if not job in self.dependants:
                        raise ppg_exceptions.JobContractError("DependencyInjectionJob %s tried to inject %s into %s, but %s was not dependand on the DependencyInjectionJob. It was dependand on %s though" % (self, nw, job, job, nw.prerequisites))
                    nw.dependants.add(job)
        #I need to check: All new jobs are now prereqs of my dependands

        #I also need to check that none of the jobs that ain't dependand on me have been injected
        logger.info("Checking for dependency injection violations")
        if True:
            for job in util.global_pipegraph.jobs.values():
                if job in self.dependants:
                    for new_job in util.global_pipegraph.new_jobs.values():
                        if not job.is_in_dependency_chain(new_job,5): #1 for the job, 2 for auto dependencies, 3 for load jobs, 4 for the dependencies of load jobs... 5 seems to work in pratice.
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was not added to the prerequisites of %s" % (self.job_id, new_job.job_id, job.job_id))
                else:
                    preq_intersection = job.prerequisites.intersection(new_job_set)
                    if preq_intersection:
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the prerequisites of %s, but was not dependant on the DependencyInjectionJob" % (self.job_id, preq_intersection, job.job_id))
                    dep_intersection = job.prerequisites.intersection(new_job_set)
                    if dep_intersection:
                            raise ppg_exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the dependants of %s, but was not dependant on the DependencyInjectionJob" % (self.job_id, dep_intersection, job.job_id))

        res = util.global_pipegraph.new_jobs
        logger.info('returning %i new jobs' % len(res))
        logger.info('%s' % ",".join(res.keys()))
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        return res

class JobGeneratingJob(_GraphModifyingJob):
    def __init__(self, job_id, callback):
        if not hasattr(callback, '__call__'):
            raise ValueError("callback was not a callable")
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False
        self.always_runs = True

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))


    def run(self):
        logger.info("Storing new jobs in %s" % id(util.global_pipegraph))
        util.global_pipegraph.new_jobs = {}
        self.callback()
        for new_job in util.global_pipegraph.new_jobs.values():
            new_job.inject_auto_invariants()
        #I need to check: All new jobs are now prereqs of my dependands
        #I also need to check that none of the jobs that ain't dependand on me have been injected
        new_job_set = set(util.global_pipegraph.new_jobs.values())
        for job in util.global_pipegraph.jobs.values():
            if new_job_set.intersection(job.prerequisites):
                    raise ppg_exceptions.JobContractError("JobGeneratingJob %s created a job that was added to the prerequisites of %s, which is invalid. Use a DependencyInjectionJob instead, this one might only create 'leave' nodes" % (self.job_id, job.job_id))
        res = util.global_pipegraph.new_jobs
        util.global_pipegraph.tranfer_new_jobs()
        util.global_pipegraph.new_jobs = False
        logger.info("Returning from %s" % self)
        return res

class PlotJob(FileGeneratingJob): 
    """Calculate some data for plotting, cache it in cache/output_filename , and plot from there.
    creates two jobs, a plot_job (this one) and a cache_job (FileGeneratingJob, in self.cache_job), 
    """
    def __init__(self, output_filename, calc_function, plot_function, render_args = None, skip_table = False, skip_caching = False):
        if not isinstance(output_filename , str) or isinstance(output_filename , unicode):
            raise ValueError("output_filename was not a string or unicode")
        if not (output_filename.endswith('.png') or output_filename.endswith('.pdf')):
            raise ValueError("Don't know how to create this file %s, must end on .png or .pdf" % output_filename)

        self.output_filename = output_filename
        self.table_filename = self.output_filename + '.tsv'
        self.calc_function = calc_function
        self.plot_function = plot_function
        self.skip_caching = skip_caching
        if render_args is None:
            render_args = {}
        self.render_args = render_args
        self._fiddle = None

        import pydataframe
        import pyggplot
        if not self.skip_caching:
            self.cache_filename = os.path.join('cache', output_filename)
            def run_calc():
                df = calc_function()
                if not isinstance(df, pydataframe.DataFrame):
                    do_raise = True
                    if isinstance(df, dict): #might be a list dfs...
                        do_raise = False
                        for x in df.values():
                            if not isinstance(x, pydataframe.DataFrame):
                                do_raise = True
                                break
                    if do_raise:
                        raise ppg_exceptions.JobContractError("%s.calc_function did not return a DataFrame (or dict of such), was %s " % (output_filename, df.__class__))
                try:
                    os.makedirs(os.path.dirname(self.cache_filename))
                except OSError:
                    pass
                of = open(self.cache_filename, 'wb')
                cPickle.dump(df, of, cPickle.HIGHEST_PROTOCOL)
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
                df = self.get_data()
                if isinstance(df, pydataframe.DataFrame):
                    pydataframe.DF2TSV().write(df, self.table_filename)
                else:
                    pydataframe.DF2Excel().write(df, self.table_filename) #must have been a dict...
            table_gen_job = FileGeneratingJob(self.table_filename, dump_table)
            table_gen_job.depends_on(cache_job)
            self.table_job = table_gen_job
        else:
            self.table_job = None

    def add_another_plot(self, output_filename, plot_function, render_args = None):
        """Add another plot job that runs on the same data as the original one (calc only done once)"""
        import pyggplot

        def run_plot():
            df = self.get_data()
            plot = plot_function(df)
            if not isinstance(plot, pyggplot.Plot):
                raise ppg_exceptions.JobContractError("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
            if not 'width' in render_args and hasattr(plot, 'width'):
                render_args['width'] = plot.width
            if not 'height' in render_args and hasattr(plot, 'height'):
                render_args['height'] = plot.width
            plot.render(output_filename, **render_args)
        job = FileGeneratingJob(output_filename, run_plot)
        job.depends_on(ParameterInvariant(self.output_filename + '_params', render_args))
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
        #FileGeneratingJob.depends_on(self, other_job) #just like the cached jobs, the plotting does not depend on the loading of prerequisites
        if self.skip_caching:
            Job.depends_on(self, other_job)
        elif hasattr(self, 'cache_job') and not other_job is self.cache_job: #activate this after we have added the invariants...
            self.cache_job.depends_on(other_job)
        return self

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            if not self.skip_caching:
                self.cache_job.depends_on(FunctionInvariant(self.job_id + '.calcfunc', self.calc_function))
            FileGeneratingJob.depends_on(self, FunctionInvariant(self.job_id + '.plotfunc', self.plot_function))

    def get_data(self):
        if self.skip_caching:
            return self.calc_function()
        else:
            of = open(self.cache_filename, 'rb')
            df = cPickle.load(of)
            of.close()
            return df

def CombinedPlotJob(output_filename, plot_jobs, facet_arguments, render_args = None):
    if not isinstance(output_filename , str) or isinstance(output_filename , unicode):
        raise ValueError("output_filename was not a string or unicode")
    if not (output_filename.endswith('.png') or output_filename.endswith('.pdf')):
        raise ValueError("Don't know how to create this file %s, must end on .png or .pdf" % output_filename)

    if render_args is None:
        render_args = {'width': 10, 'height': 10}
    def plot():
        import pydataframe
        import pyggplot
        data = pydataframe.combine([plot_job.get_data() for plot_job in plot_jobs])
        plot = plot_jobs[0].plot_function(data)
        if isinstance(facet_arguments, list):
            if facet_arguments: #empty lists mean no faceting
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
        plot.render(output_filename, **render_args)
    job = FileGeneratingJob(output_filename, plot)
    job.depends_on(ParameterInvariant(output_filename + '_params', 
        (
            list(sorted([plot_job.output_filename for plot_job in plot_jobs])), #so to detect new plot_jobs...
            render_args, 
            facet_arguments
        )))
    job.depends_on([plot_job.cache_job for plot_job in plot_jobs])
    return job






class _CacheFileGeneratingJob(FileGeneratingJob):
    """A job that takes the results from it's callback and pickles it.
    data_loading_job is dependend on somewhere"""

    def __init__(self, job_id, calc_function, dl_job):
        if not hasattr(calc_function, '__call__'):
            raise ValueError("calc_function was not a callable")
        Job.__init__(self, job_id) #FileGeneratingJob has no benefits for us
        if not hasattr(self, 'data_loading_job'): #only do this the first time...
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
        #Job.invalidated(self) #no going back up the dependants... the dataloading job takes care of that

    def run(self):
        data = self.callback()
        op = open(self.cache_filename, 'wb')
        cPickle.dump(data, op, cPickle.HIGHEST_PROTOCOL)
        op.close()

class CachedAttributeLoadingJob(AttributeLoadingJob):
    
    def __new__(cls, job_id, *args, **kwargs):
        if not isinstance(job_id, str) and not isinstance(job_id, unicode):
            raise ValueError("cache_filename/job_id was not a str/unicode jobect")

        return Job.__new__(cls, job_id + '_load')
    
    def __init__(self, cache_filename, target_object, target_attribute, calculating_function):
        if not isinstance(cache_filename, str) and not isinstance(cache_filename, unicode):
            raise ValueError("cache_filename/job_id was not a str/unicode jobect")
        if not hasattr(calculating_function, '__call__'):
            raise ValueError("calculating_function was not a callable")
        if not isinstance(target_attribute, str):
            raise ValueError("attribute_name was not a string")
        abs_cache_filename = os.path.abspath(cache_filename)
        def do_load(cache_filename = abs_cache_filename):
            op = open(cache_filename, 'rb')
            data = cPickle.load(op)
            op.close()
            return data
        AttributeLoadingJob.__init__(self, cache_filename + '_load', target_object, target_attribute, do_load)
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)

    def depends_on(self, jobs):
        self.lfg.depends_on(jobs)
        return self
        #The loading job itself should not depend on the preqs
        #because then the preqs would even have to be loaded if 
        #the lfg had run already in another job
        #and dataloadingpreqs could not be unloaded right away
        #(and anyhow, the loading job is so simple it doesn't need
        #anything but the lfg output file
        #return Job.depends_on(self, jobs)

    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()
        self.do_ignore_code_changes = True

    def __del__(self):
        self.lfg = None

    def invalidated(self, reason = ''):
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)

class CachedDataLoadingJob(DataLoadingJob):
    
    def __new__(cls, job_id, *args, **kwargs):
        if not isinstance(job_id, str) and not isinstance(job_id, unicode):
            raise ValueError("cache_filename/job_id was not a str/unicode jobect")
        return Job.__new__(cls, job_id + '_load') #plus load, so that the cached data goes into the cache_filename passed to the constructor...
    
    def __init__(self, cache_filename, calculating_function, loading_function):
        if not isinstance(cache_filename, str) and not isinstance(cache_filename, unicode):
            raise ValueError("cache_filename/job_id was not a str/unicode jobect")
        if not hasattr(calculating_function, '__call__'):
            raise ValueError("calculating_function was not a callable")
        if not hasattr(loading_function, '__call__'):
            raise ValueError("loading_function was not a callable")
        abs_cache_filename = os.path.abspath(cache_filename)

        def do_load(cache_filename = abs_cache_filename):
            op = open(cache_filename, 'rb')
            try:
                data = cPickle.load(op)
            except Exception, e:
                raise ValueError("Unpickling error in file %s - original error was %s" % (cache_filename, e))
            op.close()
            loading_function(data)
        DataLoadingJob.__init__(self, cache_filename + '_load', do_load) #todo: adjust functioninvariant injection
        lfg = _CacheFileGeneratingJob(cache_filename, calculating_function, self)
        self.lfg = lfg
        Job.depends_on(self, lfg)

    def depends_on(self, jobs):
        self.lfg.depends_on(jobs)
        return self
        #The loading job itself should not depend on the preqs
        #because then the preqs would even have to be loaded if 
        #the lfg had run already in another job
        #and dataloadingpreqs could not be unloaded right away
        #Now, if you need to have a more complex loading function,
        #that also requires further jobs being loaded (integrating, etc)
        #either add in another DataLoadingJob dependand on this CachedDataLoadingJob
        #or call Job.depends_on(this_job, jobs) yourself.
        #return Job.depends_on(self, jobs)

    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()
        self.do_ignore_code_changes = True

    def __del__(self):
        self.lfg = None

    def invalidated(self, reason = ''):
        if not self.lfg.was_invalidated:
            self.lfg.invalidated(reason)
        Job.invalidated(self, reason)
 

    #TODOs







