import exceptions
import logging
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
    def __init__(self, jobs):
        jobs = list(jobs)
        for job in jobs:
            if not isinstance(job, Job):
                raise exceptions.ValueError("%s was not a job object" % job)
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

class Job(object):

    def __new__(cls, job_id, *args, **kwargs):
        logging.info("New for %s %s" % (cls, job_id))
        if not isinstance(job_id, str):
            raise exceptions.JobContractError("Job_id must be a string")
        if not job_id in util.job_uniquifier:
            util.job_uniquifier[job_id] = object.__new__(cls)
        else:
            if util.job_uniquifier[job_id].__class__ != cls:
                raise exceptions.JobContractError("Same job id, different job classes for %s" % job_id)

        if util.global_pipegraph is None:
            raise ValueError("You must first instanciate a pypipegraph before creating jobs""")


        return util.job_uniquifier[job_id]

    def __getnewargs__(self):  #so that unpickling works
        return (self.job_id, )

    def __init__(self, job_id):
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
        util.global_pipegraph.add_job(util.job_uniquifier[job_id])

    def depends_on(self, job_joblist_or_list_of_jobs):
        if isinstance(job_joblist_or_list_of_jobs, Job):
            job_joblist_or_list_of_jobs = [job_joblist_or_list_of_jobs]

        for job in job_joblist_or_list_of_jobs:
            if self in job.prerequisites:
                raise exceptions.CycleError("Cycle adding %s to %s" % (self.job_id, job.job_id))
        self.prerequisites.add(job)
        return self

    def ignore_code_changes(self):
        raise ValueError("This job does not support ignore_code_changes")

    def inject_auto_invariants(self):
        pass

    def get_invariant(self, old):
        return False

    def is_done(self):
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

    def invalidated(self):
        self.was_invalidated = True
        for dep in self.dependants:
            dep.invalidated()

    def can_run_now(self):
        logging.info("can_run_now %s" % self)
        for preq in self.prerequisites:
            if preq.is_done():
                if preq.was_invalidated and not preq.was_run and not preq.is_loadable(): 
                    #was_run is necessary, a filegen job might have already created the file (and written a bit to it), but that does not mean that it's done enough to start the next one. Was_run means it has returned.
                    #On the other hand, it might have been a job that didn't need to run, then was_invalidated should be false.
                    #or it was a loadable job anyhow, then it doesn't matter.
                    logging.info("case 1 - false")
                    return False #false means no way
                else:
                    logging.info("case 2 - delay") #but we still need to try the other preqs if it was ok
                    pass
            else:
                logging.info("case 3 - false because of %s" % preq)
                return False
        logging.info("case 4 - true")
        return True

    def run(self):
        pass

    def check_prerequisites_for_cleanup(self):
        for preq in self.prerequisites:
            logging.info("check_prerequisites_for_cleanup %s" % preq)
            all_done = True
            for dep in preq.dependants:
                if dep.failed or not dep.was_run:
                    all_done = False
                    break
            if all_done:
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

lambdare = re.compile('<code	object	<lambda>	at	0x[a-f0-9]+[^>]+')
class FunctionInvariant(Job):
    def __init__(self, job_id, function):
        Job.__init__(self, job_id)
        self.function = function

    def runs_in_slave(self):
        return False

    def get_invariant(self, old):
        if not id(self.function.func_code) in util.func_hashes:
            util.func_hashes[id(self.function.func_code)] = self.dis_str(self.function.func_code)
        return util.func_hashes[id(self.function.func_code)] 

    def dis_str(self, code): #TODO: replace with bytecode based smarter variant
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
        res = lambdare.sub('lambda', res)
        return res

class ParameterInvariant(Job):

    def __init__(self, job_id, parameters):
        self.parameters = parameters
        Job.__init__(self, job_id)

    def runs_in_slave(self):
        return False

    def get_invariant(self, old):
        return self.parameters

class FileTimeInvariant(Job):

    def __init__(self, filename):
        Job.__init__(self, filename)
        self.input_file = filename

    def runs_in_slave(self):
        return False

    def get_invariant(self, old):
        st = os.stat(self.input_file)
        return st[stat.ST_MTIME]


class FileChecksumInvariant(Job):

    def __init__(self, filename):
        Job.__init__(self, filename)
        self.input_file = filename

    def runs_in_slave(self):
        return False

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
        Job.__init__(self, output_filename)
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            logging.info("Injecting outa invariants %s" % self)
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))
        else:
            logging.info("not Injecting outa invariants %s" % self)

    def is_done(self):
        return util.output_file_exists(self.job_id)

    def invalidated(self):
        try:
            os.unlink(self.job_id)
        except OSError:
            pass
        Job.invalidated(self)

    def run(self):
        try:
            self.callback()
        except Exception, e:
            sys.stderr.write(traceback.format_exc())
            try:
                if self.rename_broken:
                    shutil.move(self.job_id, self.job_id + '.broken')
                else:
                    os.unlink(self.job_id)
            except (OSError, IOError):
                pass
            raise e
        if not util.output_file_exists(self.job_id):
            raise exceptions.JobContractError("%s did not create its file" % (self.job_id,))


class MultiFileGeneratingJob(FileGeneratingJob):

    def __new__(cls, filenames, function, rename_broken = False):
        job_id = ":".join(sorted(str(x) for x in filenames))
        return Job.__new__(cls, job_id)

    def __init__(self, filenames, function, rename_broken = False):
        job_id = ":".join(sorted(str(x) for x in filenames))
        Job.__init__(self, job_id)
        self.filenames = filenames
        self.callback = function
        self.rename_broken = rename_broken
        self.do_ignore_code_changes = False

    def is_done(self):
        for fn in self.filenames:
            if not util.output_file_exists(fn):
                return False
        return True

    def invalidated(self):
        for fn in self.filenames:
            try:
                logging.info("Removing %s" % fn)
                os.unlink(fn)
            except OSError:
                pass
        Job.invalidated(self)

    def run(self):
        try:
            self.callback()
        except:
            if self.rename_broken:
                for fn in self.filenames:
                    try:
                        shutil.move(fn, fn + '.broken')
                    except IOError:
                        pass
            else:
                for fn in self.filenames:
                    try:
                        os.unlink(fn)
                    except OSError:
                        pass
        if not self.is_done():
            raise exceptions.JobContractError("%s did not create all of its files" % (self.job_id,))
    def runs_in_slave(self):
        return False

class TempFileGeneratingJob(FileGeneratingJob):

    def cleanup(self):
        try:
            if self.rename_broken:
                shutil.move(self.job_id, self.job_id + '.broken')
            else:
                os.unlink(self.job_id)
        except (OSError, IOError):
            pass

    def runs_in_slave(self):
        return False

class DataLoadingJob(Job):
    def __init__(self, job_id, callback):
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
            return
        for preq in self.prerequisites: #load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        self.callback()
        self.was_loaded = True

    def is_done(self): #delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

class AttributeLoadingJob(DataLoadingJob):

    def __init__(self, job_id, object, attribute_name, callback):
        self.object = object
        self.attribute_name = attribute_name
        DataLoadingJob.__init__(self, job_id, callback)

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def load(self):
        logging.info("%s load" % self)
        if self.was_loaded:
            return
        for preq in self.prerequisites: #load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        logging.info("setting %s on id %i in pid %i" % (self.attribute_name, id(self.object), os.getpid()))
        setattr(self.object, self.attribute_name, self.callback())
        self.was_loaded = True

    def is_loadable(self):
        return True

    def is_done(self): #delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

    def cleanup(self):
        logging.info("Cleanup on %s" % self.attribute_name)
        try:
            delattr(self.object, self.attribute_name)
        except AttributeError: #this can happen if you have a messed up DependencyInjectionJob, but it would block the messed up reporting...
            pass

    def __str__(self):
        return "AttributeLoadingJob (job_id=%s,id=%i,target=%i)" % (self.job_id, id(self), id(self.object))

class _GraphModifyingJob(Job):

    def modifies_jobgraph(self):
        return True

    def is_done(self):
        return self.was_run




class DependencyInjectionJob(_GraphModifyingJob):
    def __init__(self, job_id, callback):
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def run(self):
        #this is different form JobGeneratingJob.run in it's checking of the contract
        util.global_pipegraph.new_jobs = {}
        logging.info("new_jobs id %s"  %id(util.global_pipegraph.new_jobs))
        self.callback()
        #we now need to fill new_jobs.dependants
        for job in util.global_pipegraph.jobs.values():
            for nw in util.global_pipegraph.new_jobs.values():
                if nw in job.prerequisites:
                    logging.info("Checking %s against %s - %s" % (nw, job, job in self.dependants))
                    if not job in self.dependants:
                        raise exceptions.JobContractError("DependencyInjectionJob %s tried to inject %s into %s, but %s was not dependand on the DependencyInjectionJob" % (self, nw, job, job))
                    nw.dependants.add(job)
        #I need to check: All new jobs are now prereqs of my dependands
        #I also need to check that none of the jobs that ain't dependand on me have been injected
        for job in util.global_pipegraph.jobs.values():
            if job in self.dependants:
                for new_job in util.global_pipegraph.new_jobs.values():
                    if not new_job in job.prerequisites:
                        raise exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was not added to the prerequisites of %s" % (self.job_id, new_job.job_id, job.job_id))
            else:
                for new_job in util.global_pipegraph.new_jobs.values():
                    if new_job in job.prerequisites or job in new_job.dependants: #no connect_graph building so far...
                        raise exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the prerequisites of %s, but %s was not dependant on the DependencyInjectionJob" % (self.job_id, new_job.job_id, job.job_id))
        res = util.global_pipegraph.new_jobs
        logging.info('returning %i new jobs' % len(res))
        logging.info('%s' % ",".join(res.keys()))
        util.global_pipegraph.new_jobs = False
        return res

class JobGeneratingJob(_GraphModifyingJob):
    def __init__(self, job_id, callback):
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))


    def run(self):
        util.global_pipegraph.new_jobs = {}
        self.callback()
        #I need to check: All new jobs are now prereqs of my dependands
        #I also need to check that none of the jobs that ain't dependand on me have been injected
        for job in util.global_pipegraph.jobs.values():
            for new_job in util.global_pipegraph.new_jobs.values():
                if new_job in job.prerequisites:
                    raise exceptions.JobContractError("JobGeneratingJob %s created a job %s that was added to the prerequisites of %s, which is invalid. Use a DependencyInjectionJob instead, this one might only create 'leave' nodes" % (self.job_id, new_job.job_id, job.job_id))
        res = util.global_pipegraph.new_jobs
        util.global_pipegraph.new_jobs = False
        return res

def PlotJob(output_filename, calc_function, plot_function): #a convienence wrapper for a quick plotting
    """Calculate some data for plotting, cache it in cache/outputfilename, and plot from there.
    creates two jobs, a plot_job (FileGeneratingJob) and a cache_job (FileGeneratingJob), 
    returns plot_job, with plot_jbo.cache_job = cache_job
    """

    
    if not (output_filename.endswith('.png') or output_filename.endswith('.pdf')):
        raise ValueError("Don't know how to create this file %s, must end on .png or .pdf" % output_filename)
    import pydataframe
    import pyggplot
    cache_filename = os.path.join('cache', output_filename)
    def run_calc():
        df = calc_function()
        if not isinstance(df, pydataframe.DataFrame):
            raise exceptions.JobContractError("%s.calc_function did not return a DataFrame, was %s " % (output_filename, df.__class__))
        try:
            os.makedirs(os.path.dirname(cache_filename))
        except OSError:
            pass
        of = open(cache_filename, 'wb')
        cPickle.dump(df, of, cPickle.HIGHEST_PROTOCOL)
        of.close()
    def run_plot():
        of = open(cache_filename, 'rb')
        df = cPickle.load(of)
        of.close()
        plot = plot_function(df)
        if not isinstance(plot, pyggplot.Plot):
            raise exceptions.JobContractError("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
        plot.render(output_filename)

    cache_job = FileGeneratingJob(cache_filename, run_calc)
    cache_job.depends_on(FunctionInvariant(output_filename + '.calcfunc', calc_function))
    plot_job = FileGeneratingJob(output_filename, run_plot)
    plot_job.depends_on(FunctionInvariant(output_filename + '.plotfunc', plot_function))
    plot_job.depends_on(cache_job)
    plot_job.cache_job = cache_job
    return plot_job


class _LazyFileGeneratingJob(Job):
    """A job that only needs to be done if it's
    data_loading_job is dependend on somewhere"""

    def __init__(self, job_id, calc_function, dl_job):
        Job.__init__(self, job_id)
        self.cache_filename = job_id
        self.callback = calc_function
        self.data_loading_job = dl_job
        self.do_ignore_code_changes = False

    def __del__(self):
        self.data_loading_job = None

    def is_done(self):
        if util.output_file_exists(self.job_id):
            return True
        else:
            if self.data_loading_job.dependants:
                return False
            else:
                return True

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            func_invariant = FunctionInvariant(self.job_id + '_func', self.callback)
            self.depends_on(func_invariant)
            self.data_loading_job.depends_on(func_invariant)
        else:
            logging.info("not Injecting outa invariants %s" % self)

    def invalidated(self):
        try:
            os.unlink(self.job_id)
        except OSError:
            pass
        self.was_invalidated = True
        #Job.invalidated(self) #no going back up the dependants... the cached job takes care of that

    def run(self):
        data = self.callback()
        op = open(self.cache_filename, 'wb')
        cPickle.dump(data, op, cPickle.HIGHEST_PROTOCOL)
        op.close()

class CachedJob(AttributeLoadingJob):
    
    def __new__(cls, job_id, *args, **kwargs):
        return Job.__new__(cls, job_id + '_load')
    
    def __init__(self, cache_filename, target_object, target_attribute, calculating_function):
        def do_load():
            op = open(cache_filename, 'rb')
            data = cPickle.load(op)
            op.close()
            return data
        AttributeLoadingJob.__init__(self, cache_filename + '_load', target_object, target_attribute, do_load)
        lfg = _LazyFileGeneratingJob(cache_filename, calculating_function, self)
        self.depends_on(lfg)
        self.lfg = lfg

    def ignore_code_changes(self):
        self.lfg.ignore_code_changes()

    def __del__(self):
        self.lfg = None

    def invalidated(self):
        self.lfg.invalidated()
        Job.invalidated(self)

    

    #TODOs







