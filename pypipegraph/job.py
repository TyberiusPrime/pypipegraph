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
        self.exceptions = None
        self.was_run = False
        self.was_loaded = False
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
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

    def run(self):
        pass

    def check_prerequisites_for_cleanup(self):
        for preq in self.prerequisites:
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
        Job.__init__(filename)

    def runs_in_slave(self):
        return False

    def get_invariant(self, old):
        st = os.stat(self.input_file)
        return st[stat.ST_MTIME]

class FileChecksumInvariant(Job):

    def __init__(self, filename):
        Job.__init__(filename)

    def runs_in_slave(self):
        return False

    def get_invariant(self, old):
        st = os.stat(self.input_file)
        filetime = st[stat.ST_MTIME]
        filesize = st[stat.ST_SIZE]
        if not old or old[1] != filesize or old[0] != filetime:
            return filetime, filesize, self.checksum()
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
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

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
        DataLoadingJob.__init__(self, job_id, callback)

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def load(self):
        if self.was_loaded:
            return
        for preq in self.prerequisites: #load whatever is necessary...
            if preq.is_loadable():
                preq.load()
        self.callback()
        self.was_loaded = True

    def is_loadable(self):
        return True

    def is_done(self): #delegate to preqs... passthrough of 'not yet done'
        for preq in self.prerequisites:
            if not preq.is_done():
                return False
        return True

class DependencyInjectionJob(Job):
    def __init__(self, job_id, callback):
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def is_done(self):
        return False

    def modifies_jobgraph(self):
        return False

    def run(self):
        util.global_pipeline.new_jobs = {}
        self.callback()
        #I need to check: All new jobs are now prereqs of my dependands
        #I also need to check that none of the jobs that ain't dependand on me have been injected
        for job in util.global_pipeline.jobs.values():
            if job in self.dependants:
                for new_job in util.global_pipeline.new_jobs.values():
                    if not new_job in job.prerequisites:
                        raise exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was not added to the prerequisites of %s" % (self.job_id, new_job.job_id, job.job_id))
            else:
                for new_job in util.global_pipeline.new_jobs.values():
                    if new_job in job.prerequisites:
                        raise exceptions.JobContractError("DependencyInjectionJob %s created a job %s that was added to the prerequisites of %s, but %s was not dependant on the DependencyInjectionJob" % (self.job_id, new_job.job_id, job.job_id))
        return util.global_pipeline.new_jobs()



class JobGeneratingJob(Job):
    def __init__(self, job_id, callback):
        Job.__init__(self, job_id)
        self.callback = callback
        self.do_ignore_code_changes = False

    def ignore_code_changes(self):
        self.do_ignore_code_changes = True

    def inject_auto_invariants(self):
        if not self.do_ignore_code_changes:
            self.depends_on(FunctionInvariant(self.job_id + '_func', self.callback))

    def is_done(self):
        return False

    def modifies_jobgraph(self):
        return False

    def run(self):
        util.global_pipeline.new_jobs = {}
        self.callback()
        #I need to check: All new jobs are now prereqs of my dependands
        #I also need to check that none of the jobs that ain't dependand on me have been injected
        for job in util.global_pipeline.jobs.values():
            for new_job in util.global_pipeline.new_jobs.values():
                if new_job in job.prerequisites:
                    raise exceptions.JobContractError("JobGeneratingJob %s created a job %s that was added to the prerequisites of %s, which is invalid. Use a DependencyInjectionJob instead, this one might only create 'leave' nodes" % (self.job_id, new_job.job_id, job.job_id))
        return util.global_pipeline.new_jobs()

def PlotJob(output_filename, calc_function, plot_function): #a convienence wrapper for a quick plotting
    if not (output_filename.endswith('.png') or output_filename.endswith('.pdf')):
        raise ValueError("Don't know how to create this file %s, must end on .png or .pdf" % output_filename)
    import pydataframe
    import pyggplot
    cache_filename = os.path.join('cache', output_filename)
    def run_calc():
        df = calc_function
        if not isinstance(df, pydataframe.DataFrame):
            raise exceptions.JobGeneratingJob("%s.calc_function did not return a DataFrame " % (output_filename))
        try:
            os.makedirs(os.path.dirname(cache_filename))
        except OSError:
            pass
        of = open(cache_filename, 'wb')
        cPickle.dump(of, df, cPickle.HIGHEST_PROTOCOL)
        of.close()
    def run_plot():
        of = open(cache_filename, 'rb')
        df = cPickle.load(of)
        of.close()
        plot = plot_function(df)
        if not isinstance(plot, pyggplot.Plot):
            raise exceptions.JobGeneratingJob("%s.plot_function did not return a pyggplot.Plot " % (output_filename))
        plot.render(output_filename)

    cache_job = FileGeneratingJob(cache_filename, run_calc)
    cache_job.depends_on(FunctionInvariant(output_filename + '.calcfunc', calc_function))
    plot_job = FileGeneratingJob(output_filename, run_plot)
    plot_job.depends_on(FunctionInvariant(output_filename + '.plotfunc', plot_function))
    plot_job.depends_on(cache_job)
    return plot_job


class CachedJob(DataLoadingJob):

    def __init__(self, cache_filename, target_object, target_attribute, calculating_function):
        pass

    #TODOs







