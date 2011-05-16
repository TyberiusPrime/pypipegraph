import logging
import os
import traceback
import multiprocessing
import Queue
import sys
import cStringIO
import cloudpickle
import cPickle
import exceptions
import util

class LocalSystem:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it
    
    It uses multiprocessing
    """

    def __init__(self, max_cores_to_use = 8):
        self.max_cores_to_use = max_cores_to_use
        self.slave = LocalSlave(self)
        self.cores_available = max_cores_to_use
        self.memory_available = 50 * 1024 * 1024 * 1024 #50 gigs ;), todo, update to actual memory + swap...
        self.timeout = 15

    def spawn_slaves(self):
        return {
                'LocalSlave': self.slave
                }

    def get_resources(self):
        return {
                'LocalSlave': {'cores': self.cores_available,
                    'memory': self.memory_available}
                }

    def enter_loop(self):
        self.que = multiprocessing.Queue()
        logging.info("Starting first batch of jobs")
        self.pipegraph.start_jobs()
        while True:
            try:
                logging.info("Listening to que")
                was_ok, job_id_done, stdout, stderr,exception, trace, new_jobs = self.que.get(block=True, timeout=self.timeout) #was there a job done?
                logging.info("Job returned: %s, was_ok: %s" % (job_id_done, was_ok))
                job = self.pipegraph.jobs[job_id_done]
                job.stdout = stdout
                job.stderr = stderr
                job.exception = exception
                job.trace = trace
                job.failed = not was_ok
                if job.failed:
                    try:
                        logging.info("Before depickle %s" % type(exception))
                        job.exception = cPickle.loads(exception)
                        logging.info("After depickle %s" % type(job.exception))
                        logging.info("exception stored at %s" % (job))
                    except cPickle.UnpicklingError:#some exceptions can't be pickled, so we send a string instead#some exceptions can't be pickled, so we send a string instead
                        pass
                    if job.exception:
                        logging.info("Exception: %s" % repr(exception))
                        logging.info("Trace: %s" % trace)
                    logging.info("stdout: %s" % stdout)
                    logging.info("stderr: %s" % stderr)
                if new_jobs:
                    if not job.modifies_jobgraph():
                        job.exception = exceptions.JobContractError("%s created jobs, but was not a job with modifies_jobgraph() returning True" % job)
                        job.failed = True
                    else:
                        new_jobs = cPickle.loads(new_jobs)
                        logging.info("We retrieved %i new jobs from %s"  % (len(new_jobs), job))
                        self.pipegraph.new_jobs_generated_during_runtime(new_jobs)

                more_jobs = self.pipegraph.job_executed(job)
                if job.cores_needed == -1:
                    self.cores_available = self.max_cores_to_use
                else:
                    self.cores_available += job.cores_needed
                if not more_jobs: #this means that all jobs are done and there are no longer any more running...
                    break
                self.pipegraph.start_jobs()
                 
            except Queue.Empty, IOError: #either timeout, or the que failed
                logging.info("Timout")
                self.slave.check_for_dead_jobs()
                pass
        self.slave.check_for_dead_jobs()

class LocalSlave:

    def __init__(self, rc):
        self.rc = rc
        logging.info("LocalSlave pid: %i (runs in MCP!)" % os.getpid())
        self.process_to_job = {}

    def spawn(self, job):
        logging.info("Slave: Spawning %s" % job.job_id)
        logging.info("Slave: preqs are %s" % [preq.job_id for preq in job.prerequisites])
        for preq in job.prerequisites:
            if preq.is_loadable():
                logging.info("Slave: Loading %s" % preq)
                preq.load()
        if job.cores_needed == -1:
            self.rc.cores_available = 0
        else:
            self.rc.cores_available -= job.cores_needed
        if job.modifies_jobgraph():
            logging.info("Slave: Running %s in slave" % job)
            self.run_a_job(job)
        else:
            p = multiprocessing.Process(target=self.run_a_job, args=[job,])
            p.start()
            self.process_to_job[p] = job
            print p.pid

    def run_a_job(self, job): #this runs in the spawned processes, except for job.modifies_jobgraph()==True jobs
        stdout = cStringIO.StringIO()
        stderr = cStringIO.StringIO()
        old_stdout = sys.stdout 
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ''
        new_jobs = False
        try:
            temp = job.run()
            was_ok = True
            exception = None
            if job.modifies_jobgraph():
                new_jobs = self.prepare_jobs_for_transfer(temp)
            elif temp:
                raise exceptions.JobContractError("Job returned a value (which should be new jobs generated here) without having modifies_jobgraph() returning True")
        except Exception, e:
            #print 'job threw exception', e
            trace = traceback.format_exc()
            was_ok = False
            exception = e
            try:
                exception = cPickle.dumps(exception)
            except cPickle.PicklingError: #some exceptions can't be pickled, so we send a string instead
                exception = str(exception)
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        self.rc.que.put(
                (
                    was_ok, #failed?
                    job.job_id, #id...
                    stdout, #output
                    stderr, #output
                    exception, 
                    trace, 
                    new_jobs,
                ))

    def prepare_jobs_for_transfer(self, job_dict):
        """When traveling back, jobs-dependencies are wrapped as strings - this should 
        prevent nasty suprises"""
        #package as strings
        for job in job_dict.values():
            job.prerequisites = [preq.job_id for preq in job.prerequisites]
            job.dependants = [dep.job_id for dep in job.dependants]
        #unpackanging is don in new_jobs_generated_during_runtime
        self.rc.pipegraph.new_jobs_generated_during_runtime(job_dict)
        return [] # The LocalSlave does not need to serialize back the jobs, it already is running in the space of the MCP

            

    def check_for_dead_jobs(self):
        remove = []
        for proc in self.process_to_job:
            if not proc.is_alive():
                remove.append(proc)
                if proc.exitcode != 0:
                    job = self.process_to_job[proc]
                    self.rc.que.put((
                            False,
                            job.job_id, 
                            'no stdout available', 
                            'no stderr available', 
                            cPickle.dumps(exceptions.JobDied(proc.exitcode)),
                            '',
                            False #no new jobs
                            ))
        for proc in remove:
            del self.process_to_job[proc]

