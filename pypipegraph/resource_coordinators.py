import logging
import time
logger = logging.getLogger('ppg.RC')
logger.setLevel(logging.INFO)
import os
import traceback
import multiprocessing
import Queue
import sys
import cStringIO
import cloudpickle
import cPickle
import exceptions
import ppg_exceptions
import subprocess

import messages
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.protocols import amp

class LocalSystem:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it
    
    It uses multiprocessing and the LocalSlave
    """

    def __init__(self, max_cores_to_use = 8):
        self.max_cores_to_use = max_cores_to_use #todo: update to local cpu count...
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
        logger.info("Starting first batch of jobs")
        self.pipegraph.start_jobs()
        while True:
            try:
                logger.info("Listening to que")
                slave_id, was_ok, job_id_done, stdout, stderr,exception, trace, new_jobs = self.que.get(block=True, timeout=self.timeout) #was there a job done?
                logger.info("Job returned: %s, was_ok: %s" % (job_id_done, was_ok))
                job = self.pipegraph.jobs[job_id_done]
                job.was_done_on.add(slave_id)
                job.stdout = stdout
                job.stderr = stderr
                job.exception = exception
                job.trace = trace
                job.failed = not was_ok
                if job.failed:
                    try:
                        logger.info("Before depickle %s" % type(exception))
                        job.exception = cPickle.loads(exception)
                        logger.info("After depickle %s" % type(job.exception))
                        logger.info("exception stored at %s" % (job))
                    except cPickle.UnpicklingError:#some exceptions can't be pickled, so we send a string instead#some exceptions can't be pickled, so we send a string instead
                        pass
                    if job.exception:
                        logger.info("Exception: %s" % repr(exception))
                        logger.info("Trace: %s" % trace)
                    logger.info("stdout: %s" % stdout)
                    logger.info("stderr: %s" % stderr)
                if new_jobs:
                    if not job.modifies_jobgraph():
                        job.exception = ppg_exceptions.JobContractError("%s created jobs, but was not a job with modifies_jobgraph() returning True" % job)
                        job.failed = True
                    else:
                        new_jobs = cPickle.loads(new_jobs)
                        logger.info("We retrieved %i new jobs from %s"  % (len(new_jobs), job))
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
                logger.info("Timout")
                self.slave.check_for_dead_jobs()
                pass
        self.slave.check_for_dead_jobs()

class LocalSlave:

    def __init__(self, rc):
        self.rc = rc
        self.slave_id = 'LocalSlave'
        logger.info("LocalSlave pid: %i (runs in MCP!)" % os.getpid())
        self.process_to_job = {}

    def spawn(self, job):
        logger.info("Slave: Spawning %s" % job.job_id)
        logger.info("Slave: preqs are %s" % [preq.job_id for preq in job.prerequisites])
        for preq in job.prerequisites:
            if preq.is_loadable():
                logger.info("Slave: Loading %s" % preq)
                preq.load()
        if job.cores_needed == -1:
            self.rc.cores_available = 0
        else:
            self.rc.cores_available -= job.cores_needed
        if job.modifies_jobgraph():
            logger.info("Slave: Running %s in slave" % job)
            self.run_a_job(job)
        else:
            p = multiprocessing.Process(target=self.run_a_job, args=[job,])
            p.start()
            self.process_to_job[p] = job

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
                raise ppg_exceptions.JobContractError("Job returned a value (which should be new jobs generated here) without having modifies_jobgraph() returning True")
        except Exception, e:
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
                    self.slave_id,
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
                            self.slave_id, 
                            False,
                            job.job_id, 
                            'no stdout available', 
                            'no stderr available', 
                            cPickle.dumps(ppg_exceptions.JobDied(proc.exitcode)),
                            '',
                            False #no new jobs
                            ))
        for proc in remove:
            del self.process_to_job[proc]




class LocalTwisted:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it
    
    It uses Twisted and one LocalTwistedSlave
    """

    def __init__(self, max_cores_to_use = 8):
        self.max_cores_to_use = max_cores_to_use #todo: update to local cpu count...
        self.slaves = {}
        self.cores_available = max_cores_to_use
        self.memory_available = 50 * 1024 * 1024 * 1024 #50 gigs ;), todo, update to actual memory + swap...
        self.timeout = 15

    def spawn_slaves(self):
        if self.slaves:
            raise ValueError("spawn_slaves called twice")
        self.slaves = {
                    'LocalSlave': LocalTwistedSlave(self)
                    }
        return self.slaves

    def get_resources(self):
        return {
                'LocalSlave': {'cores': self.cores_available,
                    'memory': self.memory_available}
                }

    def enter_loop(self):
        self.slaves_ready_count = 0
        logger.info("starting reactor")
        reactor.run()


    def start_when_ready(self, response, slave_id): #this get's called when a slave has connected and transmitted the pipegraph...
        status = response['ok']
        logger.info("start_when_ready for %s, status was %s" % (slave_id, status))
        if status:
            self.slaves_ready_count += 1
            if self.slaves_ready_count == len(self.slaves):
                logger.info("Now calling start_jobs")
                self.pipegraph.start_jobs()
        else:
            raise exceptions.CommunicationFailure("Slave %s could not load jobgraph. Exception: %s" % (slave_id, response['exception']))

    def slave_connection_failed(self, slave_id):
        raise exceptions.CommunicationFailure('Could not connect to slave %s' % slave_id)


    def job_ended(self, slave_id, was_ok, job_id_done, stdout, stderr,exception, trace, new_jobs):
        logger.info("Job returned: %s, was_ok: %s" % (job_id_done, was_ok))
        job = self.pipegraph.jobs[job_id_done]
        job.was_done_on.add(slave_id)
        job.stdout = stdout
        job.stderr = stderr
        job.exception = exception
        job.trace = trace
        job.failed = not was_ok
        if job.failed:
            try:
                logger.info("Before depickle %s" % type(exception))
                job.exception = cPickle.loads(exception)
                logger.info("After depickle %s" % type(job.exception))
                logger.info("exception stored at %s" % (job))
            except (cPickle.UnpicklingError, exceptions.EOFError):#some exceptions can't be pickled, so we send a string instead#some exceptions can't be pickled, so we send a string instead
                pass
            if job.exception:
                logger.info("Exception: %s" % repr(exception))
                logger.info("Trace: %s" % trace)
            logger.info("stdout: %s" % stdout)
            logger.info("stderr: %s" % stderr)
        if new_jobs:
            if not job.modifies_jobgraph():
                job.exception = exceptions.JobContractError("%s created jobs, but was not a job with modifies_jobgraph() returning True" % job)
                job.failed = True
            else:
                new_jobs = cPickle.loads(new_jobs)
                logger.info("We retrieved %i new jobs from %s"  % (len(new_jobs), job))
                self.pipegraph.new_jobs_generated_during_runtime(new_jobs)

        more_jobs = self.pipegraph.job_executed(job)
        if job.cores_needed == -1:
            self.cores_available = self.max_cores_to_use
        else:
            self.cores_available += job.cores_needed
        if not more_jobs: #this means that all jobs are done and there are no longer any more running, so we can return  now...
            reactor.stop()
        else:
            self.pipegraph.start_jobs()

class AMP_Return_Protocol(amp.AMP):
    def __init__(self, slave):
        self.slave = slave

    def job_ended(self, arg_tuple_pickle):
        self.slave.job_returned(arg_tuple_pickle)
        return {}
    messages.JobEnded.responder(job_ended)

    def __call__(self):
        return self




class LocalTwistedSlave:


    def __init__(self, rc):
        self.rc = rc
        self.slave_id = 'LocalTwistedSlave'
        self.start_subprocess()


    def start_subprocess(self):
        cmd = ['python', os.path.join(os.path.dirname(__file__), 'util', 'twisted_slave.py')]
        self.process = subprocess.Popen(cmd, cwd=os.getcwd())#, stdout=subprocess.PIPE)
        time.sleep(1)
        #port = self.process.stdout.read()
        #logger.info("Connecting to %s" % port)
        logger.info("Connecting to 500001")
        ClientCreator(reactor, AMP_Return_Protocol(self)).connectTCP('127.0.0.1', 50001).addCallbacks(
                self.connected,
                self.rc.slave_connection_failed
                )
        pass

    def connected(self, proto):
        self.amp = proto
        self.transmit_pipegraph(self.rc.pipegraph).addCallbacks(
                lambda result: self.rc.start_when_ready(result, self.slave_id), 
                self.transmit_pipegraph_failed)

    def transmit_pipegraph_failed(self, failure):
        self.rc.start_when_ready({'ok': False, 'exception': 'Unknown'}, self.slave_id)

    def transmit_pipegraph(self, pipegraph):
        data = cloudpickle.dumps(pipegraph.jobs, 0)
        print data
        cPickle.loads(data)
        return self.amp.callRemote(messages.TransmitPipegraph, jobs=data)

    def spawn(self, job):
        logger.info("Slave: Spawning %s" % job.job_id)
        self.amp.callRemote(messages.StartJob, job_id = job.job_id).addErrback(
                self.job_failed_to_start(job.job_id))

    def job_returned(self, encoded_argument):
        args = cPickle.loads(encoded_argument)
        self.rc.job_ended(self.slave_id, *args)

    def job_failed_to_start(self, job_id):
        def inner(failure, job_id = job_id):
            self.rc.job_ended(
                    self.slave_id, 
                    False,
                    job_id, 
                    'Not Available',
                    'Twisted communication error: %s' % failure, 
                    '', 
                    '', 
                    False)
        return inner


