import logging
import traceback
import multiprocessing
import Queue
import sys
import cStringIO

class LocalSystem:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it
    
    It uses multiprocessing
    """

    def __init__(self, max_cores_to_use = 8):
        self.max_cores_to_use = max_cores_to_use
        self.slave = None
        self.cores_available = max_cores_to_use
        self.memory_available = 50 * 1024 * 1024 * 1024 #50 gigs ;), todo, update to actual memory + swap...

    def spawn_slaves(self):
        return {
                'LocalSlave': LocalSlave(self)
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
                was_ok, job_id_done, stdout, stderr,exception, trace = self.que.get(block=True, timeout=15) #was there a job done?
                logging.info("Job returned: %s, was_ok: %s" % (job_id_done, was_ok))
                job = self.pipegraph.jobs[job_id_done]
                job.stdout = stdout
                job.stderr = stderr
                job.exception = exception
                job.trace = trace
                job.failed = not was_ok
                if job.failed:
                    if job.exception:
                        logging.info("Exception: %s" % repr(exception))
                        logging.info("Trace: %s" % trace)
                    logging.info("stdout: %s" % stdout)
                    logging.info("stderr: %s" % stderr)

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
                pass

class LocalSlave:

    def __init__(self, rc):
        self.rc = rc

    def spawn(self, job):
        logging.info("Spawning %s" % job.job_id)
        for preq in job.prerequisites:
            if preq.is_loadable():
                preq.load()
        if job.cores_needed == -1:
            self.rc.cores_available = 0
        else:
            self.rc.cores_available -= job.cores_needed
        p = multiprocessing.Process(target=self.run_a_job, args=[job,])
        p.start()
        print p.pid

    def run_a_job(self, job): #this runs in the spawned processes...
        stdout = cStringIO.StringIO()
        stderr = cStringIO.StringIO()
        old_stdout = sys.stdout 
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ''
        try:
            job.run()
            was_ok = True
            exception = None
        except Exception, e:
            print 'job threw exception', e
            trace = traceback.format_exc()
            was_ok = False
            exception = e
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        self.rc.que.put(
                (
                    was_ok,
                    job.job_id, 
                    stdout,
                    stderr,
                    exception,
                    trace
                ))

            


