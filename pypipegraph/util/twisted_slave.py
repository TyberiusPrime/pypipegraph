"""A twisted slave.
Opens up a communications port, print's its portnumber
and answers computation requests in conjuction with a LocalTwistedSlave.
Jobs are typically executed via multiprocessing"""



from twisted.internet import reactor
from twisted.internet.protocol import Factory, ProcessProtocol
from twisted.internet.defer import Deferred
from twisted.protocols import amp

import sys
sys.path.append("../../")
import pypipegraph
twisted_fork = pypipegraph.twisted_fork
cloudpickle = pypipegraph.cloudpickle
util = pypipegraph.util
util.is_remote = True 
sys.path.remove('../../')
logger = util.start_logging('slave')
import cPickle
import cStringIO
import traceback

import exceptions
import sys
import os
up_path = os.path.join(os.path.dirname(__file__), '../../')
sys.path.append(up_path)
import pypipegraph
sys.path.remove(up_path)

magickey = sys.argv[1]

global_pipegraph = None

class DataCollectingProcessProtocol(ProcessProtocol):
    def __init__(self, slave, job_id):
        self.slave = slave
        self.job_id = job_id

    def connectionMade(self):
        self.stdout = ''
        self.stderr = ''
        pass

    def outReceived(self, data):
        self.stdout += data

    def errReceived(self, data):
        self.stderr += data

    def processExited(self, status):
        self.exit_code = status.value.exitCode
        logger.info("Fork return %i for %s" % (self.exit_code, self.job_id))
        job_id = self.job_id
        stdout = self.stdout
        stderr = self.stderr
        global_pipegraph.jobs[job_id].was_run = True
        if self.exit_code == 0:
            was_ok = True
            global_pipegraph.jobs[job_id].failed = False
            #if job_id != self.job_id:
                #raise ValueError("Job_id was not identical!")
            exception = "" #nothing happend...
            trace = ""
        else:
            global_pipegraph.jobs[job_id].failed = True
            was_ok = False
            stderr = self.stderr + " Process failed"
            exception = "Exit code was not 0"
            trace = ''
        new_jobs = "" #new jobs only can be made in modifies_jobgraph() jobs that run locally in the slave
        logger.info("Sending back result")
        check_prerequisites_for_cleanup(global_pipegraph.jobs[job_id])
        self.slave.send_result(cPickle.dumps((was_ok, job_id, stdout, stderr, exception, trace, new_jobs)))


def prepare_jobs_for_transfer(job_dict):
    """When traveling back, jobs-dependencies are wrapped as strings - this should 
    prevent nasty suprises"""
    #package as strings
    for job in job_dict.values():
        job.prerequisites = [preq.job_id for preq in job.prerequisites]
        job.dependants = [dep.job_id for dep in job.dependants]
    res = cloudpickle.dumps(job_dict)
    for job in job_dict.values():
        job.prerequisites = set([global_pipegraph.jobs[job_id] for job_id in job.prerequisites])
        job.dependants = set([global_pipegraph.jobs[job_id] for job_id in job.dependants])

    return res

def check_prerequisites_for_cleanup(job):
    for preq in job.prerequisites:
        logger.info("check_prerequisites_for_cleanup %s" % preq)
        all_done = True
        for dep in preq.dependants:
            if dep.failed or not dep.was_run:
                logger.info("Cleanup canceled because of %s failed: %s, was_run: %s" % (dep, dep.failed, dep.was_run))
                all_done = False
                break
        if all_done:
            logger.info("Can do cleanup")
            preq.cleanup()

def run_local(job, slave):
    logger.info("Running local %s" % job)
    res = _run_job(job, True)
    job.failed = not res[0]
    job.was_run = True
    check_prerequisites_for_cleanup(job)
    slave.send_result(cPickle.dumps(res))

def run_spawned(job, slave):
    logger.info("Now spawning - my pid is %s" % os.getpid())
    p = twisted_fork.ForkedProcess(reactor, lambda job=job: _run_job(job, False), DataCollectingProcessProtocol(slave,job.job_id))


def _run_job(job, is_local): #so this runs in the forked code... 
    global logger
    if not is_local:
        logger = util.start_logging('FORK')
    stdout = cStringIO.StringIO()
    stderr = cStringIO.StringIO()
    old_stdout = sys.stdout 
    old_stderr = sys.stderr
    #sys.stdout = stdout
    sys.stderr = stderr
    trace = ''
    new_jobs = False
    was_ok = False
    try:
        temp = job.run()
        print 'temp is', temp
        was_ok = True
        exception = None
        logger.info("%s was modifies_jobgraph == %s" % (job.job_id, job.modifies_jobgraph()))
        if job.modifies_jobgraph(): #should be true!
            logger.info("Returing %i new jobs" % len(temp))
            new_jobs = prepare_jobs_for_transfer(temp)
        elif temp:
            raise pypipegraph.ppg_exceptions.JobContractError("Job returned a value (which should be new jobs generated here) without having modifies_jobgraph() returning True")
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
    logger.info("Returning for %s, was_ok %s" % (job.job_id, was_ok))
    if is_local:
        return (
                was_ok, #failed?
                job.job_id, #id...
                stdout, #output
                stderr, #output
                exception, 
                trace, 
                new_jobs,
                )
    else:
        print cPickle.dumps(( #so the parent process can read it...
                was_ok, #failed?
                job.job_id, #id...
                stdout, #output
                stderr, #output
                exception, 
                trace, 
                new_jobs,
                ))
        sys.stdout.flush()
        sys.stderr.flush()
        if was_ok:
            os._exit(0)
        else:
            os._exit(1)


class Slave(amp.AMP):

    def transmit_pipegraph(self, jobs):
        global global_pipegraph
        try:
            pypipegraph.new_pipegraph(pypipegraph.resource_coordinators.DummyResourceCoordinator())
            global_pipegraph = pypipegraph.util.global_pipegraph
            jobs = cPickle.loads(jobs) #which fills the global pipegraph...
            logger.info("received pipegraph")
            logger.info("job len %i" % len(jobs))
            for name in jobs:
                logger.info("adding %s" % name)
                global_pipegraph.add_job(jobs[name])
            logger.info("Loaded pipegraph. Num jobs: %i" % len(global_pipegraph.jobs))
            global_pipegraph.running = True
            return {'ok': True, 'exception': ''}
        except Exception, e:
            logger.info("Pipegraph loading failed")
            logger.info(traceback.format_tb())
            return {"ok": False, 'exception': str(e)}
    pypipegraph.messages.TransmitPipegraph.responder(transmit_pipegraph)

    def start_job(self, job_id):
        job = global_pipegraph.jobs[job_id]
        for preq in job.prerequisites:
            if preq.is_loadable():
                logger.info("Loading %s " % preq.job_id)
                preq.load()

        logger.info("Running job: %s" % job_id)
        if job.modifies_jobgraph():
            run_local(job,self)
        else:
            run_spawned(job, self)
        return {}
    pypipegraph.messages.StartJob.responder(start_job)

    def shutdown(self):
        logger.info("Received shutdown, registering later")
        def inner_shutdown():
            logger.info("actually doing shutdown")
            self.transport.loseConnection()
            reactor.stop() #which get's us back into main...
            #reactor.callLater(0, inner_inner)
        reactor.callLater(0, inner_shutdown) #so the call returns,,,.
        return {}
    pypipegraph.messages.ShutDown.responder(shutdown)
    
    def magickey(self):
        logger.info("Queried for magic key, answering %s" % magickey)
        return {'key': magickey}
    pypipegraph.messages.MagicKey.responder(magickey)
    
        

    def send_result(self,pickle):
        self.callRemote(pypipegraph.messages.JobEnded, arg_tuple_pickle=pickle)
        pass




def main():
    pf = Factory()
    pf.protocol = Slave
    port = 50001
    sys.stdout.write('listening to %i' %  port)
    sys.stderr.flush()

    try:
        reactor.listenTCP(port, pf)
        sys.stdout.close()
        sys.stdout = sys.stderr
    except Exception, e:
        print e
        sys.exit(11)
    reactor.run()

if __name__ == '__main__':  
    main()







