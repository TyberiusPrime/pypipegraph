from __future__ import print_function

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

import time
from . import util
logger = util.start_logging('RC')
import os
import codecs
import traceback
import multiprocessing
import kitchen
try: 
    import Queue
    queue = Queue
except ImportError:
    import queue
import sys
try:
    import cPickle
    pickle = cPickle
except ImportError:
    import pickle
from . import ppg_exceptions
import tempfile

try:
    from twisted.internet import reactor
    from twisted.internet.protocol import ClientCreator, ProcessProtocol
    from twisted.protocols import amp
    from . import messages
    twisted_available = True
except ImportError:
        twisted_available = False



class DummyResourceCoordinator:
    """For the calculating slaves. so it throws exceptions..."""


def get_memory_available():
    if hasattr(os, "sysconf"):
        if "SC_NPROCESSORS_ONLN" in os.sysconf_names: # a linux or unix system
            op = open("/proc/meminfo", 'r')
            d = op.read()
            op.close()
            mem_total = d[d.find('MemTotal:') + len('MemTotal:'):]
            mem_total = mem_total[:mem_total.find("kB")].strip()
            swap_total = d[d.find('SwapTotal:') + len('SwapTotal:'):]
            swap_total = swap_total[:swap_total.find("kB")].strip()
            physical_memory = int(mem_total) * 1024
            swap_memory = int(swap_total) * 1024
            return physical_memory, swap_memory
        else: #assume it's mac os x
            physical_memory = int(os.popen2("sysctl -n hw.memsize")[1].read())
            swap_memory = physical_memory * 10 # mac os x virtual memory system uses *all* available boot device size, so a heuristic should work well enough
            return physical_memory, swap_memory
    else:
        raise ValueError("get_memory_available() does not know how to get available memory on your system.")


class LocalSystem:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it

    It uses multiprocessing and the LocalSlave
    """

    def __init__(self, max_cores_to_use=util.CPUs(), profile = False):
        self.max_cores_to_use = max_cores_to_use  # todo: update to local cpu count...
        self.slave = LocalSlave(self)
        self.cores_available = max_cores_to_use
        self.physical_memory, self.swap_memory = get_memory_available()
        self.timeout = 5
        self.profile = profile

    def spawn_slaves(self):
        return {
                'LocalSlave': self.slave
                }

    def get_resources(self):
        res = {
                'LocalSlave': {  # this is always the maximum available - the graph is handling the bookeeping of running jobs
                    'cores': self.cores_available,
                    'physical_memory': self.physical_memory,
                    'swap_memory': self.swap_memory,
                    }
                }
        logger.info('get_resources, result %s - %s' % (id(res), res))
        return res

    def enter_loop(self):
        self.spawn_slaves()
        self.que = multiprocessing.Queue()
        logger.info("Starting first batch of jobs")
        self.pipegraph.start_jobs()
        while True:
            self.slave.check_for_dead_jobs()  # whether time out or or job was done, let's check this...
            self.see_if_output_is_requested()
            try:
                logger.info("Listening to que")
                slave_id, was_ok, job_id_done, stdout, stderr, exception, trace, new_jobs = self.que.get(block=True, timeout=self.timeout)  # was there a job done?
                logger.info("Job returned: %s, was_ok: %s" % (job_id_done, was_ok))
                logger.info("Remaining in que (approx): %i" % self.que.qsize())
                job = self.pipegraph.jobs[job_id_done]
                job.was_done_on.add(slave_id)
                job.stdout = stdout
                job.stderr = stderr
                job.exception = exception
                job.trace = trace
                job.failed = not was_ok
                job.stop_time = time.time()
                if job.start_time:
                    logger.info("%s runtime: %is" % (job_id_done, job.stop_time - job.start_time))
                if job.failed:
                    try:
                        if job.exception.startswith('STR'.encode('UTF-8')):
                            job.exception = job.exception[3:]
                            raise pickle.UnpicklingError("String Transmission")  # what an ugly control flow...
                        logger.info("Before depickle %s" % type(exception))
                        job.exception = pickle.loads(exception)
                        logger.info("After depickle %s" % type(job.exception))
                        logger.info("exception stored at %s" % (job))
                    except pickle.UnpicklingError:  # some exceptions can't be pickled, so we send a string instead
                        pass
                    if job.exception:
                        logger.info("Exception: %s" % repr(exception))
                        logger.info("Trace: %s" % trace)
                    logger.info("stdout: %s" % stdout)
                    logger.info("stderr: %s" % stderr)
                if not new_jobs is False:
                    if not job.modifies_jobgraph():
                        job.exception = ppg_exceptions.JobContractError("%s created jobs, but was not a job with modifies_jobgraph() returning True" % job)
                        job.failed = True
                    else:
                        new_jobs = pickle.loads(new_jobs)
                        logger.info("We retrieved %i new jobs from %s" % (len(new_jobs), job))
                        self.pipegraph.new_jobs_generated_during_runtime(new_jobs)

                more_jobs = self.pipegraph.job_executed(job)
                #if job.cores_needed == -1:
                    #self.cores_available = self.max_cores_to_use
                #else:
                    #self.cores_available += job.cores_needed
                if not more_jobs:  # this means that all jobs are done and there are no longer any more running...
                    break
                self.pipegraph.start_jobs()

            except (queue.Empty, IOError):  # either timeout, or the que failed
                #logger.info("Timout")
                #for job in self.pipegraph.running_jobs:
                    #logger.info('running %s' % (job, ))
                pass
        self.que.close()
        self.que.join_thread()  # wait for the que to close
        logger.info("Leaving loop")

    def see_if_output_is_requested(self):
        import select
        try:
            if select.select([sys.stdin], [], [], 0)[0]:
                ch = sys.stdin.read(1)  # enter pressed...
                self.pipegraph.print_running_jobs()
                pass
        finally:
            pass
            #termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


class LocalSlave:

    def __init__(self, rc):
        self.rc = rc
        self.slave_id = 'LocalSlave'
        logger.info("LocalSlave pid: %i (runs in MCP!)" % os.getpid())
        self.process_to_job = {}

    def spawn(self, job):
        logger.info("Slave: Spawning %s" % job.job_id)
        job.start_time = time.time()
        #logger.info("Slave: preqs are %s" % [preq.job_id for preq in job.prerequisites])
        preq_failed = False
        if not job.is_final_job:  # final jobs don't load their (fake) prereqs.
            for preq in job.prerequisites:
                if preq.is_loadable():
                    logger.info("Slave: Loading %s" % preq)
                    if not self.load_job(preq):
                        preq_failed = True
                        break
        if preq_failed:
            self.rc.que.put(
                    (
                        self.slave_id,
                        False,  # failed?
                        job.job_id,  # id...
                        '',  # output
                        '',  # output
                        'STRPrerequsite failed'.encode('UTF-8'),
                        '',
                        False,
                    ))
        else:
            if job.modifies_jobgraph():
                logger.info("Slave: Running %s in slave" % job)
                stdout = tempfile.SpooledTemporaryFile(mode='w+')
                stderr = tempfile.SpooledTemporaryFile(mode='w+')
                stdout = kitchen.text.converters.getwriter('utf8')(stdout)
                stderr = kitchen.text.converters.getwriter('utf8')(stderr)
                self.run_a_job(job, stdout, stderr)
                logger.info("Slave: returned from %s in slave, data was put" % job)
            else:
                logger.info("Slave: Forking for %s" % job.job_id)
                stdout = tempfile.TemporaryFile(mode='w+') #no more spooling - it doesn't get passed back
                stderr = tempfile.TemporaryFile(mode='w+')
                stdout = kitchen.text.converters.getwriter('utf8')(stdout)
                stderr = kitchen.text.converters.getwriter('utf8')(stderr)
                stdout.fileno()
                stderr.fileno()
                p = multiprocessing.Process(target=self.run_a_job, args=[job, stdout, stderr, False])
                job.run_info = "pid = %s" % (p.pid, )
                job.stdout_handle = stdout
                job.stderr_handle = stderr
                p.start()
                self.process_to_job[p] = job
                logger.info("Slave, returning to start_jobs")

    def load_job(self, job):  # this executes a load job returns false if an error occured
        stdout = tempfile.SpooledTemporaryFile(mode='w')
        stderr = tempfile.SpooledTemporaryFile(mode='w')
        stdout = kitchen.text.converters.getwriter('utf8')(stdout)
        stderr = kitchen.text.converters.getwriter('utf8')(stderr)
                
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ''
        new_jobs = False
        try:
            job.load()
            was_ok = True
            exception = None
        except Exception as e:
            trace = traceback.format_exc()
            was_ok = False
            exception = e
            try:
                exception = pickle.dumps(exception)
            except Exception as e:  # some exceptions can't be pickled, so we send a string instead
                exception = str(exception)
        stdout.seek(0, os.SEEK_SET)
        stdout_text = stdout.read()
        stdout.close()
        stderr.seek(0, os.SEEK_SET)
        stderr_text = stderr.read()
        stderr.close()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        if not was_ok:
            self.rc.que.put(
                    (
                        self.slave_id,
                        was_ok,  # failed?
                        job.job_id,  # id...
                        stdout_text,  # output
                        stderr_text,  # output
                        exception,
                        trace,
                        new_jobs,
                    ))
        return was_ok

    def _profile_job(self, job):
        self.temp = job.run()
    def run_a_job(self, job, stdout, stderr, is_local=True):  # this runs in the spawned processes, except for job.modifies_jobgraph()==True jobs
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ''
        new_jobs = False
        util.global_pipegraph.new_jobs = None  # ignore jobs created here.
        try:
            if self.rc.profile:
                import cProfile
                cProfile.runctx('self._profile_job(job)', globals(), locals(), filename = "%s.prof" % (id(job),))
                temp = self.temp
                del self.temp
            else:
                temp = job.run()
            was_ok = True
            exception = None
            if job.modifies_jobgraph():
                new_jobs = self.prepare_jobs_for_transfer(temp)
            elif temp:
                raise ppg_exceptions.JobContractError("Job returned a value (which should be new jobs generated here) without having modifies_jobgraph() returning True")
        except Exception as e:
            trace = traceback.format_exc()
            was_ok = False
            exception = e
            try:
                exception = pickle.dumps(e)
            except Exception as e:  # some exceptions can't be pickled, so we send a string instead
                try:
                    exception = bytes("STR", 'UTF-8') + bytes(e)
                except TypeError:
                    exception = str(e)
        stdout.seek(0, os.SEEK_SET)
        stdout_text = stdout.read()
        stdout.close()
        stderr.seek(0, os.SEEK_SET)
        stderr_text = stderr.read()
        stderr.close()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        #logger.info("Now putting job data into que: %s - %s" % (job, os.getpid()))
        self.rc.que.put(
                (
                    self.slave_id,
                    was_ok,  # failed?
                    job.job_id,  # id...
                    stdout_text,  # output
                    stderr_text,  # output
                    exception,
                    trace,
                    new_jobs,
                ))
        if not is_local:
            self.rc.que.close()
            self.rc.que.join_thread()

    def prepare_jobs_for_transfer(self, job_dict):
        """When traveling back, jobs-dependencies are wrapped as strings - this should
        prevent nasty suprises"""
         # package as strings
        for job in job_dict.values():
            job.prerequisites = [preq.job_id for preq in job.prerequisites]
            job.dependants = [dep.job_id for dep in job.dependants]
         # unpackanging is don in new_jobs_generated_during_runtime
        self.rc.pipegraph.new_jobs_generated_during_runtime(job_dict)
        return pickle.dumps({})  # The LocalSlave does not need to serialize back the jobs, it already is running in the space of the MCP

    def check_for_dead_jobs(self):
        remove = []
        for proc in self.process_to_job:
            if not proc.is_alive():
                logger.info("Process ended %s" % proc)
                remove.append(proc)
                if proc.exitcode != 0:  # 0 means everything ok, we should have an answer via the que from the job itself...
                    job = self.process_to_job[proc]
                    job.stdout_handle.flush()
                    job.stderr_handle.flush()
                    job.stdout_handle.seek(0, os.SEEK_SET)
                    job.stderr_handle.seek(0, os.SEEK_SET)
                    stdout = job.stdout_handle.read()
                    stderr = job.stderr_handle.read()
                    job.stdout_handle.close()
                    job.stderr_handle.close()
                    job.stdout_handle = None
                    job.stderr_handle = None
                    self.rc.que.put((
                            self.slave_id,
                            False,
                            job.job_id,
                            stdout,
                            stderr,
                            pickle.dumps(ppg_exceptions.JobDiedException(proc.exitcode)),
                            '',
                            False  # no new jobs
                            ))
        for proc in remove:
            del self.process_to_job[proc]


if twisted_available:
    class LocalTwisted:
        """A ResourceCoordinator that uses the current machine,
        up to max_cores_to_use cores of it

        It uses Twisted and one LocalTwistedSlave
        """

        def __init__(self, max_cores_to_use=8):
            self.max_cores_to_use = max_cores_to_use  # todo: update to local cpu count...
            self.slaves = {}
            self.cores_available = max_cores_to_use
            self.memory_available = 50 * 1024 * 1024 * 1024  # 50 gigs ;), todo, update to actual memory + swap... (see LocalSystem)
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

        def enter_twisted(self, pipe):
             # this happens in a spawned process...
            reactor.run()
            print(('exception in spawned', repr(self.pipegraph.jobs['out/a'].exception)))
            from . import cloudpickle
            pipe.send((cloudpickle.dumps(self.pipegraph.jobs), pickle.dumps(self.pipegraph.invariant_status)))

        def enter_loop(self):
            self.slaves_ready_count = 0
            logger.info("starting reactor")
            if not util.reactor_was_started:
                util.reactor_was_started = True
            parent_conn, child_conn = multiprocessing.Pipe()
            p = multiprocessing.Process(None, self.enter_twisted, args=(child_conn, ))
            p.start()
            jobs_str, invariant_str = parent_conn.recv()
            p.join()
            pickle.loads(jobs_str)  # which should automatically fill in the existing jobs...
            self.pipegraph.invariant_status = pickle.loads(invariant_str)
            #p.start()
            #p.join()
            #if p.exitcode != 0:
                #raise ppg_exceptions.RuntimeError()
            #reactor.run()
             # now, the problem here is that you simply can't restart twisted reactors
             # in ordinary coding, this is hardly a problem, though it is a hazzle to being
             # able to start another pipeline.
             # it does get fairly troublesome in testing.
             # the idea of course would be to fork before calling reactor.run,
             # and not returning until the forked child returns.
             # problem with this is of course how to propegate the exceptions
             # let' multiprocessing do the worring...

        def start_when_ready(self, response, slave_id):  # this get's called when a slave has connected and transmitted the pipegraph...
            status = response['ok']
            logger.info("start_when_ready for %s, status was %s" % (slave_id, status))
            if status:
                self.slaves_ready_count += 1
                if self.slaves_ready_count == len(self.slaves):
                    logger.info("Now calling start_jobs")
                    self.pipegraph.start_jobs()
            else:
                reactor.stop()
                raise ppg_exceptions.CommunicationFailure("Slave %s could not load jobgraph. Exception: %s" % (slave_id, response['exception']))

        def end_when_slaves_down(self, slave_id):
            self.slaves_ready_count -= 1
            if self.slaves_ready_count == 0:
                reactor.stop()

        def slave_connection_failed(self, slave_id):
            logger.info("Slave %s connection failed" % slave_id)
            reactor.callLater(0, lambda: reactor.stop())

        def job_ended(self, slave_id, was_ok, job_id_done, stdout, stderr, exception, trace, new_jobs):
            logger.info("Job returned: %s, was_ok: %s, new_jobs %s" % (job_id_done, was_ok, new_jobs))
            job = self.pipegraph.jobs[job_id_done]
            job.was_done_on.add(slave_id)
            job.stdout = stdout
            job.stderr = stderr
            logger.info('stdout %s' % stdout)
            logger.info('stderr %s' % stderr)
            job.exception = exception
            job.trace = trace
            job.failed = not was_ok
            if job.failed:
                try:
                    logger.info("Before depickle %s" % type(exception))
                    job.exception = pickle.loads(exception)
                    logger.info("After depickle %s" % type(job.exception))
                    logger.info("exception stored at %s" % (job))
                except (pickle.UnpicklingError, EOFError):  # some exceptions can't be pickled, so we send a string instead
                    pass
                if job.exception:
                    logger.info("Exception: %s" % repr(exception))
                    logger.info("Trace: %s" % trace)
                logger.info("stdout: %s" % stdout)
                logger.info("stderr: %s" % stderr)
            if new_jobs is not False:
                if not job.modifies_jobgraph():
                    job.exception = ppg_exceptions.JobContractError("%s created jobs, but was not a job with modifies_jobgraph() returning True" % job)
                    job.failed = True
                else:
                    logger.info("now unpickling new jbos")
                    new_jobs = pickle.loads(new_jobs)
                    logger.info("We retrieved %i new jobs from %s" % (len(new_jobs), job))
                    if new_jobs:  # the local system sends back and empty list, because it has called new_jobs_generated_during_runtime itself (without serializing)
                        self.pipegraph.new_jobs_generated_during_runtime(new_jobs)

            more_jobs = self.pipegraph.job_executed(job)
            if job.cores_needed == -1:
                self.cores_available = self.max_cores_to_use
            else:
                self.cores_available += job.cores_needed
            if not more_jobs:  # this means that all jobs are done and there are no longer any more running, so we can return  now...
                for slave_id in self.slaves:
                    self.slaves[slave_id].shut_down().addCallbacks(lambda res: self.end_when_slaves_down(slave_id),
                            lambda failure: self.end_when_slaves_down(slave_id))
            else:
                self.pipegraph.start_jobs()


    class AMP_Return_Protocol(amp.AMP):
        def __init__(self, slave):
            self.slave = slave

        def job_ended(self, arg_tuple_pickle):
            self.slave.job_returned(arg_tuple_pickle)
            return {'ok': True}
        messages.JobEnded.responder(job_ended)

        def __call__(self):
            return self


    class LocalTwistedSlaveProcess(ProcessProtocol):

        def __init__(self, connectionMade_callback, error_callback):
            self.connectionMade_callback = connectionMade_callback
            self.error_callback = error_callback
            self.called_back = False
            self.ended = False

        def connectionMade(self):
            logger.info("LocalTwistedSlaveProcess started in pid %s" % self.transport.pid)
            self.transport.closeStdin()
            #self.slave.connected()

        def outReceived(self, data):
            print(( 'received stdout from slave', data))
            if not self.called_back:
                self.called_back = True
                self.connectionMade_callback()
            print(data)

        def errReceived(self, data):
            print(('received sttderr from slave', data))

        def processExited(self, status):
            logger.info("Slave process ended, exit code %s" % status.value.exitCode)
            exit_code = status.value.exitCode
            if exit_code != 0:
                self.error_callback()
            self.ended = True

        def processEnded(self, status):
            exit_code = status.value.exitCode
            if exit_code != 0:
                raise ppg_exceptions.CommunicationFailure('Could not connect to slave, check logging to see which one')

        def reactor_is_shutting_down(self):
            if not self.ended:
                self.transport.loseConnection()

            return None


    class LocalTwistedSlave:

        def __init__(self, rc):
            self.rc = rc
            self.slave_id = 'LocalTwistedSlave'
            self.start_subprocess()

        def start_subprocess(self):
            self.magic_key = "%s" % time.time()
            cmd = ['python', os.path.abspath(os.path.join(os.path.dirname(__file__), 'util', 'twisted_slave.py')), self.magic_key]

            def do_connect():
                logger.info("Connecting to 500001")
                ClientCreator(reactor, AMP_Return_Protocol(self)).connectTCP('127.0.0.1', 50001).addCallbacks(
                        self.connected,
                        self.rc.slave_connection_failed
                        )

            try:
                protocol = LocalTwistedSlaveProcess(do_connect, lambda: self.rc.slave_connection_failed(self.slave_id))
                self.process = reactor.spawnProcess(protocol, cmd[0],
                        args=cmd, env=os.environ)
            except:
                reactor.stop()
                raise
            reactor.addSystemEventTrigger('before', 'shutdown', protocol.reactor_is_shutting_down)
            #self.process = subprocess.Popen(cmd, cwd=os.getcwd())  # , stdout=subprocess.PIPE)
            #time.sleep(1)
            #port = self.process.stdout.read()
            #logger.info("Connecting to %s" % port)
            pass

        def terminate_process(self, org):
            logger.info("Shutting down process of %s" % self.slave_id)
            self.process.kill()

        def connected(self, proto):
            logger.info("Transport established")
            self.amp = proto
            self.amp.callRemote(messages.MagicKey).addCallbacks(
                    self.magic_key_check,
                    self.magic_key_command_failed)

        def magic_key_command_failed(self, failure):
            logger.info("Magic key command failed: %s" % failure)
            self.amp.callRemote(messages.ShutDown).addCallbacks(
                    self.rc.slave_connection_failed,
                    self.rc.slave_connection_failed,
                    )

        def magic_key_check(self, result):
            logger.info("Received magic key: %s" % result)
            if not 'key' in result:
                self.transmit_pipegraph_failed()
            if result['key'] != self.magic_key:
                logger.info("wrong magic key, sending shutdown")
                self.amp.callRemote(messages.ShutDown).addCallbacks(
                        self.transmit_pipegraph_failed,
                        self.transmit_pipegraph_failed)
                self.transmit_pipegraph_failed()
            else:
                self.transmit_pipegraph(self.rc.pipegraph).addCallbacks(
                    lambda result: self.rc.start_when_ready(result, self.slave_id),
                    self.transmit_pipegraph_failed)

        def transmit_pipegraph_failed(self, failure):
            self.rc.start_when_ready({'ok': False, 'exception': 'Unknown'}, self.slave_id)

        def transmit_pipegraph(self, pipegraph):
            data = cloudpickle.dumps(pipegraph.jobs, 2)  # must be at least version two for the correct new-style class pickling
             # try:  #TODO: remove this unnecessary check
                #cPickle.loads(data)
            #except:
                #print 'error in reloading pipegraph'
                #reactor.stop()
                #raise
            return self.amp.callRemote(messages.TransmitPipegraph, jobs=data)

        def spawn(self, job):
            logger.info("Slave: Spawning %s" % job.job_id)
            self.amp.callRemote(messages.StartJob, job_id=job.job_id).addErrback(
                    self.job_failed_to_start(job.job_id))

        def job_returned(self, encoded_argument):
            logger.info("job_returned")
            args = pickle.loads(encoded_argument)
            self.rc.job_ended(self.slave_id, *args)

        def job_failed_to_start(self, job_id):
            def inner(failure, job_id=job_id):
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

        def shut_down(self):
            logger.info("Sending shut_down to %s" % self.slave_id)
            return self.amp.callRemote(messages.ShutDown).addCallbacks(
                    lambda result: self.terminate_process(result),
                    lambda failure: self.terminate_process(failure))
