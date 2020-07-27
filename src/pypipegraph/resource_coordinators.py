from __future__ import print_function

"""
The MIT License (MIT)

Copyright (c) 2017, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

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

import os
import traceback
import multiprocessing
import threading
import signal
import sys
import collections
import tempfile
import queue
import pickle

from . import ppg_exceptions
from . import util


class DummyResourceCoordinator:
    """For the calculating workers. so it throws exceptions..."""


def get_memory_available():
    if hasattr(os, "sysconf"):
        if "SC_NPROCESSORS_ONLN" in os.sysconf_names:  # a linux or unix system
            op = open("/proc/meminfo", "r")
            d = op.read()
            op.close()
            mem_total = d[d.find("MemTotal:") + len("MemTotal:") :]
            mem_total = mem_total[: mem_total.find("kB")].strip()
            swap_total = d[d.find("SwapTotal:") + len("SwapTotal:") :]
            swap_total = swap_total[: swap_total.find("kB")].strip()
            physical_memory = int(mem_total) * 1024
            swap_memory = int(swap_total) * 1024
            return physical_memory, swap_memory
        else:  # pragma: no cover
            # assume it's mac os x
            physical_memory = int(os.popen2("sysctl -n hw.memsize")[1].read())
            swap_memory = (
                physical_memory * 10
            )  # mac os x virtual memory system uses *all* available boot device size, so a heuristic should work well enough
            return physical_memory, swap_memory
    else:  # pragma: no cover
        raise ValueError(
            "get_memory_available() does not know how to get available memory on your system."
        )


def signal_handler(signal, frame):  # pragma: no cover - interactive
    print('Ctrl-C has been disable. Please give command "abort"')


JobReturnValue = collections.namedtuple(
    "JobReturnValue",
    (
        "worker_id",
        "was_ok",
        "job_id",
        "stdout",
        "stderr",
        "exception",
        "trace",
        "new_jobs",
        "runtime",
    ),
)


class LocalSystem:
    """A ResourceCoordinator that uses the current machine,
    up to max_cores_to_use cores of it

    It uses multiprocessing and the LocalWorker
    """

    def __init__(self, max_cores_to_use=util.CPUs(), profile=False, interactive=True):
        self.max_cores_to_use = max_cores_to_use  # todo: update to local cpu count...
        self.worker = LocalWorker(self)
        self.cores_available = max_cores_to_use
        self.physical_memory, self.swap_memory = get_memory_available()
        self.timeout = 5
        self.profile = profile
        if (multiprocessing.current_process().name != "MainProcess") or (
            util._running_inside_test
        ):
            interactive = False
        self.interactive = interactive

    def spawn_workers(self):
        return {"LocalWorker": self.worker}

    def get_resources(self):
        res = {
            "LocalWorker": {  # this is always the maximum available - the graph is handling the bookeeping of running jobs
                "cores": self.cores_available,
                "physical_memory": self.physical_memory,
                "swap_memory": self.swap_memory,
            }
        }
        return res

    def enter_loop(self):
        os.environ['RAYON_NUM_THREADS'] = "%i" % (self.max_cores_to_use,)
        self.spawn_workers()
        if sys.version_info[0] == 2 and sys.version_info[1] < 7:  # pragma: no cover
            raise ValueError("pypipegraph needs python >=2.7")
        else:
            self.que = multiprocessing.Queue()

        self.pipegraph.logger.debug("Entering execution loop")
        self.pipegraph.start_jobs()
        if self.interactive:  # pragma: no cover
            from . import interactive

            interactive_thread = threading.Thread(target=interactive.thread_loop)
            interactive_thread.start()
            s = signal.signal(signal.SIGINT, signal_handler)  # ignore ctrl-c
        while True:
            self.worker.check_for_dead_jobs()  # whether time out or or job was done, let's check this...
            if self.interactive:  # pragma: no cover
                self.see_if_output_is_requested()
            try:
                start = time.time()
                r = self.que.get(block=True, timeout=self.timeout)
                stop = time.time()
                self.pipegraph.logger.info("Till que.got: %.2f" % (stop - start))

                if r is None and interactive.interpreter.terminated:  # pragma: no cover
                    # abort was requested
                    self.worker.kill_jobs()
                    break
                # worker_id, was_ok, job_id_done, stdout, stderr, exception, trace, new_jobs, runtime = (
                # r
                # )  # was there a job done?t
                self.pipegraph.logger.debug(
                    "Job returned: %s, was_ok: %s" % (r.job_id, r.was_ok)
                )
                job = self.pipegraph.jobs[r.job_id]
                job.stop_time = time.time()
                job.was_done_on.add(r.worker_id)
                job.stdout = r.stdout
                job.stderr = r.stderr
                job.exception = r.exception
                job.trace = r.trace
                job.failed = not r.was_ok
                if job.start_time:
                    delta = job.stop_time - job.start_time
                    if delta > 5:
                        self.pipegraph.logger.warning(
                            "%s runtime: %.2fs (%.2fs w/oque)"
                            % (r.job_id, delta, r.runtime)
                        )
                    job.runtime = delta
                else:
                    job.runtime = -1
                if job.failed:
                    try:
                        if job.exception.startswith("STR".encode("UTF-8")):
                            job.exception = job.exception[3:]
                            raise pickle.UnpicklingError(
                                "String Transmission"
                            )  # what an ugly control flow...
                        job.exception = pickle.loads(r.exception)
                    except (
                        pickle.UnpicklingError,
                        EOFError,
                        TypeError,
                        AttributeError,
                    ):  # some exceptions can't be pickled, so we send a string instead
                        pass
                    if job.exception:
                        self.pipegraph.logger.warning(
                            "Job returned with exception: %s" % job
                        )
                        self.pipegraph.logger.warning(
                            "Exception: %s" % repr(r.exception)
                        )
                        self.pipegraph.logger.warning("Trace: %s" % r.trace)
                if r.new_jobs is not False:
                    if not job.modifies_jobgraph():  # pragma: no cover
                        job.exception = ValueError("This branch should not be reached.")
                        job.failed = True
                    else:
                        new_jobs = pickle.loads(r.new_jobs)
                        self.pipegraph.logger.debug(
                            "We retrieved %i new jobs from %s" % (len(new_jobs), job)
                        )
                        self.pipegraph.new_jobs_generated_during_runtime(new_jobs)

                more_jobs = self.pipegraph.job_executed(job)
                if (
                    not more_jobs
                ):  # this means that all jobs are done and there are no longer any more running...
                    break
                self.pipegraph.start_jobs()

            except (queue.Empty, IOError):  # either timeout, or the que failed
                pass
        self.que.close()
        self.que.join_thread()  # wait for the que to close
        if self.interactive:  # pragma: no cover - interactive
            if not interactive.interpreter.stay:
                interactive.interpreter.terminated = True
            interactive_thread.join()
            signal.signal(signal.SIGINT, s)
        self.pipegraph.logger.debug("Leaving loop")

    def see_if_output_is_requested(self):  # pragma: no cover - interactive
        import select

        try:
            if select.select([sys.stdin], [], [], 0)[0]:
                sys.stdin.read(1)  # enter pressed...
                self.pipegraph.print_running_jobs()
                pass
        finally:
            pass
            # termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    def abort(self):  # pragma: no cover - interactive
        self.que.put(None)

    def kill_job(self, job):  # pragma: no cover - interactive
        self.worker.kill_job(job)

    def get_job_pid(self, job):  # pragma: no cover - interactive
        return self.worker.get_job_pid(job)


class LocalWorker:
    def __init__(self, rc):
        self.rc = rc
        self.worker_id = "LocalWorker"
        self.process_to_job = {}

    def spawn(self, job):
        self.rc.pipegraph.logger.debug("spawning %s", job)
        job.start_time = time.time()
        preq_failed = False
        if not job.is_final_job:  # final jobs don't load their (fake) prereqs.
            for preq in job.prerequisites:
                if preq.is_loadable():
                    self.rc.pipegraph.logger.debug("Now loading %s", preq)
                    preq.start_time = time.time()
                    if not self.load_job(preq):
                        preq_failed = True
                        break
                    preq.stop_time = time.time()
                    delta = preq.stop_time - preq.start_time
                    self.rc.pipegraph.logger.debug("load time %s %.2f", preq, delta)
                    if delta > 5:
                        self.rc.pipegraph.logger.warning(
                            "%s load runtime: %.2fs" % (preq.job_id, delta)
                        )
        if preq_failed:
            self.rc.que.put(
                JobReturnValue(
                    worker_id=self.worker_id,
                    was_ok=False,
                    job_id=job.job_id,
                    stdout="",
                    stderr="",
                    exception="STRPrerequsite failed".encode("UTF-8"),
                    trace="",
                    new_jobs=False,
                    runtime=-1,
                )
            )
            # time.sleep(0)
        else:
            if job.modifies_jobgraph():
                stdout = tempfile.SpooledTemporaryFile(mode="w+")
                stderr = tempfile.SpooledTemporaryFile(mode="w+")
                self.run_a_job(job, stdout, stderr)
            else:
                stdout = tempfile.TemporaryFile(
                    mode="w+"
                )  # no more spooling - it doesn't get passed back
                stderr = tempfile.TemporaryFile(mode="w+")
                stdout.fileno()
                stderr.fileno()
                p = multiprocessing.Process(
                    target=self.wrap_run, args=[job, stdout, stderr, False]
                )
                job.stdout_handle = stdout
                job.stderr_handle = stderr
                p.start()
                job.run_info = "pid = %s" % (p.pid,)
                job.pid = p.pid

                self.process_to_job[p] = job

    def load_job(
        self, job
    ):  # this executes a load job returns false if an error occured
        stdout = tempfile.SpooledTemporaryFile(mode="w")
        stderr = tempfile.SpooledTemporaryFile(mode="w")

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ""
        new_jobs = False
        start = time.time()
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
                exception = str(e)
        stop = time.time()
        stdout.seek(0, os.SEEK_SET)
        stdout_text = stdout.read()[-10 * 1024 :]
        stdout.close()
        stderr.seek(0, os.SEEK_SET)
        stderr_text = stderr.read()[-10 * 1024 :]
        stderr.close()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        if not was_ok:
            self.rc.que.put(
                JobReturnValue(
                    worker_id=self.worker_id,
                    was_ok=was_ok,
                    job_id=job.job_id,
                    stdout=stdout_text,
                    stderr=stderr_text,
                    exception=exception,
                    trace=trace,
                    new_jobs=new_jobs,
                    runtime=stop - start,
                )
            )
        os.chdir(self.rc.pipegraph.chdir)
        return was_ok

    def wrap_run(self, job, stdout, stderr, is_local):
        if self.rc.interactive:  # pragma: no cover
            signal.signal(signal.SIGINT, signal.SIG_IGN)  # ignore ctrl-c

        self.run_a_job(job, stdout, stderr, is_local)

    def run_a_job(
        self, job, stdout, stderr, is_local=True
    ):  # this runs in the spawned processes, except for job.modifies_jobgraph()==True jobs

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
        trace = ""
        new_jobs = False
        util.global_pipegraph.new_jobs = None  # ignore jobs created here.
        start = time.time()
        try:
            temp = job.run()
            was_ok = True
            exception = None
            if job.modifies_jobgraph():
                new_jobs = self.prepare_jobs_for_transfer(temp)
            elif temp:
                raise ppg_exceptions.JobContractError(
                    "Job returned a value (which should be new jobs generated here) without having modifies_jobgraph() returning True"
                )
        except Exception as e:
            print("exception in ", job.job_id)
            trace = traceback.format_exc()
            was_ok = False
            exception = e
            try:
                exception = pickle.dumps(e)
            except Exception as e:  # some exceptions can't be pickled, so we send a string instead
                try:
                    exception = bytes("STR", "UTF-8") + bytes(e)
                except TypeError:
                    exception = str(e)
        finally:
            stop = time.time()
        try:
            stdout.seek(0, os.SEEK_SET)
            stdout_text = stdout.read()
            stdout.close()
        except ValueError as e:  # pragma: no cover - defensive
            if "I/O operation on closed file" in str(e):
                stdout_text = (
                    "Stdout could not be captured / io operation on closed file"
                )
            else:
                raise
        try:
            stderr.seek(0, os.SEEK_SET)
            stderr_text = stderr.read()
            stderr.close()
        except ValueError as e:  # pragma: no cover - defensive
            if "I/O operation on closed file" in str(e):
                stderr_text = (
                    "stderr could not be captured / io operation on closed file"
                )
            else:
                raise
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        stop = time.time()
        self.rc.que.put(
            JobReturnValue(
                worker_id=self.worker_id,
                was_ok=was_ok,
                job_id=job.job_id,
                stdout=stdout_text,
                stderr=stderr_text,
                exception=exception,
                trace=trace,
                new_jobs=new_jobs,
                runtime=stop - start,
            )
        )
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
        return pickle.dumps(
            {}
        )  # The LocalWorker does not need to serialize back the jobs, it already is running in the space of the MCP

    def check_for_dead_jobs(self):
        remove = []
        for proc in self.process_to_job:
            if not proc.is_alive():
                remove.append(proc)
                if (
                    proc.exitcode != 0
                ):  # 0 means everything ok, we should have an answer via the que from the job itself...
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
                    self.rc.que.put(
                        JobReturnValue(
                            worker_id=self.worker_id,
                            was_ok=False,
                            job_id=job.job_id,
                            stdout=stdout,
                            stderr=stderr,
                            exception=pickle.dumps(
                                ppg_exceptions.JobDiedException(proc.exitcode)
                            ),
                            trace="",
                            new_jobs=False,  # no new jobs
                            runtime=-1,
                        )
                    )
        for proc in remove:
            del self.process_to_job[proc]

    def kill_job(self, target_job):  # pragma: no cover (needed by interactive)
        for process, job in self.process_to_job.items():
            if job == target_job:
                print("Found target job")
                self.rc.pipegraph.logger.info("Killing job on user request: %s", job)
                process.terminate()

    def kill_jobs(self):  # pragma: no cover (needed by interactive)
        print("Killing %i running children" % len(self.process_to_job))
        for proc in self.process_to_job:
            proc.terminate()

    def get_job_pid(self, target_job):  # pragma: no cover (needed by interactive)
        print(target_job)
        print(target_job.run_info)
        print(target_job.pid)
        return target_job.pid
