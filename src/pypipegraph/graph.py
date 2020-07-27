from __future__ import print_function

"""
A pipegraph models jobs depending on other jobs in a directed acyclic graph
- basically a branching pipeline of things to be done. It keeps track of what's
been already done and of what's changed. It automatically executes jobs in
parallel if possible.
"""

license = """


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
import os
import sys
import signal
import time
import collections
import pickle
import logging
from io import StringIO
from . import resource_coordinators
from . import ppg_exceptions
from . import util


# earlier on, we had a different pickling scheme,
# and that's what the files were called.
if os.path.exists(".pypipegraph_status_robust"):  # old projects keep their filename
    invariant_status_filename_default = ".pypipegraph_status_robust"  # pragma: no cover
elif "/" in sys.argv[0]:  # no script name but an executable?
    invariant_status_filename_default = ".pypipegraph_status_robust"
else:
    # script specific pipegraphs
    invariant_status_filename_default = (
        ".ppg_status_%s" % sys.argv[0]
    )  # pragma: no cover


def run_pipegraph(*args, **kwargs):
    """Run the current global pipegraph"""
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    util.global_pipegraph.run(*args, **kwargs)


def new_pipegraph(
    resource_coordinator=None,
    quiet=False,
    invariant_status_filename=invariant_status_filename_default,
    dump_graph=True,
    interactive=True,
    cache_folder="cache",
    log_file=None,
    log_level=logging.INFO,
):
    """Create a new global pipegraph.
    New jobs will automagically be attached to this pipegraph.
    Default ResourceCoordinator is L{LocalSystem}
    """
    if resource_coordinator is None:
        resource_coordinator = resource_coordinators.LocalSystem(
            interactive=interactive
        )

    util.global_pipegraph = Pipegraph(
        resource_coordinator,
        quiet=quiet,
        invariant_status_filename=invariant_status_filename,
        dump_graph=dump_graph,
        cache_folder=cache_folder,
    )
    if log_file is not None:
        util.global_pipegraph.logger.setLevel(log_level)
        util.global_pipegraph.logger.addHandler(logging.FileHandler(log_file, mode="w"))


class Pipegraph(object):
    """A pipegraph collects Jobs and runs them in a valid order on request (pipegraph.run).

    Do not instanciate directly, use new_pipegraph(...) instead

    It dumps some logging if the folder './logs' exists.

    If any of the jobs failse, run() will print their output and raise a RuntimeException.
    It will chatter about failed jobs on stderr if you don't set graph.quiet = True.

    Depending on the L{ResourceCoordinator} used, pressing enter (and waiting a few seconds)
    will list the current jobs.
    (Default ResourceCoordinator does so).
    Abort run with ctrl-c.
    """

    def __init__(
        self,
        resource_coordinator,
        quiet=False,
        invariant_status_filename=invariant_status_filename_default,
        dump_graph=True,
        cache_folder="cache",
    ):
        self.logger = logging.getLogger("pypipegraph")
        self.logger.debug("New Pipegraph")
        self.rc = resource_coordinator
        self.rc.pipegraph = self
        self.jobs = {}
        self.running = False
        self.was_run = False
        self.new_jobs = False
        self.quiet = quiet
        # used by util.assert_uniqueness_of_object to enforce pseudo-singletons
        self.object_uniquifier = {}
        self.stat_cache = {}  # used by util.stat
        self.invariant_loading_issues = (
            {}
        )  # jobs whose invariant could not be unpickled for some reason - and the exception.
        self._distribute_invariant_changes_count = 0
        self.invariant_status_filename = invariant_status_filename
        self.do_dump_graph = dump_graph
        self.restart_afterwards = False
        self.cache_folder = cache_folder
        self.job_uniquifier = {}  # to singletonize jobs on job_id
        self.filename_collider_check = (
            {}
        )  # to check wether theres' an overlap in filenames between FileGeneratingJob and MultiFileGeneratingJob
        self.func_hashes = (
            {}
        )  # to calculate invarionts on functions in a slightly more efficent manner
        self.file_hashes = {}
        self.job_id_cache = {}  # used by verify_job_id

    def __del__(self):
        # remove circle link between rc and pipegraph
        self.rc.pipegraph = None

    def add_job(self, job):
        """Add a job to a Pipegraph.
        Usually automagically called when instanciating one of the Job classes
        """
        if job.job_id in self.jobs and self.jobs[job.job_id] is job:
            return
        if not self.running:
            if self.was_run:
                raise ValueError(
                    "This pipegraph was already run. You need to create a new one for more jobs"
                )
            self.jobs[job.job_id] = job
        else:
            if self.new_jobs is False:
                raise ppg_exceptions.JobContractError(
                    "Trying to add new jobs to running pipeline without having new_jobs set (ie. outside of a graph modifying job) - tried to add %s"
                    % job
                )
            elif self.new_jobs is None:
                self.logger.debug(
                    "Ignored: Trying to add new jobs to running pipeline without having new_jobs set (ie. outside of a graph modifying job) - tried to add %s"
                    % job
                )
                return
            if job.job_id not in self.jobs:
                self.new_jobs[job.job_id] = job
            else:
                pass

    def run(self, dry_run=False):
        """Run the Pipegraph. Gives control to the graph until all jobs have run (or failed).
        May fail right away if a JobContractError occurs - for example if you've built a cycle
        of dependencies.

        If any job does not complete (exception, segfault, failed to produce output), RuntimeError
        will be thrown, and the job's exception (if available) be stored in job.exception

        if @dry_run is set, the Graph will throw an exception
        on the first invariant change it detects

        """
        if self.was_run:
            raise ValueError("Each pipegraph may be run only once.")
        self.logger.debug("MCP pid: %i" % os.getpid())
        self.logger.debug("Preparing pypipegraph")
        self.dry_run = dry_run

        # internal to the mcp
        self.fill_dependency_callbacks()
        self.inject_auto_invariants()
        self.was_run = True  # since build_

        self.running = True
        self.new_jobs = (
            False
        )  # this get's changed in graph modifying jobs, but they reset it to false, which means 'don't accept any new jobs while we are running'
        self.chdir = os.path.abspath((os.getcwd()))
        self.connect_graph()
        self.check_cycles()
        self.prune()
        self.load_invariant_status()
        self.distribute_invariant_changes()
        self.dump_invariant_status()  # the jobs will have removed their output, so we can safely store the invariant data
        self.build_todo_list()
        if self.do_dump_graph:
            self.dump_graph()

        # make up some computational engines and put them to work.
        self.logger.debug("now executing")
        self.install_signals()
        try:
            self.spawn_workers()
            self.execute_jobs()

        finally:
            # clean up
            self.logger.debug("starting cleanup")
            self.restore_signals()
            self.dump_invariant_status()
            self.destroy_job_connections()
            self.logger.debug("sucessfull cleanup")
            os.chdir(self.chdir)

        # and propagate if there was an exception
        try:
            any_failed = False
            try:
                str_error_log = StringIO()
                try:
                    if os.path.exists("logs/ppg_errors.txt.1"):
                        os.unlink("logs/ppg_errors.txt.1")
                    if os.path.exists("logs/ppg_errors.txt"):
                        os.rename("logs/ppg_errors.txt", "logs/ppg_errors.txt.1")
                    error_log = open("logs/ppg_errors.txt", "w")
                except IOError:  # pragma: no cover
                    error_log = open("/dev/null", "w")  # pragma: no cover
                exceptions = []
                for job in self.jobs.values():
                    if job.failed or job.job_id in self.invariant_loading_issues:
                        if not any_failed and not self.quiet:
                            print("\n------Pipegraph error-----", file=sys.stderr)
                            print("\n------Pipegraph error-----", file=error_log)
                        any_failed = True
                        if job.error_reason != "Indirect" and not self.quiet:
                            self.print_failed_job(job, sys.stderr)
                            self.print_failed_job(job, error_log)
                            self.print_failed_job(job, str_error_log)
                        if job.exception is not None:
                            exceptions.append(job.exception)
                        if job.job_id in self.invariant_loading_issues:
                            print(
                                "Could not unpickle invariant for %s - exception was %s"
                                % (job, self.invariant_loading_issues[job.job_id]),
                                file=error_log,
                            )
                            print(
                                "Could not unpickle invariant for %s - exception was %s"
                                % (job, self.invariant_loading_issues[job.job_id]),
                                file=sys.stderr,
                            )
                if any_failed:
                    print("exceptions", exceptions)
                    if not self.quiet:
                        print("\n------Pipegraph output end-----", file=sys.stderr)
                        print("\n------Pipegraph output end-----", file=error_log)
                        if error_log.name != "/dev/null":
                            print(
                                "\n failed job log in logs/ppg_erros", file=sys.stderr
                            )
                    if not self.restart_afterwards:
                        raise ppg_exceptions.RuntimeError(
                            str_error_log.getvalue(), exceptions
                        )
                else:
                    self.cleanup_jobs_that_requested_it()
            finally:
                error_log.close()
        finally:
            if not self.quiet:
                print(
                    "Pipegraph done. Executed %i jobs. %i failed"
                    % (
                        len([x for x in list(self.jobs.values()) if x.was_run]),
                        len([x for x in list(self.jobs.values()) if x.failed]),
                    )
                )
            self.running = False
            self.was_run = True
            if self.restart_afterwards:  # pragma: no cover
                import subprocess

                subprocess.check_call([sys.executable] + sys.argv)
            self.signal_finished()

    def inject_auto_invariants(self):
        """Go through each job and ask it to create the invariants it might need.
        Why is this not done by the jobs on init? So the user can prevent them from doing it (job.do_ignore_code_changes()...)"""
        for job in list(self.jobs.values()):
            job.inject_auto_invariants()

    def fill_dependency_callbacks(self):
        with_callback = [j for j in self.jobs.values() if j.dependency_callbacks]
        if not with_callback:
            return
        for j in with_callback:
            for c in j.dependency_callbacks:
                j.depends_on(c())
            j.dependency_callbacks = []
        self.fill_dependency_callbacks()  # nested?

    def connect_graph(self):
        """Convert the dependency graph in jobs into a bidirectional graph"""
        # connect graph
        for job in self.jobs.values():
            if job.was_run:
                continue
            for preq in job.prerequisites:
                preq.dependants.add(job)

        # inject final jobs to run after all others...
        ii = 0
        for job in self.jobs.values():
            if job.is_final_job:
                job.job_no = len(self.jobs) + ii + 123
                ii += 1
                for jobB in self.jobs.values():
                    non_final_dependands = [
                        j for j in jobB.dependants if not j.is_final_job
                    ]
                    if not jobB.is_final_job and not non_final_dependands:
                        job.prerequisites.add(jobB)
                        jobB.dependants.add(job)

    def destroy_job_connections(self):
        """Delete connections between jobs for gc purposes"""
        for job in self.jobs.values():
            job.dependants = None
            job.prerequisites = None

    def apply_topological_order(self, list_of_jobs, add_job_numbers=False):
        for ii, job in enumerate(self.jobs.values()):
            job.dependants_copy = job.dependants.copy()
            if add_job_numbers:
                job.job_no = ii + 123
        L = []
        S = [job for job in list_of_jobs if len(job.dependants_copy) == 0]
        S.sort(key=lambda job: job.prio if hasattr(job, "prio") else 0)
        while S:
            n = S.pop()
            L.append(n)
            for m in n.prerequisites:
                m.dependants_copy.remove(n)
                if not m.dependants_copy:
                    S.append(m)
        return L

    def check_cycles(self):
        """Check whether there are any loops in the graph which prevent execution.

        Basically imposes a topological ordering, and if that's impossible, we have a cycle.
        Also, this gives a valid, job by job order of executing them.
        """
        L = self.apply_topological_order(self.jobs.values(), add_job_numbers=True)
        has_edges = False
        for job in self.jobs.values():
            if job.dependants_copy:
                has_edges = True
                break
        for job in self.jobs.values():
            del job.dependants_copy
        if has_edges:  # ie. we have a circle
            import pprint

            jobs_in_circles = []
            max_depth = 50
            for job in self.jobs.values():
                if job.is_in_dependency_chain(
                    job, max_depth
                ):  # we have to terminate this, otherwise the endless loop will explode python
                    jobs_in_circles.append(job)

            def find_circle_path(current_node, search_node, path, depth=0):
                # if depth > max_depth: #never happens, max_depth cyclesl are
                # not in jobs_in_circles
                # return False
                if current_node == search_node and path:
                    return True
                for preq in current_node.prerequisites:
                    path.append(preq)
                    if find_circle_path(preq, search_node, path, depth + 1):
                        return True
                    else:
                        path.pop()
                return False

            job_path_tuples = []
            for job in jobs_in_circles:
                path = []
                find_circle_path(job, job, path)
                job_path_tuples.append((job, path))
            if job_path_tuples:
                raise ppg_exceptions.CycleError(
                    "At least one cycle in the graph was detected\nJobs involved (up to a depth of %i) are\n %s "
                    % (max_depth, pprint.pformat(job_path_tuples[0]))
                )
            else:  # ie all exceeded max_depth and were not added to the list
                raise ppg_exceptions.CycleError(
                    "At least one cycle in the graph was detected\n Too many jobs involved for detailed output (more than %i)"
                    % (max_depth,)
                )
        L.reverse()
        self.possible_execution_order = L

    def prune(self):
        from .job import FinalJob

        def prune_children(job, reason):
            for c in job.dependants:
                if not isinstance(c, FinalJob):
                    c._pruned = reason
                    prune_children(c, reason)

        for job in self.jobs.values():
            if job._pruned is True:
                job._pruned = (
                    job.job_id
                )  # we use two instead of true to prevent doing it multiple times for the same branch of the graph
                prune_children(job, job.job_id)
        self.possible_execution_order = [
            x for x in self.possible_execution_order if not isinstance(x._pruned, str)
        ]

    def load_invariant_status(self):
        """Load Job invariant status from disk (and self.invariant_status_filename)
        """
        if os.path.exists(self.invariant_status_filename):
            op = open(self.invariant_status_filename, "rb")
            # op.read()
            # op.seek(0, os.SEEK_SET)
            self.invariant_status = collections.defaultdict(bool)
            leave = False
            possible_pickle_exceptions = (TypeError, pickle.UnpicklingError)

            while not leave:
                try:
                    key = None
                    key = pickle.load(op)
                    old_pos = op.tell()
                    value = pickle.load(op)
                    self.invariant_status[key] = value
                except possible_pickle_exceptions as e:
                    if key is None:
                        raise ppg_exceptions.PyPipeGraphError(
                            "Could not depickle invariants - "
                            "Depickling key failed"
                            " Exception was %s" % (str(e),)
                        )
                    self.logger.error(
                        "Could not depickle invariant for %s - "
                        "check code for depickling bugs. "
                        "Job will rerun, probably until the (de)pickling bug is fixed."
                        "\n Exception: %s" % (key, e)
                    )
                    self.invariant_loading_issues[key] = e
                    op.seek(old_pos)
                    # use pickle tools to read the pickles op codes until
                    # the end of the current pickle, hopefully allowing decoding of the next one
                    # of course if the actual on disk file is messed up beyond this,
                    # we're done for.
                    import pickletools

                    try:
                        list(pickletools.genops(op))
                    except Exception as e:
                        raise ppg_exceptions.PyPipeGraphError(
                            "Could not depickle invariants - "
                            "depickling of %s failed, could not skip to next pickled dataset"
                            " Exception was %s" % (key, str(e))
                        )
                except EOFError:
                    leave = True
            op.close()
        else:
            self.invariant_status = collections.defaultdict(bool)
        self.logger.debug("loaded %i invariant stati" % len(self.invariant_status))

    def dump_invariant_status(self):
        """Store Job invariant status into a file named by self.invariant_status_filename"""
        finished = False
        ki_raised = False
        while not finished:
            try:
                op = open(
                    os.path.abspath(self.invariant_status_filename + ".temp"), "wb"
                )
                for key, value in self.invariant_status.items():
                    try:
                        pickle.dump(key, op, pickle.HIGHEST_PROTOCOL)
                        pickle.dump(value, op, pickle.HIGHEST_PROTOCOL)
                    except Exception as e:
                        print(key)
                        print(value)
                        raise e
                op.close()
                if os.path.exists(
                    self.invariant_status_filename + ".old"
                ):  # we use the .old copy for comparison purposes later on
                    os.unlink(self.invariant_status_filename + ".old")
                if os.path.exists(self.invariant_status_filename):
                    os.rename(
                        self.invariant_status_filename,
                        self.invariant_status_filename + ".old",
                    )
                os.rename(
                    self.invariant_status_filename + ".temp",
                    self.invariant_status_filename,
                )
                finished = True
            except KeyboardInterrupt:  # pragma: no cover
                # if the user interrupts here here risks blowing the whole
                # history, so we ignore him...
                ki_raised = True
                pass
        if ki_raised:  # pragma: no cover
            raise KeyboardInterrupt()

    def distribute_invariant_changes(self):
        """Check each job for whether it's invariance has changed,
        and propagate the invalidation by calling job.invalidated()"""
        for job in self.jobs.values():
            if (
                job.was_invalidated
            ):  # don't redo it just because we're calling this again after having received new jobs.
                if job.invalidation_count == self._distribute_invariant_changes_count:
                    continue
                else:
                    job.distribute_invalidation()
                    job.invalidation_count = self._distribute_invariant_changes_count
            old = self.invariant_status[job.job_id]
            try:
                try:
                    inv = job.get_invariant(old, self.invariant_status)
                except Exception as e:
                    if isinstance(e, ppg_exceptions.NothingChanged):
                        pass
                    else:
                        print("Offending job was %s" % job)  # pragma: no cover
                    raise
            except ppg_exceptions.NothingChanged as e:
                inv = e.new_value
                old = inv  # so no change...
                self.invariant_status[
                    job.job_id
                ] = inv  # so not to recheck next time...
            if inv != old:
                if self.dry_run:
                    print(
                        ValueError(
                            f"""Invariant change detected
Job: {job}
old: {old}
new: {inv}
"""
                        )
                    )
                else:
                    job.invalidated(reason="invariant")
                job.invalidation_count = self._distribute_invariant_changes_count
                self.invariant_status[
                    job.job_id
                ] = (
                    inv
                )  # for now, it is the dependant job's job to clean up so they get reinvalidated if the executing is terminated before they are reubild (ie. filegenjobs delete their outputfiles)
        if self.dry_run:
            raise ValueError("dry run end")
        self._distribute_invariant_changes_count += 1

    def build_todo_list(self):
        """Go through each job. If it needs to be done, invalidate() all dependants.
        also requires all prerequisites to require_loading
        """
        needs_to_be_run = set()
        for job in self.jobs.values():
            job.do_cache = True
        for job in self.jobs.values():
            if (
                not job.is_done() and not job._pruned
            ):  # is done can return True, False, and None ( = False, but even if is_temp_job, rerun dependants...)
                if not job.is_loadable():
                    needs_to_be_run.add(job.job_id)
                    if (
                        not job.always_runs
                    ):  # there is no need for the job injecting jobs to invalidate just because they need to be run.
                        if not job.is_temp_job or job.is_done() is None:
                            job.invalidated("not done")
                            if not job.was_invalidated:  # paranoia
                                raise ppg_exceptions.JobContractError(
                                    "job.invalidated called, but was_invalidated was false"
                                )
                # for preq in job.prerequisites:
                # preq.require_loading() #think I can get away with  letting the workers what they need to execute a given job...

        for job in self.jobs.values():
            if (
                job.was_invalidated  # this has been invalidated
                and job.runs_in_worker()  # it is not one of the invariantes
                and not job.is_loadable()  # and it is not a loading job (these the workers do automagically for now)
                and not (job.is_temp_job and job.is_done)
            ):
                needs_to_be_run.add(job.job_id)
            elif not job.runs_in_worker():
                job.was_run = True  # invarites get marked as ran..
            # job.do_cache = False
        # now prune the possible_execution_order
        self.possible_execution_order = [
            job
            for job in self.possible_execution_order
            if job.job_id in needs_to_be_run
        ]
        self.jobs_to_run_count = len(self.possible_execution_order)
        self.jobs_done_count = 0
        self.logger.debug(
            " Execution order %s"
            % "\n".join([str(x) for x in self.possible_execution_order])
        )

    def spawn_workers(self):
        """Tell the resource coordinator to get the workers ready"""
        self.workers = self.rc.spawn_workers()
        self.check_all_jobs_can_be_executed()

    def check_all_jobs_can_be_executed(self):
        """Check all jobs for memory/cpu requirements and prune those that we can't satisfy"""
        resources = (
            self.rc.get_resources()
        )  # a dict of worker name > {cores: y, memory: x}
        maximal_memory = max(
            [x["physical_memory"] + x["swap_memory"] for x in resources.values()]
        )
        maximal_cores = max([x["cores"] for x in resources.values()])
        if maximal_cores == 0:  # pragma: no cover
            raise ppg_exceptions.RuntimeException("No cores available?!")

        for job in self.possible_execution_order:
            if job.cores_needed > maximal_cores or job.memory_needed > maximal_memory:
                self.prune_job(job)
                job.error_reason = "Needed to much memory/cores"
                job.failed = True

    def execute_jobs(self):
        """Pass control to ResourceCoordinator"""
        self.jobs_by_worker = {}
        # the rc loop externalizes the start_jobs / job_executed, start more jobs
        self.running_jobs = set()
        if self.possible_execution_order:  # no jobs, no spawning...
            self.rc.pipegraph = self
            if not self.quiet:
                sys.stderr.write(
                    "Done %i of %i jobs (%i total including non-running)\r"
                    % (self.jobs_done_count, self.jobs_to_run_count, len(self.jobs))
                )

            self.rc.enter_loop()  # doesn't return until all jobs have been done.

    def cleanup_jobs_that_requested_it(self):
        """Temporary* generating jobs that don't get run (because their downstream is done) might still attempt to clean up their output. They register by job.setting do_cleanup_if_was_never_run = True"""
        for job in self.jobs.values():
            if job.do_cleanup_if_was_never_run and not job.was_run:
                job.cleanup()

    def start_jobs(self):  # noqa:C901
        """Instruct workers to start as many jobs as we can currently spawn under our memory/cpu restrictions"""
        # I really don't like this function... and I also have the strong inkling it should acttually sit in the resource coordinatora         # first, check what we actually have some resources...

        resources = (
            self.rc.get_resources()
        )  # a dict of worker name > {cores: y, memory: x}
        for worker in resources:
            resources[worker]["memory/core"] = (
                resources[worker]["physical_memory"] / resources[worker]["cores"]
            )
            resources[worker]["memory"] = resources[worker]["physical_memory"]
            resources[worker]["total cores"] = resources[worker]["cores"]
        # substract the running jobs
        for job in self.running_jobs:
            worker = job.worker_name
            if (
                job.cores_needed < -1 or job.modifies_jobgraph()
            ):  # since that job blocks the worker..
                resources[worker]["cores"] = 0
            else:
                resources[worker]["cores"] -= 1
            if job.memory_needed == -1:
                resources[worker]["memory"] -= resources[worker]["memory/core"]
            else:
                resources[worker][
                    "memory"
                ] -= job.memory_needed  # pragma: no cover - we don't really use this

        def resources_available(resources):
            for worker in resources:
                if resources[worker]["cores"] > 0 and resources[worker]["memory"] > 0:
                    return True
            return False

        error_count = len(self.jobs) * 2
        maximal_startable_jobs = sum(x["cores"] for x in resources.values())
        runnable_jobs = []
        for job in self.possible_execution_order:
            if job.can_run_now():
                runnable_jobs.append(job)
                if len(runnable_jobs) == maximal_startable_jobs:
                    break
        if (
            self.possible_execution_order
            and not runnable_jobs
            and not self.running_jobs
        ):  # pragma: no cover
            time.sleep(
                5
            )  # there might be an interaction with stat caching - filesize being reported as 0 after a job returned, when it really isn't. So we sleep a bit, anfd try again
            runnable_jobs = []
            for job in self.possible_execution_order:
                if job.can_run_now():
                    runnable_jobs.append(job)
                    if len(runnable_jobs) == maximal_startable_jobs:
                        break
        if (
            self.possible_execution_order
            and not runnable_jobs
            and not self.running_jobs
        ):  # pragma: no cover
            # ie. the sleeping did not help
            self.logger.error("Nothing running, nothing runnable, but work left to do")
            self.logger.error("job\tcan_run_now\tlist_blocks")
            for job in self.possible_execution_order:
                self.logger.error(
                    "Job: %s\tCan run now: %s\nBlocks: %s\n\n"
                    % (job, job.can_run_now(), job.list_blocks())
                )
            self.logger.exception("Job excution order error")
            raise ppg_exceptions.RuntimeException(
                """We had more jobs that needed to be done, none of them could be run right now and none were running. Sounds like a dependency graph bug to me.
            Or perhaps, you're actively removing a file that a job created earlier. This might happen if you add a fancy clean up function
            """
            )

        while resources_available(resources) and runnable_jobs:
            to_remove = []
            for worker in resources:
                if resources[worker]["cores"] > 0 and resources[worker]["memory"] > 0:
                    next_job = 0
                    # while next_job < len(self.possible_execution_order): #todo: restrict to runnable_jobs
                    while next_job < len(runnable_jobs):
                        job = runnable_jobs[next_job]
                        # if job.can_run_now():
                        # Todo: Keep track of which dataloadingjobs have already been performed on each node
                        # and prioritize by that...
                        if job.modifies_jobgraph():
                            to_remove.append(job)  # remove just once...
                            for worker in self.workers:
                                job.worker_name = worker
                                self.running_jobs.add(job)
                                self.workers[worker].spawn(job)
                                resources[worker][
                                    "cores"
                                ] = (
                                    0
                                )  # since the job modifying blocks the worker-Process (runs in it), no point in spawning further ones till it has returned.
                            break
                        else:
                            if (
                                (
                                    job.cores_needed == -1
                                    and resources[worker]["cores"]
                                    >= resources[worker]["total cores"] - 1
                                )  # -1 (use all cores) jobs also can be started if there's a single other (long running) one core job running
                                or (
                                    job.cores_needed == -2
                                    and resources[worker]["cores"]
                                    == resources[worker]["total cores"]
                                )  # -2 (use all cores for realz) jobs only run if the machine is really empty...
                            ) and (
                                job.memory_needed == -1
                                or (
                                    job.memory_needed < resources[worker]["memory"]
                                    or (
                                        (
                                            resources[worker]["memory"]
                                            == resources[worker]["physical_memory"]
                                        )
                                        and job.memory_needed
                                        < resources[worker]["physical_memory"]
                                        + resources[worker]["swap_memory"]
                                    )
                                )
                            ):
                                job.worker_name = worker
                                self.workers[worker].spawn(job)
                                self.running_jobs.add(job)
                                to_remove.append(job)
                                resources[worker]["cores"] = 0
                                # don't worry about memory...
                                break
                            elif (
                                job.cores_needed <= resources[worker]["cores"]
                                and (job.cores_needed != -1)
                                and (
                                    job.memory_needed == -1
                                    or (
                                        job.memory_needed < resources[worker]["memory"]
                                        or (
                                            (
                                                resources[worker]["memory"]
                                                == resources[worker]["physical_memory"]
                                            )
                                            and job.memory_needed
                                            < resources[worker]["physical_memory"]
                                            + resources[worker]["swap_memory"]
                                        )
                                    )
                                )
                            ):
                                job.worker_name = worker
                                self.workers[worker].spawn(job)
                                self.running_jobs.add(job)
                                to_remove.append(job)
                                resources[worker]["cores"] -= job.cores_needed
                                if job.memory_needed == -1:
                                    resources[worker]["memory"] -= resources[worker][
                                        "memory/core"
                                    ]
                                else:
                                    resources[worker]["memory"] -= job.memory_needed
                                break
                            else:
                                # this job needed to much resources, or was not runnable
                                runnable_jobs.remove(
                                    job
                                )  # can't run right now on this worker..., maybe later...
                                # can't exceed number of cores, that is tested
                                # before hand

                        next_job += 1
                    # we do this for every worker...
                    for job in to_remove:
                        self.possible_execution_order.remove(
                            job
                        )  # certain we could do better than with a list...
                        runnable_jobs.remove(job)
            error_count -= 1
            if error_count == 0:  # pragma: no cover
                raise ppg_exceptions.RuntimeException(
                    "There was a loop error that should never 've been reached in start_jobs"
                )

    def prune_job(self, job, depth=0):
        """Remove job (and its descendands) from the list of jobs to run"""
        try:
            self.possible_execution_order.remove(job)
            found = True
        except ValueError:  # might occur sereval times when following the graph...
            found = False
            pass
        job.failed = True
        job.error_reason = "Indirect"
        if (
            found or depth == 0 or job.is_loadable()
        ):  # if this job wasn't in the graph, it's descendands won't be either. But a job that was run and failed has already been removed. Loadable jobs are never in the possible_execution_order
            for dep in job.dependants:
                self.prune_job(dep, depth + 1)

    def job_executed(self, job):
        """A job was done. Returns whether there are more jobs read run"""
        os.chdir(
            self.chdir
        )  # no job must modify this, otherwise the checks get inconsistent
        job._reset_is_done_cache()
        if job.failed:
            self.logger.warning("job_executed %s failed: %s" % (job, job.failed))
        else:
            self.logger.info("job_executed %s failed: %s" % (job, job.failed))
        if job.failed:
            self.prune_job(job)
            if job.exception:
                job.error_reason = "Exception"
            else:  # pragma: no cover - just paranoia
                job.error_reason = "Unknown/died"
            if not self.quiet:
                self.print_failed_job(job, sys.stderr)
        else:
            job.was_run = True
            job.check_prerequisites_for_cleanup()
            self.logger.info("%s runtime: %s" % (job, time.time() - job.start_time))
        if hasattr(job, "stdout_handle") and job.stdout_handle:
            job.stdout_handle.close()
        if hasattr(job, "stderr_handle") and job.stderr_handle:
            job.stderr_handle.close()
        if (
            not job.is_loadable()
        ):  # dataloading jobs are not 'seperatly' executed, but still, they may be signaled as failed by a worker.
            self.running_jobs.remove(job)
        # self.signal_job_done()
        self.jobs_done_count += 1
        if not self.quiet:
            sys.stderr.write(
                "Done %i of %i jobs (%i total including non-running)\n"
                % (self.jobs_done_count, self.jobs_to_run_count, len(self.jobs))
            )
        return bool(self.running_jobs) or bool(self.possible_execution_order)

    def new_jobs_generated_during_runtime(self, new_jobs):  # noqa:C901
        """Received jobs from one of the job generating Jobs.
        We'll integrate them into the graph, and add them to the possible_execution_order.
        Beauty of the job-singeltonization is that all dependants that are not new are caught..."""

        def check_preqs(job):
            for preq_job_id in job.prerequisites:
                if preq_job_id not in self.jobs and preq_job_id not in new_jobs:
                    raise ppg_exceptions.JobContractError(  # pragma: no cover - defensive
                        "New job depends on job that is not in the job list but also not in the new jobs: %s"
                        % preq_job_id
                    )
            for dep_job_id in job.dependants:
                if dep_job_id not in self.jobs and dep_job_id not in new_jobs:
                    raise ppg_exceptions.JobContractError(  # pragma: no cover - defensive
                        "New job was dependency for is not in the job list but also not in the new jobs"
                    )
                    # the case that it is injected as a dependency for a job that might have already been done
                    # is being taken care of in the JobGeneratingJob and DependencyInjectionJob s

        for job in new_jobs.values():
            check_preqs(job)
            self.jobs[job.job_id] = job
        for job in new_jobs.values():  # canonize these jobs
            job.prerequisites = set([self.jobs[job_id] for job_id in job.prerequisites])
            job.dependants = set([self.jobs[job_id] for job_id in job.dependants])
        for job in new_jobs.values():
            job._reset_is_done_cache()
            for p in job.prerequisites:
                if p.failed:
                    self.prune_job(job)
                    break
                else:
                    if not job.is_done():
                        if not job.always_runs:
                            job.invalidated(reason="not done")
        # self.fill_dependency_callbacks()
        self.connect_graph()
        self.distribute_invariant_changes()
        if self.do_dump_graph:
            self.dump_graph()
        # we not only need to check the jobs we have received, we also need to check their dependants
        # for example there might have been a DependencyInjectionJob than injected a DataLoadingJob
        # that had it's invariant FunctionInvariant changed...
        jobs_to_check = set(new_jobs.values())

        def add_dependands(j):
            for dep in j.dependants:
                if dep not in self.possible_execution_order:  # don't add it twice
                    jobs_to_check.add(dep)
                add_dependands(dep)

        for job in new_jobs.values():
            if job.was_invalidated:
                add_dependands(job)

        # and now, let's see what new stuff needs to be done.
        for job in jobs_to_check:
            if job.was_run:
                raise ppg_exceptions.RuntimeException(  # pragma: no cover
                    "A job that was already run was readded to the check-if-it-needs executing list. This should not happen"
                )
            if job.is_loadable():
                pass
            elif not job.is_done() and not job.failed:
                self.possible_execution_order.append(job)
                self.jobs_to_run_count += 1
            elif not job.runs_in_worker():
                job.was_run = True  # invarites get marked as ran..
            else:
                pass
        # for worker in self.worker.values():
        # worker.transmit_new_jobs(new_jobs)
        self.check_all_jobs_can_be_executed()  # for this, the job must be in possible_execution_order

    def tranfer_new_jobs(self):
        """push jobs from self.new_jobs into self.jobs.
        This is called in a remote worker that just created a bunch of jobs and send them of to the master,
        so that they're still fresh"""
        for job_id in self.new_jobs:
            self.jobs[job_id] = self.new_jobs[job_id]

    def print_failed_job(self, job, file_handle):
        """Pretty print failure information"""
        print("-" * 75, file=file_handle)
        print("%s failed. Reason:" % (job,), file=file_handle)
        if job.exception:
            print(
                "\tThrew an exception %s"
                % (str(job.exception).encode("ascii", errors="replace"),),
                file=file_handle,
            )
            print("\tTraceback: %s" % (job.trace,), file=file_handle)

        print("\t stdout was %s" % (job.stdout,), file=file_handle)
        print("\t stderr was %s" % (job.stderr,), file=file_handle)
        print("", file=file_handle)

    def print_running_jobs(self):  # pragma: no cover
        """When the user presses enter, print the jobs that are currently being executed"""
        print("Running jobs:")
        now = time.time()
        for job in self.running_jobs:
            print(job, "running for %i seconds" % (now - job.start_time))

    def dump_graph(self):  # pragma: no cover
        """Dump the current graph in text format into logs/ppg_graph.txt if logs exists"""

        def do_dump():
            from . import job as jobs

            nodes = {}
            edges = []
            for job in self.jobs.values():
                if not (
                    isinstance(
                        job,
                        (
                            jobs.FunctionInvariant,
                            jobs.ParameterInvariant,
                            jobs.FileChecksumInvariant,
                            jobs.FinalJob,
                        ),
                    )
                ):
                    nodes[job.job_id] = {
                        "is_done": job._is_done,
                        "was_run": job.was_run,
                        "type": job.__class__.__name__,
                    }
                    for preq in job.prerequisites:
                        edges.append((preq.job_id, job.job_id))
            # self._write_xgmml("logs/ppg_graph.xgmml", nodes, edges)
            self._write_gml("logs/ppg_graph.gml", nodes, edges)

        if os.path.exists("logs") and os.path.isdir("logs"):
            pid = os.fork()
            if pid == 0:  # run this in an unrelated child process
                if "PYPIPEGRAPH_DO_COVERAGE" in os.environ:
                    import coverage

                    cov = coverage.coverage(
                        data_suffix=True,
                        config_file=os.environ["PYPIPEGRAPH_DO_COVERAGE"],
                    )
                    cov.start()
                    try:
                        do_dump()
                    finally:
                        cov.stop()
                        cov.save()
                        pass
                else:
                    do_dump()
                # raise ValueError("Dump Exit")
                os._exit(0)  # Cleanup is for parent processes!
            self.dump_pid = pid

    def _write_xgmml(
        self, output_filename, node_to_attribute_dict, edges
    ):  # pragma: no cover
        from xml.sax.saxutils import escape

        op = open(output_filename, "wb")
        op.write(
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <graph
            label="unlabled"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:cy="http://www.cytoscape.org"
            xmlns="http://www.cs.rpi.edu/XGMML"
            directed="1">
            """
        )
        node_id = 0
        names_to_ids = {}
        for node_name, attributes in node_to_attribute_dict.items():
            op.write("""<node label="%s" id="%i">\n""" % (escape(node_name), node_id))
            names_to_ids[node_name] = node_id
            node_id += 1
            op.write(
                """<att name="title" type="string" value="%s"/>\n"""
                % (escape(node_name),)
            )
            for attribute, value in attributes.items():
                op.write(
                    """<att name="%s" type="string" value="%s"/>\n"""
                    % (escape(attribute), escape("%s" % (value,)))
                )
            op.write("</node>\n")
        for start, end in edges:
            op.write(
                """<edge label="%s-%s" source="%i" target="%i"></edge>\n"""
                % (escape(start), escape(end), names_to_ids[start], names_to_ids[end])
            )
        op.write(
            """
        </graph>
        """
        )
        op.close()

    def _write_gml(
        self, output_filename, node_to_attribute_dict, edges
    ):  # pragma: no cover
        op = open(output_filename, "w")
        op.write(
            """graph
[
"""
        )
        names_to_ids = {}
        node_id = 0
        colors = {
            "FileGeneratingJob": "#5050AF",
            "TempFileGeneratingJob": "#9050AF",
            "MultiFileGeneratingJob": "#1010FF",
            "JobGeneratingJob": "#AF0000",
            "DependencyInjectionJob": "#FF0000",
            "DataLoadingJob": "#50FF50",
            "AttributeLoadingJob": "#10FF10",
            "PlotJob": "#AF00AF",
            "_CacheFileGeneratingJob": "#7f7f7f",
            "CachedDataLoadingJob": "#Ef7f7f",
        }
        for node_name, attributes in node_to_attribute_dict.items():
            op.write(
                """\tnode
    [
        id %i
        label "%s"
        graphics
        [
            fill "%s"
        ]
    ]
"""
                % (
                    node_id,
                    node_name,
                    colors[attributes["type"]]
                    if attributes["type"] in colors
                    else "#FFFFFF",
                )
            )
            names_to_ids[node_name] = node_id
            node_id += 1
        op.flush()
        for start, end in edges:
            if start in names_to_ids and end in names_to_ids:
                op.write(
                    """\tedge
    [
        source %i
        target %i
    ]
"""
                    % (names_to_ids[start], names_to_ids[end])
                )
        op.write("]\n")
        op.flush()
        if not self.quiet:
            print("wrote gml")
        op.close()

    def install_signals(self):
        """make sure we don't crash just because the user logged of"""

        def hup():  # pragma: no cover
            self.logger.debug("user logged off - continuing run")

        self._old_signal_up = signal.signal(signal.SIGHUP, hup)

    def restore_signals(self):
        if self._old_signal_up:
            signal.signal(signal.SIGHUP, self._old_signal_up)

    def get_error_count(self):  # pragma: no cover
        count = 0
        for job in self.jobs.values():
            if job.failed or job in self.invariant_loading_issues:
                count += 1
        return count

    def signal_finished(self):  # pragma: no cover
        """If there's a .pipegraph_finished.py in ~, call it"""
        fn = os.path.expanduser("~/.pipegraph_finished.py")
        if os.path.exists(fn) and not os.path.isdir(fn) and os.access(fn, os.X_OK):
            os.system(fn)

    def dump_runtimes(self, filename):  # pragma: no cover
        with open(filename, "w") as op:
            for j in self.jobs.values():
                if hasattr(j, "runtime"):
                    op.write("%.2fs\t%s\n" % (j.runtime, j.job_id))
