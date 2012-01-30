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
import time
import collections
import resource_coordinators
import cPickle
import pickle
import ppg_exceptions
import util
import sys
logger = util.start_logging('graph')

# earlier on, we had a different pickling scheme,
# and that's what the files were called.
invariant_status_filename_old = '.pypipegraph_status'
invariant_status_filename_default = '.pypipegraph_status_robust'


def run_pipegraph():
    """Run the current global pipegraph"""
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    util.global_pipegraph.run()


def new_pipegraph(resource_coordinator=None, quiet=False,
        invariant_status_filename=invariant_status_filename_default):
    """Create a new global pipegraph.
    New jobs will automagically be attached to this pipegraph.
    Default ResourceCoordinator is L{LocalSystem}
    """
    if resource_coordinator is None:
        resource_coordinator = resource_coordinators.LocalSystem()
    util.global_pipegraph = Pipegraph(resource_coordinator, quiet=quiet, 
            invariant_status_filename=invariant_status_filename)
    util.job_uniquifier = {}
    util.func_hashes = {}
    logger.info("\n\n")
    logger.info("New Pipegraph")


def forget_job_status(invariant_status_filename=invariant_status_filename_default):
    """Delete the job status file - usually only useful for testing"""
    try:
        os.unlink(invariant_status_filename)
    except OSError:
        pass
    try:
        os.unlink(invariant_status_filename_old)
    except OSError:
        pass


def destroy_global_pipegraph():
    """Free the current global pipegraph - usually only useful for testing"""
    util.global_pipegraph = None


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
    def __init__(self, resource_coordinator, quiet=False, invariant_status_filename=invariant_status_filename_default):
        self.rc = resource_coordinator
        self.rc.pipegraph = self
        self.jobs = {}
        self.running = False
        self.was_run = False
        self.new_jobs = False
        self.quiet = quiet
        self.object_uniquifier = {}  # used by util.assert_uniqueness_of_object to enforce pseudo-singletons
        self.invariant_loading_issues = {}  # jobs whose invariant could not be unpickled for some reason - and the exception.
        self._distribute_invariant_changes_count = 0
        self.invariant_status_filename = invariant_status_filename

    def __del__(self):
        # remove circle link between rc and pipegraph
        self.rc.pipegraph = None

    def add_job(self, job):
        """Add a job to a Pipegraph.
        Usually automagically called when instanciating one of the Job classes
        """
        #logger.info("Adding job %s" % job)
        if not self.running:
            if self.was_run:
                raise ValueError("This pipegraph was already run. You need to create a new one for more jobs")
            self.jobs[job.job_id] = job
        else:
            if self.new_jobs is False:
                raise ValueError("Trying to add new jobs to running pipeline without having new_jobs set (ie. outside of a graph modifying job) - tried to add %s" % job)
            elif self.new_jobs is None:
                logger.info("Ignored: Trying to add new jobs to running pipeline without having new_jobs set (ie. outside of a graph modifying job) - tried to add %s" % job)
                return
            if not job.job_id in self.jobs:
            #    logger.info("Adding job to new_jobs %s %s" % (job, id(self.new_jobs)))
                self.new_jobs[job.job_id] = job
            else:
                pass
                #logger.info("Already knew the job %s, not keeping it as new" % job)

    def run(self):
        """Run the Pipegraph. Gives control to the graph until all jobs have run (or failed).
        May fail right away if a JobContractError occurs - for example if you've built a cycle
        of dependencies.

        If any job does not complete (exception, segfault, failed to produce output), RuntimeError
        will be thrown, and the job's exception (if available) be stored in job.exception

        """
        if self.was_run:
            raise ValueError("Each pipegraph may be run only once.")
        logger.info("MCP pid: %i" % os.getpid())
        #internal to the mcp
        self.inject_auto_invariants()
        self.was_run = True  # since build_

        self.running = True
        self.new_jobs = False  # this get's changed in graph modifying jobs, but they reset it to false, which means 'don't accept any new jobs while we are running'
        self.connect_graph()
        self.check_cycles()
        self.load_invariant_status()
        self.distribute_invariant_changes()
        self.build_todo_list()
        self.dump_graph()

        #make up some computational engines and put them to work.
        logger.info("now executing")
        try:
            self.spawn_slaves()
            self.execute_jobs()

        finally:
            #clean up
            logger.info('starting cleanup')
            util.flush_logging()
            self.dump_invariant_status()
            self.destroy_job_connections()
            logger.info('sucessfull cleanup')

        #and propagate if there was an exception
        try:
            any_failed = False
            try:
                error_log = open("logs/ppg_errors.txt", 'wb')
            except:
                error_log = open("/dev/null", 'wb')
            for job in self.jobs.values():
                if job.failed or job in self.invariant_loading_issues:
                    if not any_failed and not self.quiet:
                        print >>sys.stderr, '\n------Pipegraph error-----'
                        print >>error_log, '\n------Pipegraph error-----'
                    any_failed = True
                    if job.error_reason != 'Indirect' and not self.quiet:
                        self.print_failed_job(job, sys.stderr)
                        self.print_failed_job(job, error_log)
                    if job in self.invariant_loading_issues:
                        print >>error_log, 'Could not unpickle invariant for %s - exception was %s' % (job, self.invariant_loading_issues[job])
                        print >>sys.stderr, 'Could not unpickle invariant for %s - exception was %s' % (job, self.invariant_loading_issues[job])
            if any_failed:
                if not self.quiet:
                    print >>sys.stderr, '\n------Pipegraph output end-----'
                    print >>error_log, '\n------Pipegraph output end-----'
                    if error_log.name != '/dev/null':
                        print >>sys.stderr, '\n failed job log in logs/ppg_erros'
                raise ppg_exceptions.RuntimeError()
            else:
                self.cleanup_jobs_that_requested_it()
        finally:
            if not self.quiet:
                print 'Pipegraph done. Executed %i jobs. %i failed' % (len([x for x in self.jobs.values() if x.was_run]), len([x for x in self.jobs.values() if x.failed]))

    def inject_auto_invariants(self):
        """Go through each job and ask it to create the invariants it might need.
        Why is this not done by the jobs on init? So the user can prevent them from doing it (job.do_ignore_code_changes()...)"""
        for job in self.jobs.values():
            job.inject_auto_invariants()

    def connect_graph(self):
        """Convert the dependency graph in jobs into a bidirectional graph"""
        #connect graph
        for job in self.jobs.values():
            if job.was_run:
                continue
            #logger.info("X %s %s %s " % (job , job.prerequisites, job.dependants))
            for preq in job.prerequisites:
                preq.dependants.add(job)

        #inject final jobs to run after all others...
        for job in self.jobs.values():
            if job.is_final_job:
                for jobB in self.jobs.values():
                    if not jobB.is_final_job and not jobB.dependants:
                        job.prerequisites.add(jobB)
                        jobB.dependants.add(job)

    def destroy_job_connections(self):
        """Delete connections between jobs for gc purposes"""
        for job in self.jobs.values():
            job.dependants = None
            job.prerequisites = None

    def check_cycles(self):
        """Check whether there are any loops in the graph which prevent execution.

        Basically imposes a topological ordering, and if that's impossible, we have a cycle.
        Also, this gives a valid, job by job order of executing them.
        """
        for job in self.jobs.values():
            job.dependants_copy = job.dependants.copy()
        L = []
        S = [job for job in self.jobs.values() if len(job.dependants_copy) == 0]
        while S:
            n = S.pop()
            L.append(n)
            for m in n.prerequisites:
                m.dependants_copy.remove(n)
                if not m.dependants_copy:
                    S.append(m)
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
                if job.is_in_dependency_chain(job, max_depth):  # we have to terminate this, otherwise the endless loop will explode python
                    jobs_in_circles.append(job)

            def find_circle_path(current_node, search_node, path, depth=0):
                if depth > max_depth:
                    return False
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
            raise ppg_exceptions.CycleError("At least one cycle in the graph was detected\nJobs involved (up to a depth of %i) are\n %s " % (max_depth, pprint.pformat(job_path_tuples[0])))
        L.reverse()
        self.possible_execution_order = L

    def load_invariant_status(self):
        """Load Job invariant status from disk (and self.invariant_status_filename)
        """
        if os.path.exists(invariant_status_filename_old):
            # this is support code for the late invariant status filename layout.
            # probably can be thrown out 'soonish'. Also remove the reference in forget_job_status
            op = open(invariant_status_filename_old, 'rb')
            self.invariant_status = cPickle.load(op)
            op.close()
            os.unlink(invariant_status_filename_old)  # throw away the old file
        elif os.path.exists(self.invariant_status_filename):
            op = open(self.invariant_status_filename, 'rb')
            all = op.read()
            op.seek(0, os.SEEK_SET)
            self.invariant_status = collections.defaultdict(bool)
            leave = False
            while not leave:
                try:
                    key = None
                    key = cPickle.load(op)
                    value = cPickle.load(op)
                    self.invariant_status[key] = value
                except TypeError, e:
                    print e
                    if key is None:
                        raise ValueError("Could not depickle invariants - even in the robust implementation (key not found")
                    logger.error("Could not depickle invariant for %s - check code for depickling bugs. Job will rerun, probably until the (de)pickling bug is fixed.\n Exception: %s" % (key, e))
                    self.invariant_loading_issues[key] = e
                    letter = op.read(1)
                    #at least try to find the end of the pickle... this might fail horribly though
                    while letter and letter != pickle.STOP:
                        letter = op.read(1)
                    if not letter:
                        leave = True
                except EOFError:
                    leave = True
        else:
            self.invariant_status = collections.defaultdict(bool)

    def dump_invariant_status(self):
        """Store Job invariant status into a file named by self.invariant_status_filename"""
        finished = False
        while not finished:
            try:
                op = open(self.invariant_status_filename, 'wb')
                for key, value in self.invariant_status.items():
                    cPickle.dump(key, op, cPickle.HIGHEST_PROTOCOL)
                    cPickle.dump(value, op, cPickle.HIGHEST_PROTOCOL)
                op.close()
                finished = True
            except KeyboardInterrupt:
                pass

    def distribute_invariant_changes(self):
        """Check each job for whether it's invariance has changed,
        and propagate the invalidation by calling job.invalidated()"""
        self._distribute_invariant_changes_count += 1
        for job in self.jobs.values():
            if job.was_invalidated:  # don't redo it just because we're calling this again after having received new jobs.
                if job.invalidation_count == self._distribute_invariant_changes_count:
                    continue
                else:
                    job.distribute_invalidation()
                    job.invalidation_count = self._distribute_invariant_changes_count
            old = self.invariant_status[job.job_id]
            try:
                inv = job.get_invariant(old)
                #logger.info("%s invariant was %s, is now %s" % (job, old,inv))
            except util.NothingChanged, e:
                #logger.info("Invariant difference, but NothingChanged")
                inv = e.new_value
                old = inv  # so no change...
                self.invariant_status[job.job_id] = inv  # so not to recheck next time...
            if inv != old:
                if False:
                    logger.info("Invariant change for %s" % job)
                    logger.info("%s invariant was %s, is now %s" % (job, old, inv))
                    if type(old) is str and type(inv) is str:
                        import difflib
                        for line in difflib.unified_diff(old.split("\n"), inv.split("\n"), n=5):
                            logger.info(line)
                job.invalidated(reason='invariant')
                job.invalidation_count = self._distribute_invariant_changes_count
                self.invariant_status[job.job_id] = inv  # for now, it is the dependant job's job to clean up so they get reinvalidated if the executing is terminated before they are reubild (ie. filegenjobs delete their outputfiles)

    def build_todo_list(self):
        """Go through each job. If it needs to be done, invalidate() all dependands.
        also requires all prerequisites to require_loading
        """
        needs_to_be_run = set()
        for job in self.jobs.values():
            if not job.is_done():
                logger.info("Was not done: %s" % job)
                if not job.is_loadable():
                    logger.info("and is not loadable")
                    needs_to_be_run.add(job.job_id)
                    if not job.always_runs:  # there is no need for the job injecting jobs to invalidate just because they need to be run.
                        job.invalidated('not done')
                        if not job.was_invalidated:  # paranoia
                            raise ppg_exceptions.RuntimeException("job.invalidated called, but was_invalidated was false")
                #for preq in job.prerequisites:
                    #preq.require_loading() #think I can get away with  lettinng the slaves what they need to execute a given job...
            else:
                logger.info("was done %s. Invalidation status: %s" % (job, job.was_invalidated))

        for job in self.jobs.values():
            if (job.was_invalidated  # this has been invalidated
                and job.runs_in_slave()  # it is not one of the invariantes
                and not job.is_loadable()  # and it is not a loading job (these the slaves do automagically for now)
                ):
                needs_to_be_run.add(job.job_id)
            elif (not job.runs_in_slave()):
                logger.info("Mark was_run before running: %s" % job)
                logger.info("job.was_invalidated %s, job.runs_in_slave %s, job.is_loadable: %s" % (job.was_invalidated, job.runs_in_slave(), job.is_loadable()))
                job.was_run = True  # invarites get marked as ran..
        #now prune the possible_execution_order
        self.possible_execution_order = [job for job in self.possible_execution_order if job.job_id in needs_to_be_run]
        self.jobs_to_run_count = len(self.possible_execution_order)
        self.jobs_done_count = 0
        logger.info(" possible execution order %s" % [str(x) for x in self.possible_execution_order])

    def spawn_slaves(self):
        """Tell the resource coordinator to get the slaves ready"""
        logger.info("Spawning slaves")
        self.slaves = self.rc.spawn_slaves()
        self.check_all_jobs_can_be_executed()

    def check_all_jobs_can_be_executed(self):
        """Check all jobs for memory/cpu requirements and prune those that we can't satisfy"""
        resources = self.rc.get_resources()  # a dict of slave name > {cores: y, memory: x}
        maximal_memory = max([x['physical_memory'] + x['swap_memory'] for x in resources.values()])
        maximal_cores = max([x['cores'] for x in resources.values()])
        if maximal_cores == 0:
            raise ppg_exceptions.RuntimeException("No cores available?!")

        for job in self.possible_execution_order:
            #logger.info("checking job %s,  %s %s vs %s %s " % (job, job.cores_needed, job.memory_needed, maximal_cores, maximal_memory))
            if job.cores_needed > maximal_cores or job.memory_needed > maximal_memory:
                logger.info("Pruning job %s, needed to many resources cores needed: %s, memory needed: %s vs %s" % (job, job.cores_needed, job.memory_needed, resources))
                self.prune_job(job)
                job.error_reason = "Needed to much memory/cores"
                job.failed = True

    def execute_jobs(self):
        """Pass control to ResourceCoordinator"""
        self.jobs_by_slave = {}
        #the rc loop externalizes the start_jobs / job_executed, start more jobs
        self.running_jobs = set()
        if self.possible_execution_order:  # no jobs, no spawning...
            self.rc.pipegraph = self
            if not self.quiet:
                sys.stderr.write("Done %i of %i jobs (%i total including non-running)\r" % (self.jobs_done_count, self.jobs_to_run_count, len(self.jobs)))

            logger.info("Executing jobs/passing control to RC")
            self.rc.enter_loop()  # doesn't return until all jobs have been done.
            logger.info("Control returned from ResourceCoordinator")

    def cleanup_jobs_that_requested_it(self):
        """Temporary* generating jobs that don't get run (because their downstream is done) might still attempt to clean up their output. They register by job.setting do_cleanup_if_was_never_run = True"""
        for job in self.jobs.values():
            if job.do_cleanup_if_was_never_run and not job.was_run:
                job.cleanup()

    def start_jobs(self):  
        """Instruct slaves to start as many jobs as we can currently spawn under our memory/cpu restrictions"""
        # I really don't like this function... and I also have the strong inkling it should acttually sit in the resource coordinatora         # first, check what we actually have some resources...
        resources = self.rc.get_resources()  # a dict of slave name > {cores: y, memory: x}
        for slave in resources:
            resources[slave]['memory/core'] = resources[slave]['physical_memory'] / resources[slave]['cores']
            resources[slave]['memory'] = resources[slave]['physical_memory']
            resources[slave]['total cores'] = resources[slave]['cores']
         # substract the running jobs
        for job in self.running_jobs:
            slave = job.slave_name
            if job.cores_needed == -1 or job.modifies_jobgraph():  # since that job blocks the slave..
                resources[slave]['cores'] = 0
            else:
                resources[slave]['cores'] -= 1
            if job.memory_needed == -1:
                resources[slave]['memory'] -= resources[slave]['memory/core']
            else:
                resources[slave]['memory'] -= job.memory_needed

        logger.info("Resources after substracting %i running jobs: %s" % (len(self.running_jobs), resources))
        logger.info("Currently running jobs:")

        def resources_available(resources):
            logger.info("resources available %s" % resources)
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    return True
            return False
        error_count = len(self.jobs) * 2
        maximal_startable_jobs = sum(x['cores'] for x in resources.values())
        runnable_jobs = []
        logger.info("maximal_startable_jobs: %i" % maximal_startable_jobs)
        for job in self.possible_execution_order:
            if job.can_run_now():
                runnable_jobs.append(job)
                if len(runnable_jobs) == maximal_startable_jobs:
                    break
        if self.possible_execution_order and not runnable_jobs and not self.running_jobs:
            logger.info("Nothing running, nothing runnable, but work left to do")
            logger.info("job\tcan_run_now\tlist_blocks")
            for job in self.possible_execution_order:
                logger.info("%s\t%s\t%s" % (job, job.can_run_now(), job.list_blocks()))
            util.flush_logging()
            logger.exception("Job excution order error")
            raise ppg_exceptions.RuntimeException(
            """We had more jobs that needed to be done, none of them could be run right now and none were running. Sounds like a dependency graph bug to me""")

        while resources_available(resources) and runnable_jobs:
            logger.info("resources were available %s " % resources)
            to_remove = []
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    next_job = 0
                    logger.info('remaining %i jobs' % len(self.possible_execution_order))
                    #while next_job < len(self.possible_execution_order): #todo: restrict to runnable_jobs
                    while next_job < len(runnable_jobs):
                        job = runnable_jobs[next_job]
                        #if job.can_run_now():
                        #Todo: Keep track of which dataloadingjobs have already been performed on each node
                        #and prioritize by that...
                        if job.modifies_jobgraph():
                            to_remove.append(job)  # remove just once...
                            for slave in self.slaves:
                                job.slave_name = slave
                                logger.info("running_jobs added :%s" % job)
                                self.running_jobs.add(job)
                                self.slaves[slave].spawn(job)
                                resources[slave]['cores'] = 0  # since the job modifying blocks the Slave-Process (runs in it), no point in spawning further ones till it has returned.
                            break
                        else:
                            if (job.cores_needed == -1 and resources[slave]['cores'] == resources[slave]['total cores']
                                and (job.memory_needed == -1 or
                                    (
                                        job.memory_needed < resources[slave]['memory']
                                        or (
                                            (resources[slave]['memory'] == resources[slave]['physical_memory']) and
                                            job.memory_needed < resources[slave]['physical_memory'] + resources[slave]['swap_memory'])
                                        )

                                        )):
                                    job.slave_name = slave
                                    self.slaves[slave].spawn(job)
                                    logger.info("running_jobs added2 :%s" % job)
                                    self.running_jobs.add(job)
                                    to_remove.append(job)
                                    resources[slave]['cores'] = 0
                                    #don't worry about memory...
                                    break
                            elif (job.cores_needed <= resources[slave]['cores'] and (job.cores_needed != -1)
                                and (job.memory_needed == -1 or
                                    (
                                        job.memory_needed < resources[slave]['memory']
                                        or (
                                            (resources[slave]['memory'] == resources[slave]['physical_memory']) and
                                            job.memory_needed < resources[slave]['physical_memory'] + resources[slave]['swap_memory'])
                                        ))):
                                job.slave_name = slave
                                self.slaves[slave].spawn(job)
                                logger.info("running_jobs added3 :%s" % job)
                                self.running_jobs.add(job)
                                to_remove.append(job)
                                resources[slave]['cores'] -= job.cores_needed
                                if job.memory_needed == -1:
                                    resources[slave]['memory'] -= resources[slave]['memory/core']
                                else:
                                    resources[slave]['memory'] -= job.memory_needed
                                break
                            else:
                                #this job needed to much resources, or was not runnable
                                logger.info("Job needed too many resources %s" % job)
                                runnable_jobs.remove(job)  # can't run right now on this slave..., maybe later...
                        next_job += 1
                    logger.info('Resources after spawning %s' % resources)
                    #we do this for every slave...
                    for job in to_remove:
                        logger.info("removing job %s" % job)
                        self.possible_execution_order.remove(job)  # certain we could do better than with a list...
                        logger.info("removing job from runnable %s" % job)
                        runnable_jobs.remove(job)
            error_count -= 1
            if error_count == 0:
                raise ppg_exceptions.RuntimeException("There was a loop error that should never 've been reached in start_jobs")
        logger.info("can't start any more jobs. either there are no more, or resources all utilized. There are currently %i jobs remaining" % len(self.possible_execution_order))

    def prune_job(self, job):
        """Remove job (and its descendands) from the list of jobs to run"""
        try:
            self.possible_execution_order.remove(job)
        except ValueError:  # might occur sereval times when following the graph...
            pass
        job.failed = True
        job.error_reason = "Indirect"
        for dep in job.dependants:
            self.prune_job(dep)

    def job_executed(self, job):
        """A job was done. Returns whether there are more jobs read run"""
        logger.info("job_executed %s failed: %s" % (job, job.failed))
        if job.failed:
            self.prune_job(job)
            if job.exception:
                job.error_reason = "Exception"
            else:
                job.error_reason = "Unknown/died"
            if not self.quiet:
                self.print_failed_job(job, sys.stderr)
        else:
            logger.info("Setting %s.was_run to true, returned" % job)
            job.was_run = True
            job.check_prerequisites_for_cleanup()
        if not job.is_loadable():  # dataloading jobs are not 'seperatly' executed, but still, they may be signaled as failed by a slave.
            logger.info("running_jobs removed :%s" % job)
            self.running_jobs.remove(job)
        #self.signal_job_done()
        self.jobs_done_count += 1
        if not self.quiet:
            sys.stderr.write("Done %i of %i jobs (%i total including non-running)\r" % (self.jobs_done_count, self.jobs_to_run_count, len(self.jobs)))
        return bool(self.running_jobs) or bool(self.possible_execution_order)

    def new_jobs_generated_during_runtime(self, new_jobs):
        """Received jobs from one of the job generating Jobs.
        We'll integrate them into the graph, and add them to the possible_execution_order.
        Beauty of the job-singeltonization is that all dependands that are not new are caught..."""
        logger.info('new_jobs_generated_during_runtime')

        def check_preqs(job):
            for preq_job_id in job.prerequisites:
                if not preq_job_id in self.jobs and not preq_job_id in new_jobs:
                    raise ppg_exceptions.JobContractError("New job depends on job that is not in the job list but also not in the new jobs")
            for dep_job_id in job.dependants:
                if not dep_job_id in self.jobs and not dep_job_id in new_jobs:
                    raise ppg_exceptions.JobContractError("New job was dependency for is not in the job list but also not in the new jobs")
                    #the case that it is injected as a dependency for a job that might have already been done
                    #is being taken care of in the JobGeneratingJob and DependencyInjectionJob s

        for job in new_jobs.values():
            #logger.info('new job %s' % job)
            check_preqs(job)
            self.jobs[job.job_id] = job
        for job in new_jobs.values():  # canonize these jobs
            job.prerequisites = set([self.jobs[job_id] for job_id in job.prerequisites])
            job.dependants = set([self.jobs[job_id] for job_id in job.dependants])
        for job in new_jobs.values():
            if not job.is_done():
                if not job.always_runs:
                    job.invalidated(reason='not done')
        self.connect_graph()
        self.distribute_invariant_changes()
        #we not only need to check the jobs we have received, we also need to check their dependands
        #for example there might have been a DependencyInjectionJob than injected a DataLoadingJob
        #that had it's invariant FunctionInvariant changed...
        jobs_to_check = set(new_jobs.values())

        def add_dependands(j):
            for dep in j.dependants:
                if not dep in self.possible_execution_order:  # don't add it twice
                    jobs_to_check.add(dep)
                add_dependands(dep)
        for job in new_jobs.values():
            if job.was_invalidated:
                add_dependands(job)

        #and now, let's see what new stuff needs to be done.
        for job in jobs_to_check:
            if job.was_run:
                raise ValueError("A job that was already run was readded to the check-if-it-needs executing list. This should not happen")
            if job.is_loadable():
                logger.info("Ignoring %s" % job)
                pass
            elif not job.is_done():
                logger.info("Adding %s to possible_execution_order" % job)
                self.possible_execution_order.append(job)
                self.jobs_to_run_count += 1
            elif not job.runs_in_slave():
                logger.info("ignoring invariant - does not need to run " % job)
                logger.info("Setting %s.was_run to true, generated" % job)
                job.was_run = True  # invarites get marked as ran..
            else:
                logger.info("Not doing anything with %s, was done" % job)
                pass
        #for slave in self.slaves.values():
            #slave.transmit_new_jobs(new_jobs)
        self.check_all_jobs_can_be_executed()  # for this, the job must be in possible_execution_order

    def tranfer_new_jobs(self):
        """push jobs from self.new_jobs into self.jobs.
        This is called in a remote slave that just created a bunch of jobs and send them of to the master,
        so that they're still fresh"""
        for job_id in self.new_jobs:
            self.jobs[job_id] = self.new_jobs[job_id]

    def print_failed_job(self, job, file_handle):
        """Pretty print failure information"""
        print >> file_handle, '-' * 75
        print >> file_handle, '%s failed. Reason:' % (job, )
        if job.exception:
            print >>file_handle, '\tThrew an exception %s' % (job.exception,)
            print >>file_handle, '\tTraceback: %s' % (job.trace,)

        print >>file_handle, '\t stdout was %s' % (job.stdout,)
        print >>file_handle, '\t stderr was %s' % (job.stderr,)
        print >>file_handle, ''

    def print_running_jobs(self):
        """When the user presses enter, print the jobs that are currently being executed"""
        print 'Running jobs:'
        now = time.time()
        for job in self.running_jobs:
            print job, 'running for %i seconds' % (now - job.start_time)

    def dump_graph(self):
        """Dump the current graph in text format into logs/ppg_graph.txt if logs exists"""
        if os.path.exists('logs') and os.path.isdir('logs'):
            op = open("logs/ppg_graph.txt", 'wb')
            for job in self.jobs.values():
                op.write("%s - done: %s\n" % (job, job.is_done()))
                for preq in job.prerequisites:
                    op.write("\t%s - done: %s\n" % (preq, preq.is_done()))
                op.write("\n")
            op.close()
