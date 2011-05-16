import os
import collections
import resource_coordinators
import cPickle
import exceptions
import util
import logging
logging.basicConfig(level=logging.INFO)

invariant_status_filename= '.pypipegraph_status'

def run_pipegraph():
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    util.global_pipegraph.run()


def new_pipegraph(resource_coordinator = None):
    if resource_coordinator is None:
        resource_coordinator = resource_coordinators.LocalSystem()
    util.global_pipegraph = Pipegraph(resource_coordinator)
    util.job_uniquifier = {}
    util.func_hashes = {}
    logging.info("\n\n")
    logging.info("New Pipegraph")

def forget_job_status():
    try:
        os.unlink(invariant_status_filename)
    except OSError:
        pass

def destroy_global_pipegraph():
    util.global_pipegraph = None

def get_running_job_count():
    return util.global_pipegraph.get_running_job_count()


class Pipegraph(object):

    def __init__(self, resource_coordinator):
        self.rc = resource_coordinator
        self.rc.pipegraph = self
        self.jobs = {}
        self.running = False
        self.was_run = False
        self.new_jobs = False

    def __del__(self):
        self.rc.pipegraph = None

    def add_job(self, job):
        logging.info("Adding job %s" % job)
        if not self.running:
            if self.was_run:
                raise ValueError("This pipegraph was already run. You need to create a new one for more jobs")
            self.jobs[job.job_id] = job
        else:
            if self.new_jobs is False:
                raise ValueError("Trying to add new jobs to running pipeline without having new_jobs set (ie. outside of a graph modifying job)")
            if not job.job_id in self.jobs:
                logging.info("Adding job to new_jobs %s %s" % (job, id(self.new_jobs)))
                self.new_jobs[job.job_id] = job
            else:
                logging.info("Already knew the job %s, not keeping it as new" % job)


    def run(self):
        if self.was_run:
            raise ValueError("Each pipegraph may be run only once.")
        logging.info("MCP pid: %i" % os.getpid())
        #internal to the mcp
        self.inject_auto_invariants()
        self.was_run = True #since build_

        self.running = True
        self.new_jobs = False #this get's changed in graph modifying jobs, but they reset it to false, which means 'don't accept any new jobs while we are running'
        self.connect_graph()
        self.check_cycles()
        self.load_invariant_status()
        self.distribute_invariant_changes()
        self.build_todo_list()

        #make us some computational engines and put them to work.
        logging.info("now executing")
        self.spawn_slaves()
        self.execute_jobs()

        #clean up
        self.dump_html_status()
        self.dump_invariant_status()
        self.destroy_job_connections()

        #and propagate if there was an exception
        for job in self.jobs.values():
            if job.failed:
                raise exceptions.RuntimeError()

    def inject_auto_invariants(self):
        """Go through each job and ask it to create the invariants it might need.
        Why is this not done by the jobs on init? So the user can prevent them from doing it (job.do_ignore_code_changes()...)"""
        for job in self.jobs.values():
            job.inject_auto_invariants()


    def connect_graph(self):
        """Convert the dependency graph in jobs into a bidirectional graph"""
        for job in self.jobs.values():
            for preq in job.prerequisites:
                preq.dependants.add(job)

    def destroy_job_connections(self):
        """Delete connections between jobs for gc purposes"""
        for job in self.jobs.values():
            job.dependants = None
            job.prequisites = None

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
        if has_edges:
            raise exceptions.CycleError("A cycle in the graph was detected")
        L.reverse()
        self.possible_execution_order = L

    def load_invariant_status(self):
        if os.path.exists(invariant_status_filename):
            op = open(invariant_status_filename, 'rb')
            self.invariant_status = cPickle.load(op)
            op.close()
        else:
            self.invariant_status = collections.defaultdict(bool)

    def dump_invariant_status(self):
        op = open(invariant_status_filename, 'wb')
        cPickle.dump(self.invariant_status, op)
        op.close()

    def distribute_invariant_changes(self):
        """check each job for whether it's invariance has changed,
        and propagate the invalidation by calling job.invalidated()"""

        for job in self.jobs.values():
            old = self.invariant_status[job.job_id]
            try:
                inv = job.get_invariant(old)
            except util.NothingChanged, e:
                logging.info("Invariant difference, but NothingChanged")
                inv = e.new_value
                old = inv #so no change...
            if inv != old:
                logging.info("Invariant change for %s" % job)
                job.invalidated()
                self.invariant_status[job.job_id] = inv # for now, it is the dependant job's job to clean up so they get reinvalidated if the executing is terminated before they are reubild (ie. filegenjobs delete their outputfiles)

    def build_todo_list(self):
        """Go through each job. If it needs to be done, invalidate() all dependands.
        also requires all prequisites to require_loading
        """
        needs_to_be_run = set()
        for job in self.jobs.values():
            if not job.is_done():
                if not job.is_loadable():
                    needs_to_be_run.add(job.job_id)
                    job.invalidated()
                #for preq in job.prerequisites:
                    #preq.require_loading() #think I can get away with  lettinng the slaves what they need to execute a given job...
            else:
                logging.info("was done %s. Invalidation status: %s" % (job, job.was_invalidated))

        for job in self.jobs.values():
            if ( job.was_invalidated #this has been invalidated
                and job.runs_in_slave #it is not one of the invariantes
                and not job.is_loadable #and it is not a loading job (these the slaves do automagically for now)
                ):
                needs_to_be_run.add(job.job_id)
            elif (not job.runs_in_slave()):
                job.was_run = True #invarites get marked as ran..
        #now prune the possible_execution_order
        self.possible_execution_order = [job for job in self.possible_execution_order if job.job_id in needs_to_be_run]
        logging.info(" possible execution order %s" % [str(x) for x in self.possible_execution_order])

    def spawn_slaves(self):
        logging.info("Spawning slaves")
        self.slaves = self.rc.spawn_slaves()
        self.check_all_jobs_can_be_executed()

    def check_all_jobs_can_be_executed(self):
        """Check all jobs for memory/cpu requirements and prune those that we can't satisfy"""
        resources = self.rc.get_resources() # a dict of slave name > {cores: y, memory: x}
        maximal_memory = max([x['memory'] for x in resources.values()])
        maximal_cores = max([x['cores'] for x in resources.values()])

        for job in self.possible_execution_order:
            if job.cores_needed > maximal_cores or job.memory_needed > maximal_memory:
                self.prune_job(job)
                job.error_reason = "Needed to much memory/cores"
                job.failed = True

    def execute_jobs(self):
        """Pass control to ResourceCoordinator"""
        logging.info("Executing jobs/passing control to RC")
        self.jobs_by_slave = {}
        #the rc loop externalizes the start_jobs / job_executed, start more jobs
        self.running_jobs = set()
        if self.possible_execution_order: #no jobs, no spawning...
            self.rc.enter_loop() #doesn't return until all jobs have been done.
        logging.info("Control returned from ResourceCoordinator")


    def start_jobs(self): #I really don't like this function... and I also have the strong inkling it should acttually sit in the resource coordinatora
        #first, check what we actually have some resources...
        resources = self.rc.get_resources() # a dict of slave name > {cores: y, memory: x}
        for slave in resources:
            resources[slave]['memory/core'] = resources[slave]['memory'] / resources[slave]['cores']
            resources[slave]['total cores'] = resources[slave]['cores']
        for slave in self.jobs_by_slave:
            for job in self.jobs_by_slave[slave]:
                if job.cores_needed == -1:
                    resources[slave][0] = 0
                else:
                    resources[slave][0] -= 1 
                if job.memory_needed == -1:
                    resources[slave]['memory'] -= resources[slave]['memory/core']
                else:
                    resources[slave]['memory'] -= job.memory_needed
        logging.info("Resources: %s" % resources)
        def resources_available(resources):
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    return True
            return False
        to_remove = []
        error_count = len(self.jobs) * 2
        runnable_jobs = [job for job in self.possible_execution_order if job.can_run_now()]
        if self.possible_execution_order and not runnable_jobs and not self.running_jobs:
            raise exceptions.RuntimeException(
            """We had more jobs that needed to be done, none of them could be run right now and none were running. Sounds like a dependency graph bug to me""")
        while resources_available(resources) and runnable_jobs:
            logging.info("resources were available")
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    next_job = 0
                    logging.info('remaining %i jobs'% len(self.possible_execution_order))
                    #while next_job < len(self.possible_execution_order): #todo: restrict to runnable_jobs
                    while next_job < len(runnable_jobs):
                        job = runnable_jobs[next_job]
                        if job.can_run_now():
                            #Todo: Keep track of which dataloadingjobs have already been performed on each node
                            #and prioritize by that...
                            if (job.cores_needed == -1 and resources[slave]['cores'] == resources[slave['total cores']]
                                and (job.memory_needed == -1 or job.memory_needed < resources[slave]['memory'])):
                                    self.slaves[slave].spawn(job)
                                    self.running_jobs.add(job)
                                    to_remove.append(job)
                                    resources[slave]['cores'] = 0
                                    #don't worry about memory...
                                    continue
                            elif (job.cores_needed < resources[slave]['cores'] 
                                and (job.memory_needed == -1 or job.memory_needed < resources[slave]['memory'])):
                                self.slaves[slave].spawn(job)
                                self.running_jobs.add(job)
                                to_remove.append(job)
                                resources[slave]['cores'] -= job.cores_needed
                                if job.memory_needed == -1:
                                    resources[slave]['memory'] -= resources[slave]['memory/core']
                                else:
                                    resources[slave]['memory'] -= job.memory_needed
                                    continue
                            else:
                                #this job needed to much resources, or was not runnable
                                logging.info("Job needed too many resources %s" % job)
                                runnable_jobs.remove(job)
                        next_job += 1
                for job in to_remove:
                    logging.info("removing job %s" % job)
                    self.possible_execution_order.remove(job) #certain we could do better than with a list...
                    runnable_jobs.remove(job)
            error_count -= 1
            if error_count == 0:
                raise ValueError("There was a loop error that should never 've been reached in start_jobs")
        logging.info("can't start any more jobs. either no more there, or resources all utilized")

             
    def prune_job(self, job):
        try:
            self.possible_execution_order.remove(job)
        except ValueError: # might occur sereval times when following the graph...
            pass
        job.failed = True
        job.error_reason = "Indirect"
        for dep in job.dependants:
            self.prune_job(dep)


    def job_executed(self, job):
        """A job was done. Return whether there are more jobs read run""" 
        logging.info("job_executed %s" % job)
        if job.failed:
            self.prune_job(job)
            if job.exception:
                job.error_reason = "Exception"
            else:
                job.error_reason = "Unknown/died"
        else:
            job.was_run = True
            job.check_prerequisites_for_cleanup()
        self.running_jobs.remove(job)
        #self.signal_job_done()
        return bool(self.running_jobs) or bool(self.possible_execution_order)

    def new_jobs_generated_during_runtime(self, new_jobs):
        """Received jobs from one of the generators. 
        We'll integrate them into the graph, and add them to the possible_execution_order.
        Beauty of the job-singeltonization is that all dependands that are not new"""
        logging.info('new_jobs_generated_during_runtime')
        def check_preqs(job):
            for preq_job_id in job.prerequisites:
                if not preq_job_id in self.jobs and not preq_job_id in new_jobs:
                    raise ValueError("New job dependen on job that is not in the job list but also not in the new jobs")
            for dep_job_id in job.dependants:
                if not dep_job_id in self.jobs and not dep_job_id in new_jobs:
                    raise ValueError("New job was depedency for is not in the job list but also not in the new jobs")
                    #the case that it is injected as a dependency for a job that might have already been done
                    #is being taken care of in the JobGeneratingJob and DependencyInjectionJob s
        for job in new_jobs.values():
            print job
            print job.prerequisites
            check_preqs(job)
            job.prequisites = set([self.jobs[job_id] for job_id in job.prerequisites])
            job.dependants = set([self.jobs[job_id] for job_id in job.dependants])
            self.jobs[job.job_id] = job
        for job in new_jobs.values():
            if not job.is_done():
                job.invalidated()
        for job in new_jobs.values():
            if job.is_loadable():
                logging.info("Ignoring %s" % job)
            elif not job.is_done():
                logging.info("Adding %s to possible_execution_order"%  job)
                self.possible_execution_order.append(job)
            else:
                logging.info("Not doing anything with"%  job)
        self.connect_graph()
        #for slave in self.slaves.values():
            #slave.transmit_new_jobs(new_jobs)
           





    def dump_html_status(self):
        if not os.path.exists('logs'):
            os.mkdir('logs')
        op = open("logs/pipeline_status.html",'wb')
        for job in self.jobs.values():
            if job.failed:
                op.write("<p style='color:red'>")
                op.write(job.job_id + """
                Status: %s
                <br />
                Stdout: %s
                <br />
                Stderr: %s
                <br />
                Exception: %s""" % (job.error_reason, job.stdout, job.stderr, job.exception))
            else:
                op.write("<p>")
                op.write(job.job_id + " was ok")
            op.write("</p>")
        op.close()

