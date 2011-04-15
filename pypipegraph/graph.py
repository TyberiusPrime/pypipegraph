import os
import collections
import resource_coordinators
import cPickle
import jobs
import exceptions
import util

invariant_status_filename= '.pypipegraph_status'

def run_pipegraph():
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    util.global_pipegraph.run()


def new_pipegraph(resource_coordinator = None):
    if resource_coordinator is None:
        resource_coordinator = resource_coordinators.LocalSystem()
    util.global_pipegraph = Pipegraph(resource_coordinator)
    jobs.job_uniquifier = {}
    jobs.func_hashes = {}

def forget_job_status(self):
    try:
        os.unlink(invariant_status_filename)
    except OSError:
        pass

def get_running_job_count(self):
    return util.global_pipegraph.get_running_job_count()


class Pipegraph(object):

    def __init__(self, resource_coordinator):
        self.rc = resource_coordinator
        self.jobs = set()
        self.running = False

    def add_job(self, job):
        if not self.running:
            self.jobs[job.job_id] = job
        else:
            if not job.job_id in self.jobs:
                self.new_jobs[job.job_id] = job


    def run(self):
        self.running = True
        self.new_jobs = set()
        self.connect_graph()
        self.check_cycles()
        self.load_invariant_status()
        self.distribute_invariant_changes()
        self.build_todo_list()

        self.spawn_slaves()
        self.execute_jobs()

        self.dump_html_status()
        self.dump_invariant_status()
        self.destroy_job_connections()

        for job in self.jobs.values():
            if job.failed:
                raise exceptions.RuntimeError()

    def connect_graph(self):
        """Convert the dependency graph in jobs into a bidirectional graph"""
        for job in self.jobs.values:
            for preq in job.prerequisites:
                preq.dependants.add(job)

        def destroy_job_connections(self):
            """Delete connections between jobs for gc purposes"""
            for job in self.jobs.values():
                job.dependants = None
                job.prequisites = None

    def _checkCycles(self):
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
        cPickle.dump(op, self.invariant_status)
        op.close()

    def distribute_invariant_changes(self):
        """check each job for whether it's invariance has changed,
        and propagate the invalidation by calling job.invalidated()"""
        for job in self.jobs.values():
            old = self.invariant_status[job.job_id]
            inv = job.get_invariant(old)
            if inv != old:
                job.invalidated()

    def build_todo_list(self):
        """Go through each job. If it needs to be done, invalidate() all dependands.
        also requires all prequisites to require_loading
        """
        for job in self.jobs.values():
            job.inject_auto_invariants()
        needs_to_be_run = set()
        for job in self.jobs.values():
            if not job.is_done():
                needs_to_be_run.add(job.job_id)
                job.invalidated()
                #for preq in job.prerequisites:
                    #preq.require_loading() #think I can get away with  lettinng the slaves what they need to execute a given job...

        for job in self.jobs.values():
            if job.was_invalidated():
                needs_to_be_run.add(job.job_id)
        #now prune the possible_execution_order
        self.possible_execution_order = [job for job in self.possible_execution_order if job.job_id in needs_to_be_run]

    def spawn_slaves(self):
        self.slaves = self.rc.spawn_slaves()
        self.check_all_jobs_can_be_executed()

    def check_all_jobs_can_be_executed(self):
        resources = self.rc.get_resources() # a dict of slave name > {cores: y, memory: x}
        maximal_memory = max([x['memory'] for x in resources.values()])
        maximal_cores = max([x['cores'] for x in resources.values()])

        for job in self.possible_execution_order:
            if job.cores_needed > maximal_cores or job.memory_needed > maximal_memory:
                self.prune_job(job)
                job.error_reason = "Needed to much memory/cores"

    def execute_jobs(self):
        self.jobs_by_slave = {}
        while (self.possible_execution_order): #while we have jobs that need to be taken care of...
            self.start_jobs()
            self.block_till_a_job_returned()


    def start_jobs(self):
        #first, check what we actually have as resources...
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
        def resources_available(resources):
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    return slave
            return False
        to_remove = []
        while resources_available(resources):
            for slave in resources:
                if resources[slave]['cores'] > 0 and resources[slave]['memory'] > 0:
                    next_job = 0
                    while next_job < len(self.possible_execution_order):
                        job = self.possible_execution_order[next_job]
                        if job.can_run_now():
                            #Todo: Keep track of which dataloadingjobs have already been performed on each node
                            #and prioritize by that...
                            if (job.cores_needed == -1 and resources[slave]['cores'] == resources[slave['total cores']]
                                and (job.memory_needed == -1 or job.memory_needed < resources[slave]['memory'])):
                                    self.slaves[slave].spawn(job)
                                    self.running_jobs.append(job, slave)
                                    to_remove.append(job)
                                    resources[slave]['cores'] = 0
                                    #don't worry about memory...
                                    continue
                            elif (job.cores_needed < resources[slave]['cores'] 
                                and (job.memory_needed == -1 or job.memory_needed < resources[slave]['memory'])):
                                self.slaves[slave].spawn(job)
                                self.running_jobs.append(job, slave)
                                to_remove.append(job)
                                resources[slave]['cores'] -= job.cores_needed
                                if job.memory_needed == -1:
                                    resources[slave]['memory'] -= resources[slave]['memory/core']
                                else:
                                    resources[slave]['memory'] -= job.memory_needed
                                    continue
                        #this job needed to much resources, or was not runnable
                        next_job += 1

        for job in to_remove:
            self.possible_execution_order.remove(job) #certain we could do better than with a list...
             
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
        if job.failed:
            self.prune_job(job)
            if job.exception:
                job.error_reason = "Exception"
            else:
                job.error_reason = "Unknown/died"
        else:
            job.was_ran = True
            job.check_prerequisites_for_cleanup()
        self.running_jobs.remove(job)
        self.signal_job_done()


    def dump_html_status(self):
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

