from __future__ import print_function
import cmd
import select
import sys
import util
import time

class SmartCMD(cmd.Cmd):
    def __init__(self):
        self.terminated = False
        self.stay = False
        cmd.Cmd.__init__(self)

    def cmdloop(self, intro=None):
        """Repeatedly issue a prompt, accept input, parse an initial prefix
        off the received input, and dispatch to action methods, passing them
        the remainder of the line as argument.

        """
        self.preloop()
        if self.use_rawinput and self.completekey:
            try:
                import readline
                self.old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                readline.parse_and_bind(self.completekey+": complete")
            except ImportError:
                pass
        try:
            if intro is not None:
                self.intro = intro
            if self.intro:
                self.stdout.write(str(self.intro)+"\n")
            stop = None
            while not stop:
                if self.cmdqueue:
                    line = self.cmdqueue.pop(0)
                else:
                    to =  select.select([sys.stdin], [], [], 1)[0]
                    if self.terminated:
                        return
                    if not to:
                        continue
                    if self.use_rawinput:
                        try:
                            line = raw_input(self.prompt)
                        except EOFError:
                            line = 'EOF'
                    else:
                        self.stdout.write(self.prompt)
                        self.stdout.flush()
                        line = self.stdin.readline()
                        if not len(line):
                            line = 'EOF'
                        else:
                            line = line.rstrip('\r\n')
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            self.postloop()
        finally:
            if self.use_rawinput and self.completekey:
                try:
                    import readline
                    readline.set_completer(self.old_completer)
                except ImportError:
                    pass



class GraphCmd(SmartCMD):
    """Simple command processor example."""
    prompt = '>'
    

    def do_abort(self, line):
        """Abort running pipeline"""
        if self.stay and len(util.global_pipegraph.running_jobs) == 0 and len(util.global_pipegraph.possible_execution_order) == 0:
            print ('Abort after stay - leaving')
        else:
            print ("Abort requested. Shutting down pipeline...")
            util.global_pipegraph.rc.abort()
        self.terminated = True

    def do_restart(self, line):
        """Wait till all currently running jobs have finished.
        Leave current pipegraph.
        Start again from the top
        """
        print ("restart requested. Waiting for %i running jobs to finish" % len(util.global_pipegraph.running_jobs))
        if self.stay and util.global_pipegraph.was_run:
            import subprocess
            subprocess.check_call([sys.executable] + sys.argv)
        util.global_pipegraph.possible_execution_order = []
        util.global_pipegraph.restart_afterwards = True

    def do_reboot(self, line):
        """After current pipegraph has ended, restart it
        Start again from the top
        """
        print ("restart requested. Waiting for %i running jobs to finish" % len(util.global_pipegraph.running_jobs))
        if self.stay and util.global_pipegraph.was_run:
            import subprocess
            subprocess.check_call([sys.executable] + sys.argv)
        util.global_pipegraph.restart_afterwards = Tru

    def do_stop(self, line):
        """Wait till all currently running jobs have finished.
        Leave current pipegraph.
        """
        print ("stop requested. Waiting for %i running jobs to finish" % len(util.global_pipegraph.running_jobs))
        util.global_pipegraph.possible_execution_order = []

    def do_status(self, line):
        """What's running right now"""
        print ('Running %i jobs' % len(util.global_pipegraph.running_jobs))
        print ('%i jobs remaining' % len(util.global_pipegraph.possible_execution_order))
        print ("%i jobs failed so far" % util.global_pipegraph.get_error_count())
        for ii, job in enumerate(util.global_pipegraph.running_jobs):
            print ("\t%i: %s (%.2fs)" % (job.job_no, job, time.time() - job.start_time))
            print ("")
        if self.stay:
            print ("Stay mode activated. Leave with abort")

    def do_runtimes(self, line):
        """How long did finished jobs take?"""
        by_runtime = []
        for job in util.global_pipegraph.jobs.values():
            if job.was_run and job.stop_time is not None:
                by_runtime.append((job.stop_time - job.start_time, job))
        by_runtime.sort()
        #by_runtime.reverse()
        print ('Runtimes')
        for runtime, job in by_runtime:
            print ("%.2fs %s" % (runtime, job))

    def do_next(self, line):
        """What will be started next"""
        print ("Jobs will try to start in the following order")
        for ii, job in enumerate(util.global_pipegraph.possible_execution_order):
            print ("\t%i: %s" % (job.job_no, job))


    def emptyline(self):
        return self.do_status('')

    def default(self, line):
        self.do_help(line)

    def do_stay(self, line):
        """Don't leave the interpreter when all jobs are done"""
        self.stay = True
        print ('Staying around. You will have to leave with abort at the end')

    def do_errors(self, line):
        """List failed jobs"""
        print ("Failed jobs:")
        for job in util.global_pipegraph.jobs.values():
            if job.failed:
                print ("\t%s (%s)" % (job, 'Indirect' if job.error_reason == 'Indirect'  else ('failed')))

    def do_kill(self, line):
        """Kill a specific job's process"""
        try:
            job_no = int(line)
            for job in util.global_pipegraph.running_jobs:
                print (job.job_no)
                if job.job_no == job_no:
                    print ("killing %i %s" % (job.job_no, job))
                    util.global_pipegraph.rc.kill_job(job)
                    return
            print ("Could not find that job running")
        except Exception as e:
            print (e)
            print ("Could not understand  which job to kill")


interpreter = GraphCmd()
def thread_loop():
    interpreter.cmdloop("\nPipeline now running\nType help<enter> for a list of commands")


if __name__ == '__main__':
    HelloWorld().cmdloop()
