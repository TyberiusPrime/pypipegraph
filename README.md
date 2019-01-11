
# pypipegraph 

| Build status: | [![Build Status](https://travis-ci.com/TyberiusPrime/pypipegraph.svg?branch=master)](https://travis-ci.com/TyberiusPrime/pypipegraph)|
|---------------|-----------------------------------------------------------------------------|
| Documentation | https://pypipegraph.readthedocs.io/en/latest/
| Code style    | ![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introduction

[pypipegraph](https://github.com/IMTMarburg/pypipegraph): is an
MIT-licensed library for constructing a workflow piece by piece and
executing just the parts of it that need to be (re-)done. It supports
using multiple cores (SMP) and (eventually, alpha code right now)
machines (cluster) and is a hybrid between a dependency tracker (think
'make') and a cluster engine.

More specifically, you construct Jobs, which encapsulate output (i.e.
stuff that needs to be done), invariants (which force re-evaluation of
output jobs if they change), and stuff inbetween (e.g. load data from
disk).

From your point of view, you create a pypipegraph, you create jobs,
chain them together, then ask the pypipegraph to run. It examines all
jobs for their need to run (either because the have not been finished,
or because they have been invalidated), distributes them across multiple
python instances, and get's them executed in a sensible order.

It is robust against jobs dying for whatever reason (only the failed job
and everything 'downstream' will be affected, independend jobs will
continue running), allows you to resume at any point 'in between' jobs,
and isolates jobs against each other.

pypipegraph supports Python 3.

## 30 second summary

```python
    pypipegraph.new_pipeline()
    output_filenameA = 'sampleA.txt'
    def do_the_work():
        op = open(output_filename, 'wb').write("hello world")
    jobA = pypipegraph.FileGeneratingJob(output_filenameA, do_the_work)
    output_filenameB = 'sampleB.txt'
    def do_the_work():
         op = open(output_filenameB, 'wb').write(open(output_filenameA, 'rb').read() + ",  once again")
    jobB = pypipegraph.FileGeneratingJob(output_filenameB, do_the_work)
    jobB.depends_on(jobA)
    pypipegraph.run()
    print 'the pipegraph is done and has returned control to you.'
    print 'sampleA.txt contains "hello world"'
    print 'sampleB.txt contains "hello world, once again"
```

# Jobs

All jobs have a unique name (job\_id), and all but invariant preserving
jobs encapsulate python callbacks to do their work.

There are four basic kinds of jobs:

 *  [Output generating jobs](). These are generally run in a fork of the
    cs, they might use multi cores (if you call an external pnon-python
    program), and they modify the outside world (create files, change
    databases, call the web).

    They do not modify the state of your program - neither on the mcp
    nor on the cs level.

 *  [Invariant preserving jobs](). These jobs are used to check that
    nothing changed. If it did, the downstream jobs need to be redone.
    Typical examples of what is checked are file modification time, file
    checksum, parameters, or changes to python functions.

    They are evaluated in the mcp and never 'ran' anywhere.

 *  [Compute slave modifying jobs](). These jobs load data into a
    compute slave that is then available (read-only) in all dependend
    output generating jobs (and it even only needs memory once thanks to
    linux forking behaviour).

    They are not run if there isn't an output job depending on them that
    needs to be done. They run single core per machine (since they must
    be run within the compute slave). They may run multiple times (on
    different machines). They modify compute slaves!

 *  [Job generating jobs](). They extend the job graph itself - either
    by creating new jobs depending on the data at hand (for example,
    create a graph for every line in a file generated by an output job),
    or by injecting further dependencies.

    They are executed in a compute slave (since they might well use data
    that was loaded by cs modifying jobs) and the jobs generated are
    transfered back to the mcp (so the mcp is still isolated from their
    execution function). **Note**: Job generating jobs are run each time
    the pipeline runs - otherwise we could not check whether the
    generated jobs have been done. That probably also means that their
    dependencies should be 'lightweight' in terms of runtime.

In addition, there are [compound jobs]() that combine jobs for
convienance.

## Invariant preserving jobs

### ParameterInvariant

Encapsulate a tuple defining some parameters to another job. If the
parameters change, dependend jobs are redone.

### FileTimeInvariant

Capture the file modification time of an input file.

### FileChecksumInvariant

Capture the file modification time, file size and the md5 sum of a file.
If the size changes, invalidate dependands. If the time changes, check
md5, if it changed, invalidate dependands. Otherwise, update internal
file modification time.

### FunctionInvariant

Compare (slightly sanitized) byte code of arbitrary python functions to
its last definition. If it changed: invalidate dependands.

## Output generating jobs

### FileGeneratingJob

This job creates an output file.

It get's rerun if the output file does not exist, if it is 0 byte in
size, or if the job has been invalidated. Not creating the output file
in its callback raises an exception.

If the job raises an exception, the output file created so far will be
removed. (optionally, if you pass rename\_broken = True when creating,
the output filename get's renamed into outputfilename + '.broken')

The job receives an implicit FunctionInvariant on its callback. This can
be supressed by calling myjob.ignore\_code\_changes()

The job\_id doubles up as the output filename.

### MultiFileGeneratingJob

Same as a [FileGeneratingJob](), just that multiple (defined) files are
checked and produced.

### TempFileGeneratingJob

This job creates a file that directly dependand jobs might access. After
those are through though, the file get's removed. It does not get
removed if any directly dependand job dies (instead of finishing ok), so
that it does not need to be redone later. It does get removed if the
TempFileGeneratingJob breaks (except if you pass rename\_broken = True
when construcing it).

## Compute slave modifying jobs

### AttributeLoadingJob

Load data (from wherever), and store it as attribute on an object. Only
occurs if there is another Job depending on it that needs o be done.
Might occur multiple times on different machines.

The attribute might disappear after all directly dependand jobs have
been run (Todo: I'm not sure how smart this idea is. On the one hand, it
allows us to clear some memory. On the other hand, it makes some things
mighty cumbersome.)

### DataLoadingJob

Similar to an [AttributeLoadingJob](), but does not do anything about
the data storage - that's your job.

DataLoadingJobs don't directly require their prerequisits to be done -
so chaining a bunch of them, and having a [FileGeneratingJob]() at
either makes all of them run (given that the [FileGeneratingJob]() is
not done) or none of them.

On the plus side, the data loaded via this job does not get
automatically eaten once all dependend jobs have been done. (Todo)

## Job generating jobs

### DependencyInjectionJob

This job injects further dependencies into jobs that already depends on
it.

An example: Say you want to extract the titles from a bunch of
downloaded websites. But the websites are not defined when first
creating jobs - you first have to download an overview page. So you
write a job that takes all .html in a folder and combines their titles
(Z). You also create a job that downloads the overview (A) . Then you
write a DependencyInjectionJob C (upon wich Z depends on) that depends
on A, creates a bunch of jobs (B1...Bn) that download the single
websites, and you return them. Z now also depends on B1..Bn and since Z
also depended on C, it could not have run before it had the additional
dependencies injected.

It is an error to add dependencies on a job that is not dependand on
this DependencyInjectionJob.

A DependencyInjectionJob has an implicit [FunctionInvariant]().

### JobGeneratingJob

This job generates new jobs that depend on the output of an earlier one.

Example: You want to draw a graph for each of the member of your sales
team. What members there are you of course only know after querying the
database for them. You write a JobGeneratingJob that queries the
database for a list of sales team members (or depends on some other job
if the action is more expensive). For each sales team member it creates
a [FileGeneratingJob]() that queries the database for this members
figures and draws the graph. The FileGeneratingJobs now can run in
parallel even on different machines...

A JobGeneratingJob has an implicit [FunctionInvariant]().

## Compound jobs

### PlotJob

This job wraps plotting with plotnine or pyggplot. It takes two functions: one
calculates the dataframe for the plot, and that result is cached. The
other one loads that dataframe and returns a pyggplot.Plot.

Both have their own [FunctionInvariant](), so you can fiddle with the
plot function without having the calculation part rerun.

The calc function must return a pandas.DataFrame, the plot function
a plotnine.ggplot or pyggplot.Plot 

### CachedJob

A CachedJob is a combination of of a AttributeLoadingJob and a
FileGeneratingJob. It has a single callback, that returns some
hard-to-compute value, which is pickled to a file (jobid doubles as
cache file name). The AttributeLoadingJob loads the data in question if
necessary.

The calc function does not get run if there are no dependencies.

It also has an implicit [FunctionInvariant]() on it's calc function
(supress just like a [FileGeneratingJob]() with ignore\_code\_changes())

## Exceptions

pypipegraph has a small set of exceptions (all descending from
PyPipelineGraphError). \* RuntimeError get's thrown by pypipegraph.run
if a job raised an exception, communication lines were broken etc \*
JobContractError is stored in a job's .exception if the job's callback
did not comply with it's requirements (e.g. a FileGeneratingJob did not
actually create the file) \* CycleError: you have fabricated a cycle in
your dependencies. Unfortunatly it's currently not reported where the
cycle is (though some simple circles are reported early on)

## Runtime

While the pypipegraph is running, you can terminate it by typing 'abort' and pressing
enter. 'help<enter>' will present a you a list of commands you can issue. 


## Executing structure

You write a 'master control program' (mcp) that creates Jobs and at one
point, you hand over control to the pypipegraph. The mcp then talks to a
resource-coordinator (either a local instance that says 'take all of
this machine' or a network service that coordinates between multiple
unning pypipegraphs) and spawns one compute slave (cs) for each machine.

Now each compute slave receives a copy of all jobs (which are just
definitions, and therefore pretty small). One by one the mcp (talking to
the resource-coordinator) asks the cs to execute jobs (while talking to
the resource-coordinater to share resources with others), collects their
feedback, prunes the graph on errors and returns control to you once all
of them have been done (or failed ;) ).

The mcp knows (thanks to the resource coordinator) about the resources
available (number of cpu cores, memory) and doesn't overload the nodes
(by spawning more processes than there are cores or by spawning too many
memory hungry jobs at once).

## Generated Files

Besides your output files, a pipegraph creates some auxillary files: 
  * ./.pypipegraph\_status\_robust - stores the invariant data of all jobs
  * ./logs/ppg\_run.txt - the chattery debug output of every decision the
    pipegraph makes (only if logs exists). All logging is also send to
    localhost 5005, and you can listen with util/log\_listener.py 
  * ./logs/ppg\_errors.txt - a log of all failed jobs and their
    exception/stdout/stderr (only if logs exists and the file is writable)
  * ./logs/ppg\_graph.txt - a dump of the connected graph structure
    (which job depends on which) (only if logs exists)

## Notes

 *  A pipegraph and it's jobs can only be run once (but you can create
    multiple pipegraphs serially).
 *  It is an error to create jobs before new\_pipegraph() has been
    called.
 *  Jobs magically associated with the currently existing pipegraph.
 *  Invariant status is kept in a magic .pypipegraph\_status file.
 *  Jobs are singletonized on their id (within the existance of one
    pipegraph). Little harm is done in defining a job multiple times.
 *  Adding jobs gives you an iterable of jobs (which depends\_on also
    takes). Adding a job and an iterable also gives you an iterable. So
    does adding an iterable and a job or an iterable and an iterable...
 *  Executing jobs (all [Output jobs]()) have resource attributes:
    cores\_needed (default 1, -1 means 'all you can get'),
    memory\_needed (default = -1, means don't worry about it, just start
    one per core, assume memory / cores. If you specify something above
    memory/core it's treated as if you need (your\_memory\_specification
    / (memory/core)) cores). memory\_needed is in bytes!
 *  Beware of passing instance functions to FunctionInvariants - if the
    job creation code is done again for a different instance, it will
    raise an exception, because the bound function from before is not
    the same function you pass in now. Pass in class.function instead of
    self.function
 *  pypipegraph is developed and tested on Ubuntu. It will not work
    reasonably on Windows - it's job model makes heavy use of fork() and
    windows process creating does not implicitly copy-on-write the
    current process' memory contents.

## Python function gotchas

Please keep in mind that in python functions by default bind to the name
of variables in their scope, no to their values. This means that :

    for filename in ('A', 'B', 'C'):
       def shu():
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)

will not do what you want - you'll end up with three jobs, all writing
to the same file (and the appropriate JobContractExceptions because two
of them did not create their output files). What you need to do is
rebind the variable:

    for filename in ('A', 'B', 'C'):
       def shu(filename=filename):  #that's the magic line. Also works for lambdas
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)

## Development notes

 *  We use pytest for testing.
