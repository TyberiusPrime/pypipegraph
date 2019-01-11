
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

pypipegraph supports Python 3 only.

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
    print('the pipegraph is done and has returned control to you.')
    print('sampleA.txt contains "hello world"')
    print('sampleB.txt contains "hello world, once again")
```
