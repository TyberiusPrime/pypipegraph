.. pypipegraph documentation master file, created by
   sphinx-quickstart on Mon Jan 30 14:03:54 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pypipegraph's documentation!
=======================================

This document describes the current pipegraph api as you'd use it.
Please see http://code.google.com/p/pypipegraph for the project's homepage.

.. toctree::
   :maxdepth: 2

Readme
======================================
.. include:: ../README.txt

Api documentation
===================

Core functionality
---------------------------
.. autofunction:: pypipegraph.new_pipegraph
.. autoclass:: pypipegraph.graph.Pipegraph
.. autofunction:: pypipegraph.graph.run_pipegraph

Jobs
------------------------------

Data loading jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: pypipegraph.DataLoadingJob
.. autoclass:: pypipegraph.CachedDataLoadingJob
.. autoclass:: pypipegraph.AttributeLoadingJob
.. autoclass:: pypipegraph.CachedAttributeLoadingJob

File generating jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: pypipegraph.FileGeneratingJob
.. autoclass:: pypipegraph.MultiFileGeneratingJob
.. autoclass:: pypipegraph.TempFileGeneratingJob
.. autoclass:: pypipegraph.PlotJob
.. autoclass:: pypipegraph.CombinedPlotJob

Invariant checking jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: pypipegraph.FunctionInvariant
.. autoclass:: pypipegraph.FileTimeInvariant
.. autoclass:: pypipegraph.FileChecksumInvariant
.. autoclass:: pypipegraph.ParameterInvariant

Job graph modifiying jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: pypipegraph.DependencyInjectionJob
.. autoclass:: pypipegraph.JobGeneratingJob


Exceptions:
------------------------------
A Pipegraph may throw the following exceptions:
.. autoclass:: pypipegraph.ppg_exceptions.PyPipeGraphError
.. autoclass:: pypipegraph.ppg_exceptions.CycleError
.. autoclass:: pypipegraph.ppg_exceptions.RuntimeError
.. autoclass:: pypipegraph.ppg_exceptions.RuntimeException
.. autoclass:: pypipegraph.ppg_exceptions.JobContractError
.. autoclass:: pypipegraph.ppg_exceptions.JobDiedException


Utility functions
------------------------------
.. autofunction:: pypipegraph.util.change_logging_port
.. autofunction:: pypipegraph.util.output_file_exists
Python function gotchas
-------------------------
Please keep in mind that in python functions by default bind to the name of variables in their scope, no to their values.
This means that
::
    for filename in ('A', 'B', 'C'):
       def shu():
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)
will not do what you want - you'll end up with three jobs, all writing to the same file (and the appropriate JobContractExceptions because two of them did not create their output files).
What you need to do is rebind the variable:
::
    for filename in ('A', 'B', 'C'):
       def shu(filename=filename):  #that's the magic line. Also works for lambdas
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

