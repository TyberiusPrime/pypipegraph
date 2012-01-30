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

   tutorial
   api

Readme
======================================
.. include:: ../README.txt


Python function gotchas
-------------------------
Please keep in mind that in python functions by default bind to the name of variables in their scope, no to their values.
This means that ::

    for filename in ('A', 'B', 'C'):
       def shu():
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)

will not do what you want - you'll end up with three jobs, all writing to the same file (and the appropriate JobContractExceptions because two of them did not create their output files).
What you need to do is rebind the variable::

    for filename in ('A', 'B', 'C'):
       def shu(filename=filename):  #that's the magic line. Also works for lambdas
           write_to_file(filename=filename, text='hello world')
       job = pypipegraph.FileGeneratingJob(i, shu)



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

