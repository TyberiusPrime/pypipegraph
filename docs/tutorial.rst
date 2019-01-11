A slightly contrived tutorial for pypipegraph
===============================================
.. highlight:: python

Let's say we want to download a web page, save it to a file, and count the
number of characters within that web page, and write that count to file as
well. I said it was contrived ;)

The very basics
----------------
Here's the source for our first tutorial

.. literalInclude:: tutorial_1.py
    :language: python
    :linenos:

Now you might be confused why it said that 4 jobs were executed when you only
defined two.  The answer is that internally there were two FunctionInvariants
created for the callback functions, which will detect if you change the code of
either.

Go ahead, replace::

    file_handle.write(data) 

with ::

    file_handle.write(data[data.find('<body'):])

as if we only wanted to count the HTML body in the file.
You'll notice we're rerunning both jobs.
If we had instead changed ::

    count = len(file_handle.read())

to::

    data = file_handle.read()
    count = len(data[data.find("<body"):])

only the job_count FileGeneratingJob would have been rerun.

Now, the FunctionInvariant really only tracks the function source code itself.
It will not notice if you change for example the URL (jobs will get rerun if you 
change the output filename though - they're identified by it).

This is easily fixed, we'll introduce a ParameterInvariant.
Just add 
python::

    job_download.depends_on(
        pypipegraph.ParameterInvariant('url', url))

Which creates an invariant Job named 'url' (all jobs have a distinct name, a job_id so to speak),
which will force job_download (and thereby job_count) to be rerun if the url changes 
(It will also rerun next time, since the Invariant wasn't there on the last run).

Slightly more powerful
-----------------------
Now what happens if we want to download and count multiple websites?
We'll need to generate a job object for each of them. And a callback function that knows
what to download where.
Here's the code 

.. literalInclude:: tutorial_2.py
    :language: python
    :linenos:


Note that running this in the same directory as another pipegraph will clobber the job status file.

Again, the pipegraph makes sure we execute all the download jobs before calling count_characters_and_write_output.

What happens if there's an error in one of the jobs?
Extend urls to read
::

    urls = ['http://code.google.com/p/pypipegraph',
            'http://code.google.com/p/pypipegraph/w/list',
            'http://broken'
            ]

Oops, you get a bunch of error output. Once when the job died in the first place.
Then again in the end as a summary of all errors.
It reports 2 jobs failed. That's the broken download job, and the job_count - since it could not execute with it's prerequisite failing.



Full throttle
---------------

For the final example, we'll remove the restriction that we know all jobs
before calling pypipegraph.run_pipegraph(). To stay in the context of this
example, what if we were downloading the list of urls? We wouldn't want to
do that before starting the pipegraph. There might be dozens of these lists to
download, and the downloads could run very well concurrently instead of blocking all
other jobs. (By default, the pipegraph uses as many concurrent processes as
you have cores in your manchine).

We'll create a function that creates download jobs from a retrieved list of urls.
This we'll turn into a Job that generates other jobs (A)
Our counting job (B) will depend on that job (A). So (B) can only run after (A).
But (A) will introduce download Jobs (C1...Cn) on which (A) now also depends.
So the pipegraph will execute A, then C1 through Cn (up to #cores concurrently), 
then finally B.

So here's our final example

.. literalInclude:: tutorial_3.py
    :language: python
    :linenos:


