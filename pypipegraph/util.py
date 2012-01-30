"""
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
import stat
import logging
import logging.handlers

global_pipegraph = None
is_remote = False
job_uniquifier = {}  # to singletonize jobs on job_id
func_hashes = {}  # to calculate invarionts on functions in a slightly more efficent manner
reactor_was_started = False

default_logging_handler = logging.handlers.SocketHandler('localhost', 5005)
if os.path.exists('logs'):
    file_logging_handler = logging.FileHandler("logs/ppg_run.txt", mode="w")
else:
    file_logging_handler = None
loggers = []


def start_logging(module):
    key = 'rem' if is_remote else 'ppg'
    name = "%s.%s" % (key, module)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if default_logging_handler:
        logger.addHandler(default_logging_handler)
    if file_logging_handler is not None:
        logger.addHandler(file_logging_handler)
    loggers.append(logger)
    return logger


def change_logging_port(port):
    """By default, a running Pipegraph chatters to localhost:5005 via tcp 
    (use utils/log_listener.py to listen).
    If you want it to log to another port, use this function before createing the graph.
    """
    global default_logging_handler
    new_handler = logging.handlers.SocketHandler('127.0.0.1', port)
    for logger in loggers:
        logger.removeHandler(default_logging_handler)
        logger.addHandler(new_handler)
    default_logging_handler = new_handler


def flush_logging():
    default_logging_handler.flush()
    if file_logging_handler:
        file_logging_handler.flush()


def output_file_exists(filename):
    """Check if a file exists and its size is > 0"""
    if not os.path.exists(filename):
        return False
    st = os.stat(filename)
    if st[stat.ST_SIZE] == 0:
        return False
    return True


class NothingChanged(Exception):
    """For Invariant communication where
    the invariant value changed, but we don't need to invalidate
    the jobs because of it (and we also want the stored value to be updated).

    This is necessary for the FileChecksumInvariant, the filetime might change,
    then we need to check the checksum. If the checksum matches, we need
    a way to tell the Pipegraph to store the new (filetime, filesize, checksum)
    tuple, without invalidating the jobs.
    """

    def __init__(self, new_value):
        self.new_value = new_value


def assert_uniqueness_of_object(object_with_name_attribute, pipeline=None):
    """Makes certain there is only one object with this class & .name.

    This is necesarry so the pipeline jobs assign their data only to the
    objects you're actually working with."""
    if pipeline is None:
        pipeline = global_pipegraph

    if object_with_name_attribute.name.find('/') != -1:
        raise ValueError("Names must not contain /, it confuses the directory calculations")
    typ = object_with_name_attribute.__class__
    if not typ in pipeline.object_uniquifier:
        pipeline.object_uniquifier[typ] = {}
    if object_with_name_attribute.name in pipeline.object_uniquifier[typ]:
        raise ValueError("Doublicate object: %s, %s" % (typ, object_with_name_attribute.name))
    object_with_name_attribute.unique_id = len(pipeline.object_uniquifier[typ])
    pipeline.object_uniquifier[typ][object_with_name_attribute.name] = True


cpu_count = 0


def CPUs():
    """
    Detects the number of CPUs on a system. Cribbed from pp.
    """
    global cpu_count
    if cpu_count == 0:
        cpu_count = 1  # default
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    cpu_count = ncpus
            else:  # OSX:
                cpu_count = int(os.popen2("sysctl -n hw.ncpu")[1].read())
         # Windows:
        if "NUMBER_OF_PROCESSORS" in os.environ:
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
            if ncpus > 0:
                cpu_count = ncpus
    return cpu_count
