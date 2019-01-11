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
import stat as stat_module
import logging
import logging.handlers
import hashlib
import time

global_pipegraph = None
is_remote = False
job_uniquifier = {}  # to singletonize jobs on job_id
filename_collider_check = (
    {}
)  # to check wether theres' an overlap in filenames between FileGeneratingJob and MultiFileGeneratingJob
func_hashes = (
    {}
)  # to calculate invarionts on functions in a slightly more efficent manner
reactor_was_started = False

# import gc
# gc.set_debug(gc.DEBUG_UNCOLLECTABLE)


if os.environ.get("PYPIPEGRAPH_NO_LOGGING", False):  # pragma: no cover
    default_logging_handler = logging.NullHandler()
    file_logging_handler = None
else:
    default_logging_handler = logging.handlers.SocketHandler("localhost", 5005)
    if os.path.exists("logs"):
        file_logging_handler = logging.FileHandler("logs/ppg_run.txt", mode="w")
    else:  # pragma: no cover
        file_logging_handler = None
loggers = {}
file_logging_handlers = {}


class DummyLogger(object):  # pragma: no cover
    def __init__(self):
        pass

    def warn(self, *args):
        pass

    def info(self, *args):
        pass

    def debug(self, *args):
        pass

    def error(self, *args):
        pass

    def exception(self, *args):
        pass


def start_logging(module, other_file=None):  # pragma: no cover
    if os.environ.get("PYPIPEGRAPH_NO_LOGGING", False):
        return DummyLogger()
    key = "rem" if is_remote else "ppg"
    name = "%s.%s" % (key, module)
    if name not in loggers:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        if default_logging_handler:
            default_logging_handler.setFormatter(formatter)
            logger.addHandler(default_logging_handler)
        if file_logging_handler is not None:
            file_logging_handler.setFormatter(formatter)
            logger.addHandler(file_logging_handler)
        if other_file:
            if other_file not in file_logging_handlers:  # pragma: no cover
                file_logging_handlers[other_file] = logging.FileHandler(
                    os.path.join("logs", other_file), mode="w"
                )
                file_logging_handlers[other_file].setFormatter(formatter)
            logger.addHandler(file_logging_handlers[other_file])

        loggers[name] = logger
    return loggers[name]


util_logger = start_logging("util")


def change_logging_port(port):  # pragma: no cover
    """By default, a running Pipegraph chatters to localhost:5005 via tcp
    (use utils/log_listener.py to listen).
    If you want it to log to another port, use this function before createing the graph.
    """
    global default_logging_handler
    new_handler = logging.handlers.SocketHandler("127.0.0.1", port)
    for logger in list(loggers.values()):
        logger.removeHandler(default_logging_handler)
        logger.addHandler(new_handler)
    default_logging_handler = new_handler


def flush_logging():
    default_logging_handler.flush()
    if file_logging_handler:
        file_logging_handler.flush()


def output_file_exists(filename):
    """Check if a file exists and its size is > 0"""
    if not file_exists(filename):
        util_logger.info("did not exist %s" % filename)
        return False
    st = stat(filename)
    if st[stat_module.ST_SIZE] == 0:
        util_logger.info("stat size 0 %s - %s" % (os.path.abspath(filename), st))
        return False
    return True


def assert_uniqueness_of_object(
    object_with_name_attribute, pipeline=None, also_check=None
):
    """Makes certain there is only one object with this class & .name.

    This is necesarry so the pipeline jobs assign their data only to the
    objects you're actually working with."""
    if pipeline is None:
        pipeline = global_pipegraph

    if object_with_name_attribute.name.find("/") != -1:
        raise ValueError(
            "Names must not contain /, it confuses the directory calculations"
        )
    typ = object_with_name_attribute.__class__
    if typ not in pipeline.object_uniquifier:
        pipeline.object_uniquifier[typ] = {}
    if object_with_name_attribute.name in pipeline.object_uniquifier[typ]:
        raise ValueError(
            "Doublicate object: %s, %s" % (typ, object_with_name_attribute.name)
        )
    if also_check:
        if not isinstance(also_check, list):
            also_check = [also_check]
        for other_typ in also_check:
            if (
                other_typ in pipeline.object_uniquifier
                and object_with_name_attribute.name
                in pipeline.object_uniquifier[other_typ]
            ):
                raise ValueError(
                    "Doublicate object: %s, %s"
                    % (other_typ, object_with_name_attribute.name)
                )
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
            else:  # OSX: pragma: no cover
                cpu_count = int(os.popen2("sysctl -n hw.ncpu")[1].read())  # pragma: no cover
        # Windows:
        if "NUMBER_OF_PROCESSORS" in os.environ:  # pragma: no cover
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
            if ncpus > 0:
                cpu_count = ncpus
    return cpu_count


def file_exists(filename):
    return os.path.exists(filename)


def stat(filename):
    stat_cache = global_pipegraph.stat_cache
    if filename not in stat_cache:
        stat_cache[filename] = (os.stat(filename), time.time())
        return stat_cache[filename][0]
    s, t = stat_cache[filename]
    if (time.time() - t) > 1:
        stat_cache[filename] = (os.stat(filename), time.time())
    s, t = stat_cache[filename]
    return s


def job_or_filename(job_or_filename):
    """Take a filename, or a job. Return filename, dependency-for-that-file
    ie. either the job, or a FileChecksumInvariant"""
    from .job import FileGeneratingJob, FileChecksumInvariant

    if isinstance(job_or_filename, FileGeneratingJob):
        filename = job_or_filename.job_id
        deps = [job_or_filename]
    elif job_or_filename is not None:
        filename = job_or_filename
        deps = [FileChecksumInvariant(filename)]
    else:
        filename = None
        deps = []
    return filename, deps


def checksum_file(filename):
    file_size = os.stat(filename)[stat_module.ST_SIZE]
    if file_size > 200 * 1024 * 1024:  # pragma: no cover
        print("Taking md5 of large file", filename)
    with open(filename, "rb") as op:
        block_size = 1024 ** 2 * 10
        block = op.read(block_size)
        _hash = hashlib.md5()
        while block:
            _hash.update(block)
            block = op.read(block_size)
        res = _hash.hexdigest()
    print("checksuming", os.path.abspath(filename), file_size, res)
    return res
