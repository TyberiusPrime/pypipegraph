import os
import stat
import logging
import logging.handlers
from twisted.internet import reactor

global_pipegraph = None
is_remote = False
job_uniquifier = {} #to singletonize jobs on job_id
func_hashes = {} #to calculate invarionts on functions in a slightly more efficent manner
reactor_was_started = False

default_logging_handler = logging.handlers.SocketHandler('127.0.0.1', 5005)
loggers = []

def start_logging(module):
    key = 'rem' if is_remote else 'ppg'
    name = "%s.%s" % (key, module)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(default_logging_handler)
    loggers.append(logger)
    return logger

def change_logging_port(port):
    global default_logging_handler
    new_handler = logging.handlers.SocketHandler('127.0.0.1', port)
    for logger in loggers:
        logger.removeHandler(default_logging_handler)
        logger.addHandler(new_handler)
    default_logging_handler = new_handler



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

def assert_uniqueness_of_object(object_with_name_attribute, pipeline = None):
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
        cpu_count = 1 #default
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    cpu_count = ncpus
            else: # OSX:
                cpu_count = int(os.popen2("sysctl -n hw.ncpu")[1].read())
         # Windows:
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
            if ncpus > 0:
                cpu_count = ncpus
    return cpu_count

