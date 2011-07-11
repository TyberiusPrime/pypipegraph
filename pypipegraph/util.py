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

def start_logging(module):
    key = 'rem' if is_remote else 'ppg'
    name = "%s.%s" % (key, module)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.handlers.SocketHandler('127.0.0.1', 5005)
    logger.addHandler(handler)
    return logger


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

