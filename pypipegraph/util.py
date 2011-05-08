import os
import stat

global_pipegraph = None
job_uniquifier = {} #to singletonize jobs on job_id
func_hashes = {} #to calculate invarionts on functions in a slightly more efficent manner

def output_file_exists(filename):
    """Check if a file exists and its size is > 0"""
    if not os.path.exists(filename):
        return False
    st = os.stat(filename)
    if st[stat.ST_SIZE] == 0:
        return False
    return True

