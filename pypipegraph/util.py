import os
import stat

global_pipeline = None

def output_file_exists(filename):
    """Check if a file exists and its size is > 0"""
    if not os.path.exists(filename):
        return False
    st = os.stat(filename)
    if st[stat.ST_SIZE] == 0:
        return False
    return True

