import cPickle as pickle
import subprocess
import tempfile
import sys


def print_usage():
    print("invariant_diff.py name_of_job_to_diff")
    print("Dumps the current and the last invariant for this job and then attempts a diff between them")
    sys.exit(1)

if len(sys.argv) != 2:
    print_usage()



def load_invariant(filename, job_id):
    with open(filename) as op:
        try:
            while True:
                name = pickle.load(op)
                value = pickle.load(op)
                if name == job_id:
                    return value
        except pickle.EOFError:
            pass
    raise KeyError("Job not found in %s" % filename)

job_id_to_compare = sys.argv[1]
new = load_invariant('.pypipegraph_status_robust', job_id_to_compare)
old = load_invariant('.pypipegraph_status_robust.old', job_id_to_compare)

print 'New:'
print new
print ''
print 'Old:'
print old

print 'Comparison'
if old == new:
    print 'Identicial'
else:
    newf = tempfile.NamedTemporaryFile(prefix='new_')
    oldf = tempfile.NamedTemporaryFile(prefix='old_')
    newf.write(str(new))
    old.write(str(old))
    newf.flush()
    oldf.flush()
    p = subprocess.Popen(['diff', newf.filename, oldf.filename])
    p.communicate()
