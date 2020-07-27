try:
    import cPickle as pickle
except ImportError:
    import pickle
import subprocess
import tempfile
import sys
import os


def print_usage():
    print("invariant_diff.py name_of_job_to_diff|-all [--status=prefix]")
    print(
        "Dumps the current and the last invariant for this job and"
        " then attempts a diff between them"
    )
    print(
        "--status=prefix is used if the .pypipegraph_status_robust file"
        " is named differently"
    )
    sys.exit(1)


def print_status_not_found(status_prefix):
    print("Status filename: %s  not found" % status_prefix)
    print("try passing --status=")
    sys.exit(1)


def main():  # noqa: C901

    if len(sys.argv) < 2:
        print_usage()

    default_status_prefix = ".pypipegraph_status_robust"
    status_prefix = default_status_prefix
    job_id_to_compare = None
    for x in sys.argv[1:]:
        if x.startswith("--status="):
            status_prefix = x[x.find("=") + 1 :]
        elif x.startswith("--all"):
            modus = "all"
        else:
            modus = "job"
            if job_id_to_compare:
                print_usage()
            else:
                job_id_to_compare = x

    if not os.path.exists(status_prefix):
        if status_prefix == default_status_prefix:
            for fn in sorted(os.listdir(".")):
                if fn.startswith(".ppg_status_"):
                    status_prefix = fn
                    print(
                        "Using %s as status filename,"
                        " use --status if you want another one" % status_prefix
                    )
                    break
            else:
                print_status_not_found(status_prefix)
        else:
            print_status_not_found(status_prefix)

    if modus == "job":

        def load_invariant(filename, job_id):
            with open(filename, "rb") as op:
                try:
                    while True:
                        name = pickle.load(op)
                        value = pickle.load(op)
                        if name == job_id:
                            return value
                except EOFError:
                    # raise KeyError("Job not found in %s" % filename)
                    return "job was not present"
            print(
                "Using %s as status filename, use --status if you want another one"
                % status_prefix
            )

        new = load_invariant(status_prefix, job_id_to_compare)
        old = load_invariant(status_prefix + ".old", job_id_to_compare)

        print("New:")
        print(new)
        print("")
        if old == new:
            print("Identicial")
        else:
            print("Old:")
            print(old)
            print("")
            print("Comparison")

            newf = tempfile.NamedTemporaryFile(prefix="new_")
            oldf = tempfile.NamedTemporaryFile(prefix="old_")
            newf.write(str(new).encode("utf-8"))
            oldf.write(str(old).encode("utf-8"))
            newf.flush()
            oldf.flush()
            try:
                subprocess.check_call(["which", "icdiff"])
                p = subprocess.Popen(["icdiff", newf.name, oldf.name])
            except subprocess.CalledProcessError:
                p = subprocess.Popen(["diff", newf.name, oldf.name, "-c5"])
            p.communicate()

    elif modus == "all":
        with open(status_prefix, "rb") as op:
            new = {}
            try:
                while True:
                    key = pickle.load(op)
                    value = pickle.load(op)
                    new[key] = value
            except EOFError:
                pass
        with open(status_prefix + ".old", "rb") as op:
            old = {}
            try:
                while True:
                    key = pickle.load(op)
                    value = pickle.load(op)
                    old[key] = value
            except EOFError:
                pass
        print("Jobs having a status for the first time")
        for x in sorted(set(new.keys()).difference(old.keys())):
            print(x)
        print("")
        print("jobs with changed invariants")
        for job_id in sorted(set(new.keys()).intersection(old.keys())):
            if new[job_id] != old[job_id]:
                print(job_id)


if __name__ == "__main__":
    main()
