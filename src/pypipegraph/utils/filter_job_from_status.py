import pickle
import re
import shutil
import sys
import os


# todo: script, entry point, clickify...


def print_usage():
    print("filter_job_from_status.py name_of_job_to_filter [--do-it]")
    print(
        "Removes a job from the status file\n"
        "Default is to just print maching jobs, pass --do-it to really filter them"
    )
    print(
        "--status=prefix is used if the .pypipegraph_status_robust file"
        " is named differently"
    )
    sys.exit(1)


def print_status_not_found():
    print("Status filename: %s  not found" % status_prefix)
    print("try passing --status=")
    sys.exit(1)


if len(sys.argv) < 2:
    print_usage()

default_status_prefix = ".pypipegraph_status_robust"
status_prefix = default_status_prefix
job_id_to_compare = None
dry_run = True
modus = "job"
for x in sys.argv[1:]:
    if x.startswith("--status="):
        status_prefix = x[x.find("=") + 1 :]
    elif x.startswith("--regex"):
        mode = "regexps"
    elif x == "--do-it":
        dry_run = False
    else:
        job_id_to_compare = x

if not os.path.exists(status_prefix):
    if status_prefix == default_status_prefix:
        for fn in sorted(os.listdir(".")):
            if (
                fn.startswith(".ppg_status_")
                and ".backup" not in fn
                and ".old" not in fn
            ):
                status_prefix = fn
                print(
                    "Using %s as status filename,"
                    " use --status if you want another one" % status_prefix
                )
                break
        else:
            print_status_not_found()
    else:
        print_status_not_found()

filename = status_prefix
shutil.move(filename, filename + ".backup")
count = 0
print("hunting for", job_id_to_compare)
if not dry_run:
    op_out = open(filename, "wb")
else:
    op_out = None
with open(filename + ".backup", "rb") as op_in:
    try:
        while True:
            name = pickle.load(op_in)
            value = pickle.load(op_in)
            if mode == "job" and name == job_id_to_compare:
                count += 1
                print("found job", name)
                continue
            elif mode == "regexps":
                # print(name)
                if re.search(job_id_to_compare, name):
                    print("found job", name)
                    count += 1
                    continue
            if not dry_run:
                pickle.dump(name, op_out, pickle.HIGHEST_PROTOCOL)
                pickle.dump(value, op_out, pickle.HIGHEST_PROTOCOL)
    except EOFError:
        pass
if op_out is not None:
    op_out.close()

if count:
    print("done - see above for filtered jobs")
else:
    print("no jobs matched your criteria")
