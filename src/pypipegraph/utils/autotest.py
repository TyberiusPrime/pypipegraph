#!/usr/bin/python3

# run tests whenever a file changes
# smarter than pytest-xdist -f
# which monitors all files - even the test generated ones
import hashlib
import collections
import itertools
import time
import pathlib
import subprocess
import sys

cmd = ["pytest", "--durations=5"]
cmd.extend(sys.argv[1:])
hashes = collections.defaultdict(lambda: "")


def get_hash(fn, second=False):
    try:
        with open(fn, "rb") as d:
            return hashlib.md5(d.read()).hexdigest()
    except FileNotFoundError:
        if not second:
            time.sleep(1)
            return get_hash(fn, True)
        else:
            return False


known_files = None
i = 1
while True:
    rebuild = False
    if not known_files or i % 10 == 0:
        known_files = list(
            itertools.chain(
                pathlib.Path("./src").glob("**/*.py"),
                pathlib.Path("./src").glob("**/*.pyx"),
                pathlib.Path("./tests").glob("**/*.py"),
            )
        )
    if not known_files:
        known_files = list(
            itertools.chain(
                pathlib.Path("./").glob("**/*.py"), pathlib.Path("./").glob("**/*.pyx")
            )
        )

    i += 1
    for fn in known_files:
        fn = str(fn)
        new_hash = get_hash(fn)
        if hashes[fn] != new_hash:
            rebuild = True
        hashes[fn] = new_hash
    if rebuild:
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            continue
    time.sleep(0.5)
