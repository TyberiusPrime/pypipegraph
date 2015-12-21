import time
import os
import sys
sys.path.append('../../')
import pypipegraph as ppg

ppg.new_pipegraph()


def run_long():
    time.sleep(20)
    raise ValueError()

def run_short():
    time.sleep(5)
    with open("short.dat","wb") as op:
        op.write("DONO")

if os.path.exists('short.dat'):
    os.unlink('short.dat')
if os.path.exists('long.dat'):
    os.unlink('long.dat')


job1 = ppg.FileGeneratingJob('long.dat', run_long)
job2 =  ppg.FileGeneratingJob('short.dat', run_short)
job1.depends_on(job2)

ppg.run_pipegraph()

