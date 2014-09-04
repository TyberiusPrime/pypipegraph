modules_to_test = ['test_pypipegraph', 'test_plotjobs']

import os
os.environ['PYPIPEGRAPH_DO_COVERAGE'] = '/code/pypipegraph/pypipegraph/tests/.coveragerc'
import coverage
with open(".coveragerc",'wb') as op:
    op.write(b"""
[run]
data_file = /code/pypipegraph/pypipegraph/tests/.coverageX
parallel=True
[report]
include = *pypipegraph*
    """) 


cov = coverage.coverage(source = [os.path.abspath('../')], config_file = '.coveragerc', data_suffix=True)
cov.start()

import unittest
import nose
import noseprogressive
import sys
sys.path.append('/code/pypipegraph/')
import pypipegraph as ppg

try:
    nose.core.runmodule(modules_to_test, argv=sys.argv + ['--with-progressive', 
        '--nologcapture'
        ], exit=False)  #log capture get's a ton of output from the pipegraph... enable if you need it
except Exception as e:
    print ('error')
    print (e)
    pass
cov.stop()
cov.save()
os.system('coverage combine')
os.system('coverage html -d covhtml')
print ('coverage report is in covhtml/index.html')
