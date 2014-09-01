modules_to_test = ['test_pypipegraph']

import os
os.environ['PYPIPEGRAPH_DO_COVERAGE'] = 'True'
import coverage
with open(".coveragerc",'wb') as op:
    op.write("""
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
    nose.core.runmodule(modules_to_test, argv=sys.argv + ['--with-progressive'], exit=False)
except Exception, e:
    print' error'
    print e
    pass
cov.stop()
cov.save()
os.system('coverage combine')
os.system('coverage html -d covhtml')
print 'coverage report is in covhtml/index.html'
