from multiprocessing import util
import coverage
import unittest
import os
import nose
import noseprogressive
import sys

coverage.process_startup()
cov = coverage.coverage(source = [os.path.abspath('../')])
cov.start()
import test_pypipegraph
try:
    nose.core.runmodule('test_pypipegraph', argv=sys.argv + ['--with-progressive'], exit=False)
except Exception, e:
    print e
    pass
print 'now doing coverage report'
cov.stop()
cov.html_report(directory='covhtml')
print 'done, result in covhtml/index.html'
