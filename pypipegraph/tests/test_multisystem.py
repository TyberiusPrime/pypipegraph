"""
The MIT License (MIT)

Copyright (c) 2012, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import unittest
import time
import sys
sys.path.append('../../')
import pypipegraph as ppg
logger = ppg.util.start_logging('test')
import os
import shutil
import subprocess

if False:  # these are currently not run - we don't have a working multi system implementation
    class ResourceCoordinatorTests(unittest.TestCase):
        def setUp(self):
            try:
                os.mkdir('out')
            except OSError:
                pass

        def tearDown(self):
            shutil.rmtree('out')

        def test_one_slave(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 1, 4096)])
            ppg.new_pipegraph(coordinator)
            of = 'out/a'
            job = ppg.FileGeneratingJob(of, lambda: write(of, of))
            ppg.run_pipegraph()
            self.assertEqual(read(of), of)


        def test_two_slaves(self):
            coordinator = TestResourceCoordinator(
                    [('slave_one', 1, 4096),
                    ('slave_two', 1, 4096)
                    ])
            ppg.new_pipegraph(coordinator)
            for i in xrange(0, 10):
                of = 'out/%i' % i
                job = ppg.FileGeneratingJob(of, lambda: write(of, ppg.get_slave_name()))
            ppg.run_pipegraph()
            seen = set()
            for i in xrange(0, 10):
                seen.add(read('out/%i' % i))
            self.assertEqual(seen, set(('slave_one', 'slave_two')))

        def test_two_slaves_one_blocked(self):
            coordinator = TestResourceCoordinator(
                    [('slave_one', 4, 4096),
                    ('slave_two', 0, 4096)
                    ])
            ppg.new_pipegraph(coordinator)
            for i in xrange(0, 10):
                of = 'out/%i' % i
                job = ppg.FileGeneratingJob(of, lambda: write(of, ppg.get_slave_name()))
            ppg.run_pipegraph()
            seen = set()
            for i in xrange(0, 10):
                seen.add(read('out/%i' % i))
            self.assertEqual(seen, set(('slave_one',)))

        def test_needs_all_cores(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 4, 4096)])
            ppg.new_pipegraph(coordinator)
            def get_job(of):
                def do_write():
                    write(of, "%i" % ppg.get_running_job_count())
                return ppg.FileGeneratingJob(of, do_write)
            jobA = get_job('out/A')
            jobB = get_job('out/B')
            jobC = get_job('out/C')
            jobD = get_job('out/D')
            jobD.cores_needed = -1
            ppg.run_pipegraph()
            self.assertEqual(read('out/D'), '1') #this job runs by itself.
            self.assertTrue(int(read('out/A')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/B')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/C')) <= 3 ) #the other jobs might have run in parallel

        def test_needs_multiple_cores(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 4, 4096)])
            ppg.new_pipegraph(coordinator)
            def get_job(of):
                def do_write():
                    write(of, "%i" % ppg.get_running_job_count())
                return ppg.FileGeneratingJob(of, do_write)
            jobA = get_job('out/A')
            jobB = get_job('out/B')
            jobC = get_job('out/C')
            jobD = get_job('out/D')
            jobA.cores_needed = 2 
            jobB.cores_needed = 2 
            jobD.cores_needed = 2 
            ppg.run_pipegraph()
            self.assertTrue(int(read('out/A')) <= 2 ) #no way to add 2, 2, 2, 1 to more than 4, so no more than two jobs in parallel
            self.assertTrue(int(read('out/B')) <= 2 ) 
            self.assertTrue(int(read('out/C')) <= 2 )
            self.assertTrue(int(read('out/D')) <= 2 )
            self.assertTrue(
                    (int(read('out/A')) == 2 ) or 
                    (int(read('out/B')) == 2 ) or 
                    (int(read('out/C')) == 2 ) or 
                    (int(read('out/D')) == 2 )) #make sure that at least at one time, there was multicoring ;)

        def test_needs_more_ram(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 4, 4096)])
            ppg.new_pipegraph(coordinator)
            def get_job(of):
                def do_write():
                    write(of, "%i" % ppg.get_running_job_count())
                return ppg.FileGeneratingJob(of, do_write)
            jobA = get_job('out/A')
            jobB = get_job('out/B')
            jobC = get_job('out/C')
            jobD = get_job('out/D')
            jobD.needed_memory = 3580
            ppg.run_pipegraph()
            self.assertEqual(read('out/D'), '1') #this job runs by itself.
            self.assertTrue(int(read('out/A')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/B')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/C')) <= 3 ) #the other jobs might have run in parallel

        def test_needs_too_much_ram(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 4, 4096)])
            ppg.new_pipegraph(coordinator)
            def get_job(of):
                def do_write():
                    write(of, "%i" % ppg.get_running_job_count())
                return ppg.FileGeneratingJob(of, do_write)
            jobA = get_job('out/A')
            jobB = get_job('out/B')
            jobC = get_job('out/C')
            jobD = get_job('out/D')
            jobD.needed_memory = 5580
            try:
                ppg.run_pipegraph()
                raise ValueError("should not be reached")
            except ppg.RuntimeError:
                pass         
            self.assertFalse(os.path.exists('out/D'))
            self.assertTrue(int(read('out/A')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/B')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/C')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(jobD.failed)
            self.assertTrue(jobD.error_reason.find('too much') != -1)
            self.assertFalse(jobD.exception)

        def test_needs_too_many_cores(self):
            coordinator = TestResourceCoordinator(
                    [('pinky', 4, 4096)])
            ppg.new_pipegraph(coordinator)
            def get_job(of):
                def do_write():
                    write(of, "%i" % ppg.get_running_job_count())
                return ppg.FileGeneratingJob(of, do_write)
            jobA = get_job('out/A')
            jobB = get_job('out/B')
            jobC = get_job('out/C')
            jobD = get_job('out/D')
            jobD.cores_needed = 16
            try:
                ppg.run_pipegraph()
                raise ValueError("should not be reached")
            except ppg.RuntimeError:
                pass         
            self.assertFalse(os.path.exists('out/D'))
            self.assertTrue(int(read('out/A')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/B')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(int(read('out/C')) <= 3 ) #the other jobs might have run in parallel
            self.assertTrue(jobD.failed)
            self.assertTrue(jobD.error_reason.find('too much') != -1)
            self.assertFalse(jobD.exception)



    class MessagingTests(unittest.TestCase):

        def test_large_job_descriptions(self):
            #break the 65k barrier on the AMP values...
            raise NotImplementedError()

        def test_spawn_slave_failure(self):
            raise NotImplementedError()

