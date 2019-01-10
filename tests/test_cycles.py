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

import pytest
import pypipegraph as ppg
from .shared import write, assertRaises


@pytest.mark.usefixtures("new_pipegraph")
class TestCycles:
    def test_simple_cycle(self):
        def inner():
            ppg.new_pipegraph(quiet=True, dump_graph=False)
            jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
            jobB = ppg.FileGeneratingJob("A", lambda: write("B", "A"))
            jobA.depends_on(jobB)
            jobB.depends_on(jobA)
            # ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    def test_indirect_cicle(self):
        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobA = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda: write("B", "A"))
        jobC = ppg.FileGeneratingJob("C", lambda: write("C", "A"))
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        jobA.depends_on(jobC)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    def test_exceeding_max_cycle(self, new_pipegraph):
        max_depth = 50
        # this raisess...
        jobs = []
        for x in range(0, max_depth - 1):
            j = ppg.FileGeneratingJob(str(x), lambda: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

        new_pipegraph.new_pipegraph()
        jobs = []
        for x in range(0, max_depth + 100):
            j = ppg.FileGeneratingJob(str(x), lambda: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        with pytest.raises(ppg.CycleError):
            ppg.run_pipegraph()
