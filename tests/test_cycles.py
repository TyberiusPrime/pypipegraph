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

    def test_exceeding_max_cycle(self):
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

        ppg.new_pipegraph(quiet=True, dump_graph=False)
        jobs = []
        for x in range(0, max_depth + 10):
            j = ppg.FileGeneratingJob(str(x), lambda: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        def inner():
            ppg.run_pipegraph()

        assertRaises(ppg.CycleError, inner)

    @pytest.mark.xfail
    def test_prioritize_simple(self):
        raise NotImplementedError()
        jobA = ppg.Job("A")
        jobB = ppg.Job("B")
        jobA.depends_on(jobB)
        jobC = ppg.Job("C")
        jobD = ppg.Job("D")
        jobC.depends_on(jobD)
        ppg.util.global_pipegraph.connect_graph()
        ppg.util.global_pipegraph.check_cycles()
        assert jobD in ppg.util.global_pipegraph.possible_execution_order
        if jobD == ppg.util.global_pipegraph.possible_execution_order[0]:
            to_prio = jobB
        else:
            to_prio = jobD
        assert not (to_prio == ppg.util.global_pipegraph.possible_execution_order[0])
        ppg.util.global_pipegraph.prioritize(to_prio)

        assert to_prio == ppg.util.global_pipegraph.possible_execution_order[0]

        ppg.util.global_pipegraph.prioritize(jobB)
        print("after prio b")
        for x in ppg.util.global_pipegraph.possible_execution_order:
            print(x.job_id)

        assert jobB == ppg.util.global_pipegraph.possible_execution_order[0]
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)
        ppg.util.global_pipegraph.prioritize(jobC)
        print("after prio c")
        for x in ppg.util.global_pipegraph.possible_execution_order:
            print(x.job_id)

        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobC
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobD)

        assert jobD == ppg.util.global_pipegraph.possible_execution_order[0]
        assert jobC == ppg.util.global_pipegraph.possible_execution_order[1]

        ppg.util.global_pipegraph.prioritize(jobB)
        assert jobB == ppg.util.global_pipegraph.possible_execution_order[0]
        assert ppg.util.global_pipegraph.possible_execution_order.index(
            jobA
        ) > ppg.util.global_pipegraph.possible_execution_order.index(jobB)

    def test_prioritize_raises_on_done_job(self):
        def dump():
            pass

        ppg.FileGeneratingJob("out/A", dump)
        jobB = ppg.FileGeneratingJob("out/B", dump)
        jobB.ignore_code_changes()
        with open("out/B", "wb") as op:
            op.write(b"Done")
        ppg.util.global_pipegraph.connect_graph()
        ppg.util.global_pipegraph.check_cycles()
        ppg.util.global_pipegraph.load_invariant_status()
        ppg.util.global_pipegraph.distribute_invariant_changes()
        ppg.util.global_pipegraph.dump_invariant_status()  # the jobs will have removed their output, so we can safely store the invariant data
        ppg.util.global_pipegraph.build_todo_list()

        def inner():
            ppg.util.global_pipegraph.prioritize(jobB)

        assertRaises(ValueError, inner)
