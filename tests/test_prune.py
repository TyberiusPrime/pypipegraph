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

from pathlib import Path
import pytest
import pypipegraph as ppg
from .shared import write, read


@pytest.mark.usefixtures("new_pipegraph")
class TestPruning:
    def test_basic_prune(self):
        ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda: write("B", "B"))
        b.prune()
        ppg.run_pipegraph()
        assert Path("A").read_text() == "A"
        assert not Path("B").exists()

    def test_basic_prune2(self):
        a = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda: write("B", "B"))
        b.depends_on(a)
        b.prune()
        ppg.run_pipegraph()
        assert Path("A").read_text() == "A"
        assert not Path("B").exists()

    def test_basic_prune3(self):
        a = ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda: write("B", "B"))
        c = ppg.FileGeneratingJob("C", lambda: write("C", "C"))
        d = ppg.FileGeneratingJob("D", lambda: write("D", "D"))
        b.depends_on(a)
        b.prune()
        c.depends_on(b)  # that is ok, pruning happens after complet build.
        d.depends_on(a)
        ppg.run_pipegraph()
        assert Path("A").read_text() == "A"
        assert Path("D").read_text() == "D"
        assert not Path("B").exists()
        assert not Path("C").exists()
        assert c._pruned == b.job_id

    def test_pruning_does_not_prune_final_jobs(self):
        ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda: write("B", "B"))
        ppg.FinalJob("shu", lambda: write("C", "C"))
        b.prune()
        ppg.run_pipegraph()
        assert Path("A").read_text() == "A"
        assert Path("C").read_text() == "C"
        assert not Path("B").exists()

    def test_pruning_final_jobs_directly(self):
        ppg.FileGeneratingJob("A", lambda: write("A", "A"))
        ppg.FileGeneratingJob("B", lambda: write("B", "B"))
        c = ppg.FinalJob("shu", lambda: write("C", "C"))
        c.prune()
        ppg.run_pipegraph()
        assert Path("A").read_text() == "A"
        assert Path("B").read_text() == "B"
        assert not Path("C").exists()

    def test_tempfile_not_run_on_prune(self):
         a = ppg.TempFileGeneratingJob("A", lambda: write("A", "A"))
         b = ppg.FileGeneratingJob("B", lambda: write("B", "B" + read("A")))
         b.depends_on(a)
         b.prune()
         ppg.run_pipegraph()
         assert not Path('B').exists()
         assert not Path('A').exists()

    def test_tempfile_still_run_if_needed_for_other(self):
         a = ppg.TempFileGeneratingJob("A", lambda: write("A", "A"))
         b = ppg.FileGeneratingJob("B", lambda: write("B", "B" + read("A")))
         c = ppg.FileGeneratingJob("C", lambda: write("C", "C" + read("A")))
         b.depends_on(a)
         c.depends_on(a)
         b.prune()
         ppg.run_pipegraph()
         assert not Path('B').exists()
         assert Path('C').exists()
         assert Path('C').read_text() == 'CA'
         assert not Path('A').exists()

