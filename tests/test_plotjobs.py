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
from .shared import append, write, read
import unittest
import os

try:
    import pyggplot

    has_pyggplot = True
except ImportError:
    has_pyggplot = False
    pass


if has_pyggplot:  # noqa C901
    # import R
    import pandas as pd
    import pypipegraph as ppg
    import subprocess

    def magic(filename):
        """See what linux 'file' commando says about that file"""
        if not os.path.exists(filename):
            raise OSError("Does not exists %s" % filename)
        p = subprocess.Popen(["file", filename], stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return stdout

    @pytest.mark.usefixtures("new_pipegraph")
    class TestPlotJob:
        def test_basic(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1

        def test_pdf(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.pdf"
            ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PDF document") != -1

        def test_raises_on_invalid_filename(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.shu"

            def inner():
                ppg.PlotJob(of, calc, plot)

            with pytest.raises(ValueError):
                inner()

        def test_reruns_just_plot_if_plot_changed(self, new_pipegraph):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            new_pipegraph.new_pipegraph()

            def plot2(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("Y", "X")

            ppg.PlotJob(of, calc, plot2)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "BB"

        def test_no_rerun_if_ignore_code_changes_and_plot_changes(self, new_pipegraph):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            job = ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            new_pipegraph.new_pipegraph()

            def plot2(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("Y", "X")

            job = ppg.PlotJob(of, calc, plot2)
            job.ignore_code_changes()
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

        def test_reruns_both_if_calc_changed(self, new_pipegraph):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            new_pipegraph.new_pipegraph()

            def calc2():
                append("out/calc", "A")
                x = 5  # noqa: E157,F841
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            ppg.PlotJob(of, calc2, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "AA"
            assert read("out/plot") == "BB"

        def test_no_rerun_if_calc_change_but_ignore_codechanges(self, new_pipegraph):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            job = ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            new_pipegraph.new_pipegraph()

            def calc2():
                append("out/calc", "A")
                x = 5  # noqa: E157,F841
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            job = ppg.PlotJob(of, calc2, plot)
            job.ignore_code_changes()
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

        def test_plot_job_dependencies_are_added_to_just_the_cache_job(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            job = ppg.PlotJob(of, calc, plot)
            dep = ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
            job.depends_on(dep)
            assert dep in job.cache_job.prerequisites

        def test_raises_if_calc_returns_non_df(self):
            def calc():
                return None

            def plot(df):
                append("out/plot", "B")
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            job = ppg.PlotJob(of, calc, plot)
            try:
                ppg.run_pipegraph()
                raise ValueError("should not be reached")
            except ppg.RuntimeError:
                pass
            assert isinstance(job.cache_job.exception, ppg.JobContractError)

        def test_raises_if_plot_returns_non_plot(self):
            # import pyggplot
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return None

            of = "out/test.png"
            job = ppg.PlotJob(of, calc, plot)
            try:
                ppg.run_pipegraph()
                raise ValueError("should not be reached")
            except ppg.RuntimeError:
                pass
            assert isinstance(job.exception, ppg.JobContractError)

        def test_passing_non_function_for_calc(self):
            def inner():
                ppg.PlotJob("out/a", "shu", lambda df: 1)

            with pytest.raises(ValueError):
                inner()

        def test_passing_non_function_for_plot(self):
            def inner():
                ppg.PlotJob("out/a", lambda: 55, "shu")

            with pytest.raises(ValueError):
                inner()

        def test_passing_non_string_as_jobid(self):
            def inner():
                ppg.PlotJob(5, lambda: 1, lambda df: 34)

            with pytest.raises(TypeError):
                inner()


if __name__ == "__main__":
    unittest.main()
