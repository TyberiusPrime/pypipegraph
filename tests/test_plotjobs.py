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

            def plot2(df):
                p = pyggplot.Plot(df).add_scatter("Y", "X")
                p.width = 5
                p.height = 2
                return p

            of = "out/test.png"
            p = ppg.PlotJob(of, calc, plot)
            p.add_fiddle(lambda p: p.scale_x_log10())
            p.add_another_plot("out/test2.png", plot2)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert os.path.exists(of + ".tsv")
            assert os.path.exists("cache/out/test.png")
            assert os.path.exists("out/test2.png")
            assert not os.path.exists("cache/out/test2.png")
            assert not os.path.exists("cache/out/test2.png.tsv")

        def test_basic_skip_table(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot, skip_table=True)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert not os.path.exists(of + ".tsv")
            assert os.path.exists("cache/out/test.png")

        def test_basic_return_dict(self):
            def calc():
                return {
                    "A": pd.DataFrame(
                        {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                    )
                }

            def plot(df):
                p = pyggplot.Plot(df["A"]).add_scatter("X", "Y")
                p.width = 5
                p.height = 1
                return p

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert read(of + ".tsv").find("#A\n") != -1

        def test_basic_return_dict_non_df_raises(self):
            def calc():
                return {
                    "A": pd.DataFrame(
                        {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                    ),
                    "B": "not_a_df",
                }

            def plot(df):
                return pyggplot.Plot(df["A"]).add_scatter("X", "Y")

            of = "out/test.png"
            p = ppg.PlotJob(of, calc, plot)
            p.height = 1200
            p.width = 800
            with pytest.raises(ppg.RuntimeError):
                ppg.run_pipegraph()
            assert "did not return a DataFrame" in str(p.cache_job.exception)

        def test_skip_caching(self):
            def calc():
                if not os.path.exists("A"):
                    raise ValueError()
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            def prep_job():
                write("A", "A")

            p = ppg.FileGeneratingJob("A", prep_job)
            # this tests the correct dependency setting on skip_caching
            of = "out/test.png"
            p2 = ppg.PlotJob(of, calc, plot, skip_caching=True)
            p2.depends_on(p)
            ppg.run_pipegraph()
            assert magic(of).find(b"PNG image") != -1
            assert not os.path.exists("cache/out/test.png")

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

        def test_unpickling_error(self, new_pipegraph):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            of = "out/test.png"
            p = ppg.PlotJob(of, calc, plot)
            ppg.run_pipegraph()
            new_pipegraph.new_pipegraph()
            p = ppg.PlotJob(of, calc, plot)
            with open("cache/out/test.png", "w") as op:
                op.write("no unpickling")
            os.unlink("out/test.png")  # so it reruns
            with pytest.raises(ppg.RuntimeError):
                ppg.run_pipegraph()
            assert not os.path.exists("out/test.png")
            assert isinstance(p.exception, ValueError)
            assert "Unpickling error in file" in str(p.exception)

        def test_add_another_not_returning_plot(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            def plot2(df):
                return

            of = "out/test.png"
            p = ppg.PlotJob(of, calc, plot)
            p.add_fiddle(lambda p: p.scale_x_log10())
            p2 = p.add_another_plot("out/test2.png", plot2)
            with pytest.raises(ppg.RuntimeError):
                ppg.run_pipegraph()
            assert isinstance(p2.exception, ppg.JobContractError)

    @pytest.mark.usefixtures("new_pipegraph")
    class TestCombinedPlotJobs:
        def test_complete(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150)), "w": "A"}
                )

            def calc2():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150)), "w": "B"}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            p1 = ppg.PlotJob("out/A.png", calc, plot)
            p2 = ppg.PlotJob("out/B.png", calc2, plot)
            import pathlib

            ppg.CombinedPlotJob(pathlib.Path("out/C.png"), [p1, p2], ["w"])
            ppg.CombinedPlotJob(pathlib.Path("out/D.png"), [p1, p2], [])
            ppg.CombinedPlotJob(
                pathlib.Path("out/E.png"),
                [p1, p2],
                {"facets": "w"},
                fiddle=lambda p: p.scale_x_log10(),
            )
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob(pathlib.Path("out/C.png"), [p1, p2], "w")
            with pytest.raises(TypeError):
                ppg.CombinedPlotJob(5, [p1, p2], "w")
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob("out/D.something", [p1, p2], "w")
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob("out/D.png", [], "w")
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob("out/D.png", [p1, p2.job_id], "w")

            ppg.run_pipegraph()
            assert magic("out/C.png").find(b"PNG image") != -1
            assert magic("out/D.png").find(b"PNG image") != -1
            assert magic("out/E.png").find(b"PNG image") != -1

        def test_plotjob_fails(self):
            def calc():
                return None

            def calc2():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150)), "w": "B"}
                )

            def plot(df):
                return pyggplot.Plot(df).add_scatter("X", "Y")

            p1 = ppg.PlotJob("out/A.png", calc, plot)
            p2 = ppg.PlotJob("out/B.png", calc2, plot)
            import pathlib

            pc = ppg.CombinedPlotJob(
                pathlib.Path("out/C.png"), [p1, p2], {"facet": "w"}
            )
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob(pathlib.Path("out/C.png"), [p1, p2], [])
            with pytest.raises(ValueError):
                ppg.CombinedPlotJob(pathlib.Path("out/C.png"), [p1], {"facet": "w"})

            ppg.CombinedPlotJob(pathlib.Path("out/D.png"), [p1, p2], [])
            ppg.CombinedPlotJob(pathlib.Path("out/E.png"), [p1, p2], {"facet": "w"})

            with pytest.raises(ppg.RuntimeError):
                ppg.run_pipegraph()
            assert "did not return a" in str(p1.cache_job.exception)
            assert pc.error_reason == "Indirect"


if __name__ == "__main__":
    unittest.main()
