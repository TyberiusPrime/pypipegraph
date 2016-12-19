from test_pypipegraph import PPGPerTest, rc_gen, append, write, read, writeappend
import unittest
import sys
sys.path.insert(0, '/code')
sys.path.append('/code/pydataframe')
sys.path.append('/code/pyggplot')
#import R
import pydataframe
import pyggplot
import pypipegraph as ppg
import os
import subprocess

def magic(filename):
    """See what linux 'file' commando says about that file"""
    if not os.path.exists(filename):
        raise OSError("Does not exists %s" % filename)
    p = subprocess.Popen(['file', filename], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout

class PlotJobTests(PPGPerTest):

    def test_basic(self):
        ppg.new_pipegraph(rc_gen(), quiet=False)
        import pydataframe
        def calc():
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)

    def test_pdf(self):
        import pydataframe
        def calc():
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.pdf'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PDF document') != -1)

    def test_raises_on_invalid_filename(self):
        import pydataframe
        def calc():
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.shu'
        def inner():
            job = ppg.PlotJob(of, calc, plot)
        self.assertRaises(ValueError, inner)


    def test_reruns_just_plot_if_plot_changed(self):
        import pydataframe
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'B')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def plot2(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('Y','X')
        job = ppg.PlotJob(of, calc, plot2)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'BB')

    def test_no_rerun_if_ignore_code_changes_and_plot_changes(self):
        import pydataframe
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'B')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def plot2(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('Y','X')
        job = ppg.PlotJob(of, calc, plot2)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'B')


    def test_reruns_both_if_calc_changed(self):
        import pydataframe
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'B')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def calc2():
            append('out/calc', 'A')
            x = 5
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        job = ppg.PlotJob(of, calc2, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'AA')
        self.assertEqual(read('out/plot'),'BB')

    def test_no_rerun_if_calc_change_but_ignore_codechanges(self):
        import pydataframe
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')
        self.assertEqual(read('out/plot'),'B')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def calc2():
            append('out/calc', 'A')
            x = 5
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        job = ppg.PlotJob(of, calc2, plot)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')

        self.assertEqual(read('out/plot'),'B')
    def test_plot_job_dependencies_are_added_to_just_the_cache_job(self):
        import pydataframe

        def calc():
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        dep = ppg.FileGeneratingJob('out/A', lambda : write('out/A', 'A'))
        job.depends_on(dep)
        #self.assertTrue(dep in job.prerequisites)
        self.assertTrue(dep in job.cache_job.prerequisites)

    def test_raises_if_calc_returns_non_df(self):
        #import pydataframe
        def calc():
            return None
        def plot(df):
            append('out/plot', 'B')
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertTrue(isinstance(job.cache_job.exception, ppg.JobContractError))

    def test_raises_if_plot_returns_non_plot(self):
        import pydataframe
        #import pyggplot
        def calc():
            return pydataframe.DataFrame({"X": list(range(0, 100)), 'Y': list(range(50, 150))})
        def plot(df):
            return None
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertTrue(isinstance(job.exception, ppg.JobContractError))

    def test_passing_non_function_for_calc(self):
        def inner():
            job = ppg.PlotJob('out/a', 'shu', lambda df: 1)
        self.assertRaises(ValueError, inner)

    def test_passing_non_function_for_plot(self):
        def inner():
            job = ppg.PlotJob('out/a', lambda: 55, 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.PlotJob(5, lambda: 1, lambda df: 34)
        self.assertRaises(ValueError, inner)

if __name__ == '__main__': 
    unittest.main()
