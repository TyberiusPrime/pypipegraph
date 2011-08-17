import unittest
import time
import sys
sys.path.append('../../')
import pypipegraph as ppg
logger = ppg.util.start_logging('test')
import os
import shutil
import subprocess

#rc_gen = lambda : ppg.resource_coordinators.LocalTwisted()
rc_gen = lambda: ppg.resource_coordinators.LocalSystem()

def read(filename): 
    """simply read a file"""
    op = open(filename)
    data = op.read()
    op.close()
    return data

def write(filename, string):
    """open file for writing, dump string, close file"""
    op = open(filename, 'wb')
    op.write(string)
    op.close()

def append(filename, string):
    """open file for appending, dump string, close file"""
    op = open(filename, 'ab')
    op.write(string)
    op.close()

def magic(filename):
    """See what linux 'file' commando says about that file"""
    if not os.path.exists(filename):
        raise OSError("Does not exists %s" % filename)
    p = subprocess.Popen(['file', filename], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout

class PPGPerTest(unittest.TestCase):
    """For those testcases that need a new pipeline each time..."""
    def setUp(self):
        try:
            logger.info("rmtre out")
            shutil.rmtree('out')
        except:
            pass
        try:
            os.mkdir('out')
            logger.info('mkdir out')
        except OSError:
            pass
        ppg.forget_job_status()
        ppg.new_pipegraph(rc_gen(), quiet=True)
        logger.info("Starting new test\n" + "-" * 50 + "\n\n\n")

    def tearDown(self):
        pass


class SimpleTests(unittest.TestCase):

    def test_job_creation_before_pipegraph_creation_raises(self):
        ppg.destroy_global_pipegraph()
        def inner():
            job = ppg.FileGeneratingJob("A", lambda : None)
        self.assertRaises(ValueError, inner)

    def test_job_creation_after_pipegraph_run_raises(self):
        def inner():
            job = ppg.FileGeneratingJob("A", lambda : None)
        ppg.new_pipegraph(quiet=True)
        ppg.run_pipegraph()
        self.assertRaises(ValueError, inner)

    def test_run_may_be_called_only_once(self):
        ppg.new_pipegraph(quiet=True)
        ppg.run_pipegraph()
        def inner():
            ppg.run_pipegraph()
        self.assertRaises(ValueError, inner)



class CycleTests(unittest.TestCase):
    def setUp(self):
        try:
            os.mkdir('out')
        except OSError:
            pass
        ppg.forget_job_status()
        ppg.new_pipegraph(quiet=True)

    def tearDown(self):
        try:
            shutil.rmtree('out')
        except:
            pass

    def test_simple_cycle(self):
        def inner():
            ppg.new_pipegraph(quiet=True)
            jobA = ppg.FileGeneratingJob("A", lambda :write("A","A"))
            jobB = ppg.FileGeneratingJob("A", lambda :write("B","A"))
            jobA.depends_on(jobB)
            jobB.depends_on(jobA)
            ppg.run_pipegraph()
        self.assertRaises(ppg.CycleError, inner)

    def test_indirect_cicle(self):
        ppg.new_pipegraph(quiet=True)
        jobA = ppg.FileGeneratingJob("A", lambda :write("A","A"))
        jobB = ppg.FileGeneratingJob("B", lambda :write("B","A"))
        jobC = ppg.FileGeneratingJob("C", lambda :write("C","A"))
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        jobA.depends_on(jobC)
        def inner():
            ppg.run_pipegraph()
        self.assertRaises(ppg.CycleError, inner)






class JobTests(unittest.TestCase):
    def tearDown(self):
        try:
            shutil.rmtree('out')
        except:
            pass

    def test_assert_singletonicity_of_jobs(self):
        ppg.forget_job_status()
        ppg.new_pipegraph(quiet=True)
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            write(of, data_to_write)
        job = ppg.FileGeneratingJob(of, do_write)
        job2 = ppg.FileGeneratingJob(of, do_write)
        self.assertTrue(job is job2)

    def test_redifining_a_jobid_with_different_class_raises(self):
        ppg.forget_job_status()
        ppg.new_pipegraph(quiet=True)
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            write(of, data_to_write)
        job = ppg.FileGeneratingJob(of, do_write)
        def load():
            return 'shu'
        def inner():
            job_dl = ppg.DataLoadingJob(of, job)
        self.assertRaises(ValueError, inner)



    def test_addition(self):
        def write_func(of):
            def do_write():
                write(of, 'do_write done')
            return of, do_write
        ppg.new_pipegraph(quiet=True)
        jobA = ppg.FileGeneratingJob(*write_func('out/a'))
        jobB = ppg.FileGeneratingJob(*write_func('out/b'))
        jobC = ppg.FileGeneratingJob(*write_func('out/c'))
        jobD = ppg.FileGeneratingJob(*write_func('out/d'))

        aAndB = jobA + jobB
        #self.assertEqual(len(jobA), 2)
        self.assertEqual(len(aAndB), 2)
        self.assertTrue(jobA in aAndB)
        self.assertTrue(jobB in aAndB)

        aAndBandC = aAndB + jobC
        self.assertTrue(jobA in aAndBandC )
        self.assertTrue(jobB in aAndBandC )
        self.assertTrue(jobC in aAndBandC )

        aAndBAndD = jobD + aAndB
        self.assertTrue(jobA in aAndBAndD )
        self.assertTrue(jobB in aAndBAndD )
        self.assertTrue(jobD in aAndBAndD )

        cAndD = jobC + jobD
        all = aAndB + cAndD
        self.assertTrue(len(all), 4)
        self.assertTrue(jobA in all )
        self.assertTrue(jobB in all )
        self.assertTrue(jobC in all )
        self.assertTrue(jobD in all )

    def test_raises_on_non_str_job_id(self):
        def inner():
            job = ppg.FileGeneratingJob(1234, lambda : None)
        self.assertRaises( ValueError, inner)

    def test_equality_is_identity(self):
        def write_func(of):
            def do_write():
                write(of, 'do_write done')
            return of, do_write
        ppg.new_pipegraph(quiet=True)
        jobA = ppg.FileGeneratingJob(*write_func('out/a'))
        jobA1 = ppg.FileGeneratingJob(*write_func('out/a'))
        jobB = ppg.FileGeneratingJob(*write_func('out/b'))
        self.assertTrue(jobA is jobA1)
        self.assertTrue(jobA == jobA1)
        self.assertFalse(jobA == jobB)

    def test_has_hash(self):
        ppg.new_pipegraph(quiet=True)
        jobA = ppg.FileGeneratingJob('out/',lambda : None)
        self.assertTrue(hasattr(jobA, '__hash__'))





class FileGeneratingJobTests(PPGPerTest):

    def test_basic(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            print 'do_write was called'
            write(of, data_to_write)
        job = ppg.FileGeneratingJob(of, do_write)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        self.assertFalse(job.failed)
        self.assertTrue(os.path.exists(of))
        op = open(of, 'rb')
        data = op.read()
        op.close()
        self.assertEqual(data, data_to_write)
        self.assertEqual(job.was_run, True)

    def test_simple_filegeneration_with_function_dependency(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            print 'do_write was called'
            write(of, data_to_write)
        job = ppg.FileGeneratingJob(of, do_write)
        #job.ignore_code_changes() this would be the magic line to remove the function dependency
        ppg.run_pipegraph()
        self.assertFalse(job.failed)
        self.assertTrue(os.path.exists(of))
        op = open(of, 'rb')
        data = op.read()
        op.close()
        self.assertEqual(data, data_to_write)

    def test_filejob_raises_if_no_data_is_written(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            pass
        job = ppg.FileGeneratingJob(of, do_write)
        def inner():
            ppg.run_pipegraph()
        self.assertRaises(ppg.RuntimeError, inner)
        self.assertTrue(job.failed)
        #print 'exception is', repr(job.exception)
        self.assertTrue(isinstance(job.exception, ppg.JobContractError))
        self.assertFalse(os.path.exists(of))

    def test_simple_filegeneration_removes_file_on_exception(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            write(of, data_to_write)
            raise ValueError("shu")
        job = ppg.FileGeneratingJob(of, do_write)
        try:
            ppg.run_pipegraph()
            raise ValueError("should have raised RuntimeError")
        except ppg.RuntimeError:
            pass
        self.assertTrue(job.failed)
        self.assertFalse(os.path.exists(of))
        self.assertTrue(isinstance(job.exception, ValueError))

    def test_simple_filegeneration_renames_file_on_exception(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            write(of, data_to_write)
            raise ValueError("shu")
        job = ppg.FileGeneratingJob(of, do_write, rename_broken=True)
        try:
            ppg.run_pipegraph()
            raise ValueError("should have raised RuntimeError")
        except ppg.RuntimeError:
            pass
        self.assertTrue(job.failed)
        self.assertFalse(os.path.exists(of))
        self.assertTrue(os.path.exists(of + '.broken'))
        self.assertTrue(isinstance(job.exception, ValueError))

    def test_simple_filegeneration_captures_stdout_stderr(self):
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            op = open(of,'wb')
            op.write(data_to_write)
            op.close()
            print 'stdout is cool'
            sys.stderr.write("I am stderr")
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertFalse(job.failed)
        self.assertTrue(os.path.exists(of))
        op = open(of, 'rb')
        data = op.read()
        op.close()
        self.assertEqual(data, data_to_write)
        self.assertEqual(job.stdout, 'stdout is cool\n')
        self.assertEqual(job.stderr, 'I am stderr') #no \n here

    def test_filegeneration_does_not_change_mcp(self):
        global global_test
        global_test = 1
        of = "out/a"
        data_to_write = "hello"
        def do_write():
            write(of, data_to_write)
            global global_test
            global_test = 2
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertEqual(global_test, 1)


    def test_file_generation_chaining_simple(self):
        ofA = 'out/a'
        def writeA():
            write(ofA, 'Hello')
        jobA = ppg.FileGeneratingJob(ofA, writeA)
        ofB = 'out/b'
        def writeB():
            op = open(ofB, 'wb')
            ip = open(ofA, 'rb')
            op.write(ip.read()[::-1])
            op.close()
            ip.close()
        jobB = ppg.FileGeneratingJob(ofB, writeB)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        self.assertTrue(read(ofA) == read(ofB)[::-1])

    def test_file_generation_multicore(self): 
        #one fork per FileGeneratingJob...
        ofA = 'out/a'
        def writeA():
            write(ofA, '%i' % os.getpid())
        ofB = 'out/b'
        def writeB():
            write(ofB, '%i' % os.getpid())
        jobA = ppg.FileGeneratingJob(ofA, writeA)
        jobB = ppg.FileGeneratingJob(ofB, writeB)
        ppg.run_pipegraph()
        self.assertNotEqual(read(ofA), read(ofB))

    def test_invaliding_removes_file(self):
        of = 'out/a'
        sentinel = 'out/b'
        def do_write():
            if os.path.exists(sentinel):
                raise ValueError("second run")
            write(of,'shu')
            write(sentinel, 'done')
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.ParameterInvariant('my_params', (1,))
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertTrue(os.path.exists(of))
        self.assertTrue(os.path.exists(sentinel))

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.ParameterInvariant('my_params', (2,)) #same name ,changed params, job needs to rerun, but explodes...
        job.depends_on(dep) #on average, half the mistakes are in the tests...
        try:
            ppg.run_pipegraph()
            raise ValueError("Should not have been reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists(of))

    def test_passing_non_function(self):
        def inner():
            job = ppg.FileGeneratingJob('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.FileGeneratingJob(5, lambda: 1)
        self.assertRaises(ValueError, inner)

class MultiFileGeneratingJobTests(PPGPerTest):
    def setUp(self):
        try:
            shutil.rmtree('out')
        except:
            pass
        try:
            os.mkdir('out')
        except OSError:
            pass
        ppg.forget_job_status()
        ppg.new_pipegraph(rc_gen(), quiet=True)

    def tearDown(self):
        #shutil.rmtree('out')
        pass

    def test_basic(self):
        of = ['out/a', 'out/b']
        def do_write():
            for f in of:
                append(f, 'shu')
        job = ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            self.assertEqual(read(f),'shu')
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        for f in of:
            self.assertEqual(read(f),'shu') #ie. job has net been rerun...
        #but if I now delete one...
        os.unlink(of[0])
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertEqual(read(of[0]), 'shu')
        self.assertEqual(read(of[1]), 'shu') #Since that file was also deleted when MultiFileGeneratingJob was invalidated...

    def test_exception_destroys_all_files(self):
        of = ['out/a', 'out/b']
        def do_write():
            for f in of:
                append(f, 'shu')
            raise ValueError("explode")
        job = ppg.MultiFileGeneratingJob(of, do_write)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            self.assertFalse(os.path.exists(f))

    def test_exception_destroys_renames_files(self):
        of = ['out/a', 'out/b']
        def do_write():
            for f in of:
                append(f, 'shu')
            raise ValueError("explode")
        job = ppg.MultiFileGeneratingJob(of, do_write, rename_broken = True)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            self.assertTrue(os.path.exists(f + '.broken'))

    def test_invalidation_removes_all_files(self):
        of = ['out/a', 'out/b']
        sentinel = 'out/sentinel'#hack so this one does something different the second time around...
        def do_write():
            if os.path.exists(sentinel):
                raise ValueError("explode")
            write(sentinel, 'shu')
            for f in of:
                append(f, 'shu')
        job = ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant('myparam', (1,))
            )
        ppg.run_pipegraph()
        for f in of:
            self.assertTrue(os.path.exists(f))
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant('myparam', (2,))
            )
        try:
            ppg.run_pipegraph() #since this should blow up
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        for f in of:
            self.assertFalse(os.path.exists(f))
    
    def test_passing_not_a_list_of_str(self):
        def inner():
            job = ppg.MultiFileGeneratingJob('out/a', lambda: 1)
        self.assertRaises(ValueError, inner)

    def test_passing_non_function(self):
        def inner():
            job = ppg.MultiFileGeneratingJob(['out/a'], 'shu')
        self.assertRaises(ValueError, inner)

test_modifies_shared_global = []
class DataLoadingJobTests(PPGPerTest):

    def test_modifies_slave(self):
        #global shared
        #shared = "I was the the global in the mcp"
        def load():
            test_modifies_shared_global.append("shared data")
        of = 'out/a'
        def do_write():
            write(of, "\n".join(test_modifies_shared_global)) #this might actually be a problem when defining this?
        dlJo = ppg.DataLoadingJob('myjob', load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shared data')


    def test_global_statement_works(self): 
        #this currently does not work in the cloudpickle transmitted jobs - 
        #two jobs refereing to global have different globals afterwards
        #or the 'global shared' does not work as expected after loading
        global shared
        shared = "I was the the global in the mcp"
        def load():
            global shared
            shared = "shared data"
        of = 'out/a'
        def do_write():
            write(of, shared)
        dlJo = ppg.DataLoadingJob('myjob', load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shared data')


    def test_does_not_get_run_without_dep_job(self):
        of = 'out/shu'
        def load():
            write(of, 'shu') #not the fine english way, but we need a sideeffect that's checkable
        job = ppg.DataLoadingJob('myjob', load)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(of))

    def test_does_not_get_run_in_chain_without_final_dep(self):
        of = 'out/shu'
        def load():
            write(of, 'shu') #not the fine english way, but we need a sideeffect that's checkable
        job = ppg.DataLoadingJob('myjob', load)
        ofB = 'out/sha'
        def loadB():
            write(ofB, 'sha')
        jobB = ppg.DataLoadingJob('myjobB', loadB).depends_on(job)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(of))
        self.assertFalse(os.path.exists(ofB))

    def test_does_get_run_in_chain_all(self):
        of = 'out/shu'
        def load():
            write(of, 'shu') #not the fine english way, but we need a sideeffect that's checkable
        job = ppg.DataLoadingJob('myjob', load)
        ofB = 'out/sha'
        def loadB():
            write(ofB, 'sha')
        jobB = ppg.DataLoadingJob('myjobB', loadB).depends_on(job)
        ofC = 'out/c'
        def do_write():
            write(ofC, ofC)
        jobC = ppg.FileGeneratingJob(ofC, do_write).depends_on(jobB)
        ppg.run_pipegraph()
        self.assertTrue(os.path.exists(of))
        self.assertTrue(os.path.exists(ofB))
        self.assertTrue(os.path.exists(ofC))

    def test_chain_with_filegenerating_works(self):
        of = 'out/a'
        def do_write():
            write(of, of)
        jobA = ppg.FileGeneratingJob(of, do_write)
        o = Dummy()
        def do_load():
            o.a = read(of)
        jobB = ppg.DataLoadingJob('loadme', do_load).depends_on(jobA)
        ofB = 'out/b'
        def write2():
            write(ofB, o.a)
        jobC = ppg.FileGeneratingJob(ofB, write2).depends_on(jobB)
        ppg.run_pipegraph()
        self.assertEqual(read(of), of)
        self.assertEqual(read(ofB), of)


    def test_does_get_run_depending_on_jobgenjob(self):
        of = 'out/shu'
        def load():
            write(of, 'shu') #not the fine english way, but we need a sideeffect that's checkable
        job = ppg.DataLoadingJob('myjob', load)
        def gen():
            ofB = "out/b"
            def do_write():
                write(ofB, 'hello')
            ppg.FileGeneratingJob(ofB, do_write)
        gen_job = ppg.JobGeneratingJob('mygen', gen).depends_on(job)
        ppg.run_pipegraph()
        self.assertTrue(os.path.exists(of)) #so the data loading job was run
        self.assertEqual(read('out/b'), 'hello') #and so was the jobgen and filegen job.

    def test_passing_non_function(self):
        def inner():
            job = ppg.DataLoadingJob('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.DataLoadingJob(5, lambda: 1)
        self.assertRaises(ValueError, inner)

    def test_failing_dataloading_jobs(self):
        o = Dummy()
        of = 'out/A'
        def write():
            write(of, o.a)
        def load():
            o.a = 'shu'
            raise ValueError()
        job_fg = ppg.FileGeneratingJob(of, write)
        job_dl = ppg.DataLoadingJob('doload', load)
        job_fg.depends_on(job_dl)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists(of))
        self.assertTrue(job_dl.failed)
        self.assertTrue(job_fg.failed)
        self.assertTrue(isinstance(job_dl.exception, ValueError))


class Dummy(object):
    pass

class AttributeJobTests(PPGPerTest):

    def test_basic_attribute_loading(self):
        o = Dummy()
        def load():
            return 'shu'
        job = ppg.AttributeLoadingJob('load_dummy_shu', o, 'a', load)
        of = 'out/a'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu')

    def test_attribute_loading_does_not_affect_mcp(self):
        o = Dummy()
        def load():
            return 'shu'
        job = ppg.AttributeLoadingJob('load_dummy_shu', o, 'a', load)
        of = 'out/a'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu')
        self.assertFalse(hasattr(o, 'a'))

    def test_attribute_loading_does_not_run_withot_dependency(self):
        o = Dummy()
        tf = 'out/testfile'
        def load():
            write(tf, 'hello')
            return 'shu'
        job = ppg.AttributeLoadingJob('load_dummy_shu', o, 'a', load)
        ppg.run_pipegraph()
        self.assertFalse(hasattr(o, 'a'))
        self.assertFalse(os.path.exists(tf))

    def test_attribute_disappears_after_direct_dependency(self):
        o = Dummy()
        job = ppg.AttributeLoadingJob('load_dummy_shu', o, 'a', lambda: 'shu')
        of = 'out/A'
        def do_write():
            write(of, o.a)
        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = 'out/B'
        def later_write():
            write(of2, o.a)
        fgjobB = ppg.FileGeneratingJob(of2, later_write).depends_on(fgjob)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertEqual(read(of), 'shu')
        self.assertFalse(os.path.exists(of2))

    def test_attribute_disappears_after_direct_dependencies(self):
        o = Dummy()
        job = ppg.AttributeLoadingJob('load_dummy_shu', o, 'a', lambda: 'shu')
        of = 'out/A'
        def do_write():
            write(of, o.a)
        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = 'out/B'
        def later_write():
            write(of2, o.a)
        fgjobB = ppg.FileGeneratingJob(of2, later_write).depends_on(fgjob)#now, this one does not depend on job, o it should not be able to access o.a
        of3 = 'out/C'
        def also_write():
            write(of3, o.a)
        fgjobC = ppg.FileGeneratingJob(of3, also_write).depends_on(job)
        fgjobB.depends_on(fgjobC) #otherwise, B might be started C returned, and the cleanup will not have occured!
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertEqual(read(of), 'shu')
        self.assertEqual(read(of3), 'shu')
        self.assertFalse(os.path.exists(of2))

    def test_passing_non_string_as_attribute(self):
        o = Dummy()
        def inner():
            job = ppg.AttributeLoadingJob('out/a', o, 5, 55)
        self.assertRaises(ValueError, inner)

    def test_passing_non_function(self):
        o = Dummy()
        def inner():
            job = ppg.AttributeLoadingJob('out/a', o, 'a', 55)
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()
        def inner():
            job = ppg.AttributeLoadingJob(5, o, 'a', lambda: 55)
        self.assertRaises(ValueError, inner)

class TempFileGeneratingJobTest(PPGPerTest):

    def test_basic(self):
        temp_file = 'out/temp'
        def write_temp():
            write(temp_file, 'hello')
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = 'out/A'
        def write_A():
            write(ofA, read(temp_file))
        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()
        self.assertEqual(read(ofA), 'hello')
        self.assertFalse(os.path.exists(temp_file))

    def test_does_not_get_return_if_output_is_done(self):
        temp_file = 'out/temp'
        out_file = 'out/A'
        count_file = 'out/count'
        normal_count_file = 'out/countA'
        def write_count():
            try:
                count = read(out_file)
                count = count[:count.find(':')]
            except IOError:
                count = '0'
            count = int(count) + 1
            write(out_file, str(count) + ':' + read(temp_file))
            append(normal_count_file,'A')
        def write_temp():
            write(temp_file, 'temp')
            append(count_file, 'X')
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(temp_file))
        self.assertEqual(read(out_file), '1:temp')
        self.assertEqual(read(count_file), 'X')
        self.assertEqual(read(normal_count_file), 'A')
        #now, rerun. Tempfile has been deleted, 
        #and should not be regenerated
        ppg.new_pipegraph(rc_gen(), quiet=True)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(temp_file))
        self.assertEqual(read(out_file), '1:temp')
        self.assertEqual(read(count_file), 'X')
        self.assertEqual(read(normal_count_file), 'A')


    def test_does_not_get_return_if_output_is_not(self):
        temp_file = 'out/temp'
        out_file = 'out/A'
        count_file = 'out/count'
        normal_count_file = 'out/countA'
        def write_count():
            try:
                count = read(out_file)
                count = count[:count.find(':')]
            except IOError:
                count = '0'
            count = int(count) + 1
            write(out_file, str(count) + ':' + read(temp_file))
            append(normal_count_file,'A')
        def write_temp():
            write(temp_file, 'temp')
            append(count_file, 'X')
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(temp_file))
        self.assertEqual(read(out_file), '1:temp')
        self.assertEqual(read(count_file), 'X')
        self.assertEqual(read(normal_count_file), 'A')
        #now, rerun. Tempfile has been deleted, 
        #and should  be regenerated
        os.unlink(out_file)
        ppg.new_pipegraph(rc_gen(), quiet=True)
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists(temp_file))
        self.assertEqual(read(out_file), '1:temp') #since the outfile was removed...
        self.assertEqual(read(count_file), 'XX')
        self.assertEqual(read(normal_count_file), 'AA')



    def test_dependand_explodes(self):
        temp_file = 'out/temp'
        def write_temp():
            append(temp_file, 'hello')
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = 'out/A'
        def write_A():
            raise ValueError("shu")
        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        #ppg.run_pipegraph()
        self.assertFalse(os.path.exists(ofA))
        self.assertTrue(os.path.exists(temp_file))

        ppg.new_pipegraph(rc_gen())
        def write_A_ok():
            write(ofA, read(temp_file))
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        fgjob = ppg.FileGeneratingJob(ofA, write_A_ok)
        fgjob.depends_on(temp_job)
        ppg.run_pipegraph()

        self.assertEqual(read(ofA), 'hello') #tempfile job has not been rerun
        self.assertFalse(os.path.exists(temp_file)) #and the tempfile has been removed...

    def test_removes_tempfile_on_exception(self):
        temp_file = 'out/temp'
        def write_temp():
            write(temp_file, 'hello')
            raise ValueError("should")
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = 'out/A'
        def write_A():
            write(ofA, read(temp_file))
        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists(temp_file))
        self.assertFalse(os.path.exists(ofA))

    def test_renames_tempfile_on_exception_if_requested(self):
        temp_file = 'out/temp'
        def write_temp():
            write(temp_file, 'hello')
            raise ValueError("should")
        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp, rename_broken=True)
        ofA = 'out/A'
        def write_A():
            write(ofA, read(temp_file))
        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists(temp_file))
        self.assertTrue(os.path.exists(temp_file + '.broken'))
        self.assertFalse(os.path.exists(ofA))

    def test_passing_non_function(self):
        def inner():
            job = ppg.TempFileGeneratingJob('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.TempFileGeneratingJob(5, lambda: 1)
        self.assertRaises(ValueError, inner)

    def test_rerun_because_of_new_dependency_does_not_rerun_old(self):
        jobA = ppg.FileGeneratingJob('out/A', lambda: append("out/A", read('out/temp')) or append('out/Ax', 'A'))
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda :write("out/temp", 'T'))
        jobA.depends_on(jobB)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists('out/temp'))
        self.assertEqual(read("out/A"), "T")
        self.assertEqual(read("out/Ax"), "A") #ran once

        ppg.new_pipegraph(rc_gen(), quiet=True)
        jobA = ppg.FileGeneratingJob('out/A', lambda: append("out/A", read('out/temp')))
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda :write("out/temp", 'T'))
        jobA.depends_on(jobB)
        jobC = ppg.FileGeneratingJob('out/C', lambda: append("out/C", read('out/temp')))
        jobC.depends_on(jobB)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists('out/temp'))
        self.assertEqual(read("out/Ax"), "A") #ran once, not rewritten
        self.assertEqual(read("out/C"), "T") #a new file

    def test_chaining_multiple(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write('out/A', 'A'))
        jobB = ppg.TempFileGeneratingJob('out/B', lambda: write('out/B', read('out/A') + 'B'))
        jobC = ppg.TempFileGeneratingJob('out/C', lambda: write('out/C', read('out/A') + 'C'))
        jobD = ppg.FileGeneratingJob('out/D', lambda: write('out/D', read('out/B') + read('out/C')))
        jobD.depends_on(jobC)
        jobD.depends_on(jobB)
        jobC.depends_on(jobA)
        jobB.depends_on(jobA)
        ppg.run_pipegraph()
        self.assertEqual(read('out/D'), 'ABAC')
        self.assertFalse(os.path.exists('out/A'))
        self.assertFalse(os.path.exists('out/B'))
        self.assertFalse(os.path.exists('out/C'))

    def test_chaining_multiple_differently(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda: write('out/A', 'A'))
        jobB = ppg.TempFileGeneratingJob('out/B', lambda: write('out/B', read('out/A') + 'B'))
        jobD = ppg.FileGeneratingJob('out/D', lambda: write('out/D', read('out/B') + "D"))
        jobE = ppg.FileGeneratingJob('out/E', lambda: write('out/E', read('out/B') + "E"))
        jobF = ppg.FileGeneratingJob('out/F', lambda: write('out/F', read('out/A') + 'F'))
        jobD.depends_on(jobB)
        jobE.depends_on(jobB)
        jobB.depends_on(jobA)
        jobF.depends_on(jobA)
        ppg.run_pipegraph()
        self.assertEqual(read('out/D'), 'ABD')
        self.assertEqual(read('out/E'), 'ABE')
        self.assertEqual(read('out/F'), 'AF')
        self.assertFalse(os.path.exists('out/A'))
        self.assertFalse(os.path.exists('out/B'))
        self.assertFalse(os.path.exists('out/C'))


class InvariantTests(PPGPerTest):

    def setUp(self):
        try:
            os.mkdir('out')
        except OSError:
            pass
        ppg.forget_job_status()
        ppg.new_pipegraph(rc_gen(), quiet=True)

    def tearDown(self):
        shutil.rmtree('out')

    def sentinel_count(self):
        sentinel = 'out/sentinel'
        try:
            op = open(sentinel, 'rb')
            count = int(op.read())
            op.close()
        except:
            count = 1
        op = open(sentinel, 'wb')
        op.write("%i" % (count + 1))
        op.close()
        return count

    def test_filegen_jobs_detect_code_change(self):
        of = 'out/a'
        def do_write():
            append(of, 'shu' * self.sentinel_count())
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu')
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #has not been run again...
        def do_write2():
            append(of, 'sha')
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'sha') #has been run again ;).

    def test_filegen_jobs_ignores_code_change(self):
        of = 'out/a'
        def do_write():
            append(of, 'shu' * self.sentinel_count())
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()

        self.assertEqual(read(of), 'shu')
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #has not been run again, for no change

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def do_write2():
            append(of, 'sha')
        job = ppg.FileGeneratingJob(of, do_write2)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #has not been run again, since we ignored the changes

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write2)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'sha') #But the new code had not been stored, not ignoring => redoing.

    def test_parameter_dependency(self):
        of = 'out/a'
        sentinel = 'out/sentinel'
        def do_write():
            append(of, 'shu' * self.sentinel_count())
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant('myparam', (1,2,3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu')
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant('myparam', (1,2,3))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #has not been run again...
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant('myparam', (1,2,3, 4))
        job.depends_on(param_dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shushu') #has been run again ;).

    def test_parameter_invariant_adds_hidden_job_id_prefix(self):
        param = 'A'
        jobA = ppg.FileGeneratingJob('out/A', lambda: write('out/A', param))
        jobB = ppg.ParameterInvariant('out/A', param)
        jobA.depends_on(jobB)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), param)


    def test_filetime_dependency(self):
        of = 'out/a'
        def do_write():
            append(of, 'shu' * self.sentinel_count())

        ftfn = 'out/ftdep'
        write(ftfn,'hello')
        write(of,'hello')
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #job get's run though there is a file, because the FileTimeInvariant was not stored before...
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #job does not get rerun...

        time.sleep(1) #so linux actually advances the file time in the next line
        write(ftfn,'hello') #same content, different time

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileTimeInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shushu') #job does get rerun...


    def test_filechecksum_dependency(self):
        of = 'out/a'
        def do_write():
            append(of, 'shu' * self.sentinel_count())
        ftfn = 'out/ftdep'
        write(ftfn,'hello')
        import stat
        #logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of,'hello')

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #job get's run though there is a file, because the FileTimeInvariant was not stored before...
        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #job does not get rerun...

        time.sleep(1) #so linux actually advances the file time in the next line
        #logging.info("NOW REWRITE")
        write(ftfn,'hello') #same content, different time

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shu') #job does not get rerun...

        #time.sleep(1) #we don't care about the time, size should be enough...
        write(ftfn,'hello world') #different time

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileChecksumInvariant(ftfn)
        job.depends_on(dep)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'shushu') #job does get rerun

class FunctionInvariantTests(PPGPerTest):
    #most of the function invariant testing is handled by other test classes.
    #but these are more specialized.

    def test_generator_expressions(self):
        def get_func(r):
            def shu():
                return sum(i + 0 for i in r)
            return shu
        def get_func2(r):
            def shu():
                return sum(i + 0 for i in r)
            return shu
        def get_func3(r):
            def shu():
                return sum(i + 1 for i in r)
            return shu
        a = ppg.FunctionInvariant('a', get_func(100))
        b = ppg.FunctionInvariant('b', get_func2(100))#that invariant should be the same
        c = ppg.FunctionInvariant('c', get_func3(100)) #and this invariant should be different
        av = a.get_invariant(False)
        bv = b.get_invariant(False)
        cv= c.get_invariant(False)
        self.assertTrue(a.get_invariant(False))
        self.assertEqual(bv, av)
        self.assertNotEqual(av, cv)

    def test_lambdas(self):
        def get_func(x):
            def inner():
                arg = lambda y: x + x + x
                return arg(1)
            return inner
        def get_func2(x):
            def inner():
                arg = lambda y: x + x + x
                return arg(1)
            return inner
        def get_func3(x):
            def inner():
                arg = lambda y: x + x
                return arg(1)
            return inner
        a = ppg.FunctionInvariant('a', get_func(100))
        b = ppg.FunctionInvariant('b', get_func2(100))#that invariant should be the same
        c = ppg.FunctionInvariant('c', get_func3(100)) #and this invariant should be different
        av = a.get_invariant(False)
        bv = b.get_invariant(False)
        cv= c.get_invariant(False)
        self.assertTrue(a.get_invariant(False))
        self.assertEqual(bv, av)
        self.assertNotEqual(av, cv)


    def test_passing_non_function_raises(self):
        def inner():
            job = ppg.FunctionInvariant('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_none_as_function_is_ok(self):
        job = ppg.FunctionInvariant('out/a',None)
        jobB = ppg.FileGeneratingJob('out/A', lambda: write('out/A', 'A'))
        jobB.depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), 'A')



    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.FunctionInvariant(5, lambda: 1)
        self.assertRaises(ValueError, inner)

class DependencyTests(PPGPerTest):


    def test_simple_chain(self):
        o = Dummy()
        def load_a():
            return 'shu'
        jobA = ppg.AttributeLoadingJob('a', o, 'myattr', load_a)
        ofB = 'out/B'
        def do_write_b():
            write(ofB, o.myattr)
        jobB = ppg.FileGeneratingJob(ofB, do_write_b).depends_on(jobA)
        ofC = 'out/C'
        def do_write_C():
            write(ofC, o.myattr)
        jobC = ppg.FileGeneratingJob(ofC, do_write_C).depends_on(jobA)

        ofD = 'out/D'
        def do_write_d():
            write(ofD, read(ofC) + read(ofB))
        jobD = ppg.FileGeneratingJob(ofD, do_write_d).depends_on([jobA, jobB])

    def test_failed_job_kills_those_after(self):
        ofA = 'out/A'
        def write_a():
            append(ofA, 'hello')
        jobA = ppg.FileGeneratingJob(ofA, write_a)

        ofB = 'out/B'
        def write_b():
            raise ValueError("shu")
        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = 'out/C'
        def write_c():
            write(ofC, 'hello')
        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertTrue(os.path.exists(ofA)) #which was before the error
        self.assertFalse(os.path.exists(ofB)) #which was on the error
        self.assertFalse(os.path.exists(ofC)) #which was after the error
        ppg.new_pipegraph(rc_gen(), quiet=True)
        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobC = ppg.FileGeneratingJob(ofC, write_c)
        def write_b_ok():
            write(ofB, 'BB')
        jobB = ppg.FileGeneratingJob(ofB, write_b_ok)
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run_pipegraph()

        self.assertTrue(os.path.exists(ofA))
        self.assertEqual(read(ofA), 'hello') #run only once!
        self.assertTrue(os.path.exists(ofB)) 
        self.assertTrue(os.path.exists(ofC))

    def test_done_filejob_does_not_gum_up_execution(self):
        ofA = 'out/A'
        write(ofA, '1111')
        def write_a():
            append(ofA, 'hello')
        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobA.ignore_code_changes() #or it will inject a function dependency and run never the less...

        ofB = 'out/B'
        def write_b():
            append(ofB, 'hello')
        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = 'out/C'
        def write_c():
            write(ofC, 'hello')
        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        self.assertTrue(os.path.exists(ofA))

        ppg.run_pipegraph()

        self.assertTrue(os.path.exists(ofB))
        self.assertTrue(os.path.exists(ofC))
        self.assertEqual(read(ofA), '1111')
     


    def test_invariant_violation_redoes_deps_but_not_nondeps(self):
        def get_job(name):
            fn = 'out/' + name
            def do_write():
                if os.path.exists(fn + '.sentinel'):
                    d = read(fn + '.sentinel')
                else:
                    d = ''
                append(fn + '.sentinel', name) #get's longer all the time...
                write(fn, d + name) #get's deleted anyhow...
            return ppg.FileGeneratingJob(fn, do_write)
        jobA = get_job('A')
        jobB = get_job('B')
        jobC = get_job('C')
        jobD = get_job('D')
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant('myparam', ('hello',))
        jobA.depends_on(dep)
        ppg.run_pipegraph()
        self.assertTrue(read('out/A'), 'A')
        self.assertTrue(read('out/B'), 'B')
        self.assertTrue(read('out/C'), 'C')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        jobA = get_job('A')
        jobB = get_job('B')
        jobC = get_job('C')
        jobD = get_job('D')
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant('myparam', ('hello stranger',))
        jobA.depends_on(dep) #now, the invariant has been changed, all jobs rerun...
        ppg.run_pipegraph()
        self.assertTrue(read('out/A'), 'AA') #thanks to our smart rerun aware job definition..
        self.assertTrue(read('out/B'), 'BB')
        self.assertTrue(read('out/C'), 'CC')
        self.assertTrue(read('out/D'), 'D') #since that one does not to be rerun...

    def test_depends_on_accepts_a_list(self):
        o = Dummy()
        jobA = ppg.FileGeneratingJob('out/A', lambda: write('out/A', 'A'))
        jobB = ppg.FileGeneratingJob('out/B', lambda: write('out/B', 'B'))
        jobC = ppg.FileGeneratingJob('out/C', lambda: write('out/C', 'C'))
        jobC.depends_on([jobA, jobB])
        ppg.run_pipegraph()
        self.assertTrue(read('out/A'), 'A')
        self.assertTrue(read('out/B'), 'B')
        self.assertTrue(read('out/C'), 'C')

    def test_depends_on_accepts_a_list_of_lists(self):
        o = Dummy()
        jobA = ppg.FileGeneratingJob('out/A', lambda: write('out/A', 'A'))
        jobB = ppg.FileGeneratingJob('out/B', lambda: write('out/B', 'B'))
        jobC = ppg.FileGeneratingJob('out/C', lambda: write('out/C', read('out/A') + read('out/B') + read('out/D')))
        jobD = ppg.FileGeneratingJob('out/D', lambda: write('out/D', 'D'))
        jobC.depends_on([jobA, [jobB, jobD]])
        ppg.run_pipegraph()
        self.assertTrue(jobD in jobC.prerequisites)
        self.assertTrue(jobA in jobC.prerequisites)
        self.assertTrue(jobB in jobC.prerequisites)
        self.assertTrue(read('out/A'), 'A')
        self.assertTrue(read('out/B'), 'B')
        self.assertTrue(read('out/C'), 'ABD')
        self.assertTrue(read('out/D'), 'D')


class DependencyInjectionJobTests(PPGPerTest):

    def test_basic(self):
        #TODO: there is a problem with this apporach. The AttributeLoadingJob
        #references different objects, since it get's pickled alongside with the method,
        #and depickled again, and then it's not the same object anymore,
        #so the FileGeneratingJob and the AttributeLoadingJob in this test
        #reference different objects.
        #I'm not sure how to handle this right now though.

        #I have an idea: Do JobGraphModifyingJobs in each slave, and send back just the
        #dependency data (and new job name).
        #that way, we can still execute on any slave, and all the pointers should be
        #right.
        ppg.new_pipegraph(rc_gen())

        o = Dummy()
        of = 'out/A'
        def do_write():
            #logging.info("Accessing dummy (o) %i in pid %s" % (id(o), os.getpid()))
            write(of, o.A + o.B)
        job = ppg.FileGeneratingJob(of, do_write)
        def generate_deps():
            def load_a():
                #logging.info('executing load A')
                return "A"
            def load_b():
                #logging.info('executing load B')
                return "B"
            #logging.info("Creating dl on %i in pid %s" % (id(o), os.getpid()))
            dlA = ppg.AttributeLoadingJob('dlA', o, 'A', load_a)
            #logging.info("created dlA")
            dlB = ppg.AttributeLoadingJob('dlB', o, 'B', load_b)
            job.depends_on(dlA)
            job.depends_on(dlB)
        gen_job = ppg.DependencyInjectionJob('C', generate_deps)
        job.depends_on(gen_job)
        ppg.run_pipegraph()
        self.assertEqual(read(of), 'AB')

    def test_raises_on_non_dependend_job_injection(self):
        o = Dummy()
        of = 'out/A'
        def do_write():
            write(of, o.A + o.B)
        job = ppg.FileGeneratingJob(of, do_write)
        jobD = ppg.FileGeneratingJob('out/D', lambda : write('out/D', 'D'))
        def generate_deps():
            def load_a():
                return "A"
            def load_b():
                return "B"
            dlA = ppg.AttributeLoadingJob('dlA', o, 'A', load_a)
            dlB = ppg.AttributeLoadingJob('dlB', o, 'B', load_b)
            job.depends_on(dlA)
            jobD.depends_on(dlB) #this line must raise
        gen_job = ppg.DependencyInjectionJob('C', generate_deps)
        job.depends_on(gen_job)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists(of)) #since the gen job crashed
        self.assertTrue(os.path.exists('out/D')) #since it has no relation to the gen job actually...
        self.assertTrue(isinstance(gen_job.exception, ppg.JobContractError))

    def test_injecting_filegenerating_job(self):
        of = 'out/A'
        def do_write():
            write(of, read("out/B"))
        job = ppg.FileGeneratingJob(of, do_write)
        def generate_dep():
            def write_B():
                write("out/B", "B")
            inner_job = ppg.FileGeneratingJob('out/B', write_B)
            job.depends_on(inner_job)
        job_gen = ppg.DependencyInjectionJob('gen_job', generate_dep)
        job.depends_on(job_gen)
        ppg.run_pipegraph()
        self.assertEqual(read("out/A"), 'B')

    def test_passing_non_function(self):
        def inner():
            job = ppg.DependencyInjectionJob('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.DependencyInjectionJob(5, lambda: 1)
        self.assertRaises(ValueError, inner)


class JobGeneratingJobTests(PPGPerTest):

    def test_basic(self):
        def gen():
            jobA = ppg.FileGeneratingJob('out/A', lambda : write('out/A', 'A'))
            jobB = ppg.FileGeneratingJob('out/B', lambda : write('out/B', 'B'))
            jobC = ppg.FileGeneratingJob('out/C', lambda : write('out/C', 'C'))
        genjob = ppg.JobGeneratingJob('genjob', gen)
        ppg.run_pipegraph()
        self.assertTrue(read('out/A'), 'A')
        self.assertTrue(read('out/B'), 'B')
        self.assertTrue(read('out/C'), 'C')

    def test_raises_if_needs_more_cores_than_we_have(self):
        def gen():
            jobA = ppg.FileGeneratingJob('out/A', lambda : write('out/A', 'A'))
            jobA.cores_needed = 20000
        genjob = ppg.JobGeneratingJob('genjob', gen)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists('out/A')) #since the gen job crashed
        jobGenerated = ppg.util.global_pipegraph.jobs['out/A']
        self.assertTrue(jobGenerated.failed)
        self.assertEqual(jobGenerated.error_reason, "Needed to much memory/cores")

    def test_raises_if_needs_more_ram_than_we_have(self):
        def gen():
            jobA = ppg.FileGeneratingJob('out/A', lambda : write('out/A', 'A'))
            jobA.memory_needed = 1024 * 1024 * 1024 * 1024
        genjob = ppg.JobGeneratingJob('genjob', gen)
        try:
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists('out/A')) #since the gen job crashed
        jobGenerated = ppg.util.global_pipegraph.jobs['out/A']
        self.assertTrue(jobGenerated.failed)
        self.assertEqual(jobGenerated.error_reason, "Needed to much memory/cores")

    def test_injecting_multiple_stages(self):
        def gen():
            def genB():
                def genC():
                    jobD = ppg.FileGeneratingJob('out/D', lambda : write('out/D', 'D'))
                jobC = ppg.JobGeneratingJob('C', genC)
            jobB = ppg.JobGeneratingJob('B', genB)

        job = ppg.JobGeneratingJob('A', gen)
        ppg.run_pipegraph()
        self.assertTrue(read('out/D'), 'D')

    def test_generated_job_depending_on_each_other_one_of_them_is_Invariant(self):
        #basic idea. You have jobgen A, 
        #it not only creates filegenB, but also ParameterDependencyC that A depends on
        #does that work
        def gen():
            jobB = ppg.FileGeneratingJob('out/B', lambda : write('out/B', 'B'))
            jobB.ignore_code_changes()
            jobC = ppg.ParameterInvariant('C', ('ccc',))
            jobB.depends_on(jobC)
        jobA = ppg.JobGeneratingJob("A", gen)
        ppg.run_pipegraph()
        self.assertEqual(read('out/B'), 'B')

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def gen2():
            jobB = ppg.FileGeneratingJob('out/B', lambda : write('out/B', 'C'))
            jobB.ignore_code_changes()
            jobC = ppg.ParameterInvariant('C', ('ccc',))
            jobB.depends_on(jobC)
        jobA = ppg.JobGeneratingJob("A", gen2)
        ppg.run_pipegraph()
        self.assertEqual(read('out/B'), 'B') #no rerun

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def gen3():
            jobB = ppg.FileGeneratingJob('out/B', lambda : write('out/B', 'C'))
            jobB.ignore_code_changes()
            jobCX = ppg.ParameterInvariant('C', ('DDD',))
            jobB.depends_on(jobCX)
        jobA = ppg.JobGeneratingJob("A", gen3)
        ppg.run_pipegraph()
        self.assertEqual(read('out/B'), 'C') #did get rerun


    def test_generated_job_depending_on_job_that_cant_have_finished(self):
        #basic idea. You have jobgen A, and filegen B.
        #filegenB depends on jobgenA.
        #jobGenA created C depends on filegenB
        #Perhaps add a filegen D that's independand of jobGenA, but C also deps on D
        def a():
            jobB = ppg.FileGeneratingJob('out/B', lambda: write('out/B', 'B'))
            def genA():
                jobC = ppg.FileGeneratingJob('out/C', lambda: write('out/C', 'C'))
                jobC.depends_on(jobB)
            jobA = ppg.JobGeneratingJob('A', genA)
            jobB.depends_on(jobA)
            ppg.run_pipegraph()
            self.assertEqual(read('out/B'), 'B')
            self.assertEqual(read('out/C'), 'C')
        def b():
            jobB = ppg.FileGeneratingJob('out/B', lambda: write('out/B', 'B'))
            jobD = ppg.FileGeneratingJob('out/D', lambda: write('out/D', 'D'))
            def genA():
                jobC = ppg.FileGeneratingJob('out/C', lambda: write('out/C', 'C'))
                jobC.depends_on(jobB)
                jobC.depends_on(jobD)
            jobA = ppg.JobGeneratingJob('A', genA)
            jobB.depends_on(jobA)
            ppg.run_pipegraph()
            self.assertEqual(read('out/B'), 'B')
            self.assertEqual(read('out/C'), 'C')
        a()
        ppg.new_pipegraph(rc_gen(), quiet=True)
        b()

    def test_generated_job_depending_on_each_other(self):
        #basic idea. You have jobgen A, 
        #it not only creates filegenB, but also filegenC that depends on B
        #does that work
        def gen():
            jobB = ppg.FileGeneratingJob('out/B', lambda: write('out/B', 'B'))
            jobC = ppg.FileGeneratingJob('out/C', lambda: write('out/C', read('out/B')))
            jobC.depends_on(jobB)
        jobA = ppg.JobGeneratingJob('A', gen)
        ppg.run_pipegraph()
        self.assertEqual(read('out/B'), 'B')
        self.assertEqual(read('out/C'), 'B')

    def test_generated_job_depending_on_each_other_one_of_them_is_loading(self):
        #basic idea. You have jobgen A, 
        #it not only creates filegenB, but also DataloadingC that depends on B
        #does that work
        def gen():
            def load():
                global shu
                shu = "123"
            def do_write():
                global shu
                write('out/A', shu)
            dl = ppg.DataLoadingJob('dl', load)
            jobB = ppg.FileGeneratingJob('out/A', do_write)
            jobB.depends_on(dl)
        ppg.JobGeneratingJob('gen', gen)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), '123')

    def test_passing_non_function(self):
        def inner():
            job = ppg.JobGeneratingJob('out/a', 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.JobGeneratingJob(5, lambda: 1)
        self.assertRaises(ValueError, inner)


import exptools # i really don't like this, but it seems to be the only way to test this
exptools.load_software('pyggplot')


class PlotJobTests(PPGPerTest):
    def setUp(self):
        PPGPerTest.setUp(self)

    def test_basic(self):
        import pydataframe
        import pyggplot
        def calc():
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.png'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)

    def test_pdf(self):
        import pydataframe
        import pyggplot
        def calc():
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.pdf'
        job = ppg.PlotJob(of, calc, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PDF document') != -1)

    def test_raises_on_invalid_filename(self):
        import pydataframe
        import pyggplot
        def calc():
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
        def plot(df):
            return pyggplot.Plot(df).add_scatter('X','Y')
        of = 'out/test.shu'
        def inner():
            job = ppg.PlotJob(of, calc, plot)
        self.assertRaises(ValueError, inner)


    def test_reruns_just_plot_if_plot_changed(self):
        import pydataframe
        import pyggplot
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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
        import pyggplot
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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
        import pyggplot
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
        job = ppg.PlotJob(of, calc2, plot)
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'AA')
        self.assertEqual(read('out/plot'),'BB')

    def test_no_rerun_if_calc_change_but_ignore_codechanges(self):
        import pydataframe
        import pyggplot
        def calc():
            append('out/calc', 'A')
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
        job = ppg.PlotJob(of, calc2, plot)
        job.ignore_code_changes()
        ppg.run_pipegraph()
        self.assertTrue(magic(of).find('PNG image') != -1)
        self.assertEqual(read('out/calc'),'A')

        self.assertEqual(read('out/plot'),'B')
    def test_plot_job_dependencies_are_added_to_just_the_cache_job(self):
        import pydataframe
        import pyggplot

        def calc():
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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
        import pyggplot
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
            return pydataframe.DataFrame({"X": range(0, 100), 'Y': range(50, 150)})
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

class CachedAttributeJobTests(PPGPerTest):
 
    def test_simple(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 100)))

    def test_preqrequisites_end_up_on_lfg(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        job_preq= ppg.FileGeneratingJob('out/B', do_write)
        job.depends_on(job_preq)
        self.assertFalse(job_preq in job.prerequisites)
        self.assertTrue(job_preq in job.lfg.prerequisites)


    def test_no_dependand_no_calc(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc)
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists('out/mycalc'))

    def test_invalidation_redoes_output(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 100)))

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def calc2():
            return ", ".join(str(x) for x in range(0, 200))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc2) #now, jobB should be deleted...
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 200)))

    def test_invalidation_ignored_does_not_redo_output(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 100)))

        ppg.new_pipegraph(rc_gen(), quiet=True)
        def calc2():
            return ", ".join(str(x) for x in range(0, 200))
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc2)
        job.ignore_code_changes()
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 100)))

        ppg.new_pipegraph(rc_gen(), quiet=True)
        job = ppg.CachedAttributeLoadingJob('out/mycalc', o, 'a', calc2)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 200))) #The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result

    def test_throws_on_non_function_func(self):
        o = Dummy()
        def calc():
            return 55
        def inner():
            x = ppg.CachedAttributeLoadingJob('out/mycalc', calc, o, 'a')
        self.assertRaises(ValueError, inner)

    def test_calc_depends_on_added_dependencies(self):
        o = Dummy()
        load_attr = ppg.AttributeLoadingJob('load_attr', o, 'o', lambda: 55)
        def calc():
            return o.o
        def out():
            write('out/A', str(o.o2))
        cached = ppg.CachedAttributeLoadingJob('out/cached_job', o , 'o2', calc)
        fg = ppg.FileGeneratingJob('out/A', out)
        fg.depends_on(cached)
        cached.depends_on(load_attr)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), '55')

    def test_depends_on_returns_self(self):
        ppg.new_pipegraph(rc_gen(), quiet=True)
        o = Dummy()
        jobA = ppg.CachedAttributeLoadingJob('out/A',o, 'shu', lambda : write('out/A', 'shu'))
        jobB = ppg.FileGeneratingJob('out/B' ,lambda : write('out/B', 'shu'))
        self.assertTrue(jobA.depends_on(jobB) is jobA)

    def test_passing_non_function(self):
        o = Dummy()
        def inner():
            job = ppg.CachedAttributeLoadingJob('out/a', o, 'a', 55)
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()
        def inner():
            job = ppg.CachedAttributeLoadingJob(5, o, 'a', lambda: 55)
        self.assertRaises(ValueError, inner)


class CachedDataLoadingJobTests(PPGPerTest):

    def test_simple(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        def store(value):
            o.a = value
        job = ppg.CachedDataLoadingJob('out/mycalc', calc, store)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run_pipegraph()
        self.assertEqual(
                read(of),
                ", ".join(str(x) for x in range(0, 100)))

    def test_no_dependand_no_calc(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        def store(value):
            o.a = value
        job = ppg.CachedDataLoadingJob('out/mycalc', calc, store) 
        #job.ignore_code_changes() #or it would run anyway... hm.
        self.assertFalse(os.path.exists('out/mycalc'))
        ppg.run_pipegraph()
        self.assertFalse(os.path.exists('out/mycalc'))

    def test_preqrequisites_end_up_on_lfg(self):
        o = Dummy()
        def calc():
            return ", ".join(str(x) for x in range(0, 100))
        def store(value):
            o.a = value
        job = ppg.CachedDataLoadingJob('out/mycalc', calc, store)
        of = 'out/A'
        def do_write():
            write(of, o.a)
        jobB = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        job_preq= ppg.FileGeneratingJob('out/B', do_write)
        job.depends_on(job_preq)
        self.assertFalse(job_preq in job.prerequisites)
        self.assertTrue(job_preq in job.lfg.prerequisites)

    def test_passing_non_function_to_calc(self):
        def inner():
            job = ppg.CachedDataLoadingJob('out/a', 'shu', lambda value: 55)
        self.assertRaises(ValueError, inner)

    def test_passing_non_function_to_store(self):
        def inner():
            job = ppg.CachedDataLoadingJob('out/a', lambda value: 55, 'shu')
        self.assertRaises(ValueError, inner)

    def test_passing_non_string_as_jobid(self):
        def inner():
            job = ppg.CachedDataLoadingJob(5, lambda: 1, lambda value: 55)
        self.assertRaises(ValueError, inner)


class TestResourceCoordinator:
    def __init__(self, list_of_slaves):
        """List of slaves entries are tuples of (name, number of cores, megabytes of memory)"""
        self.slaves = list_of_slaves
        



if False:
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

class CantDepickle():
    """A class that can't be depickled (throws a type error, 
    just like the numpy.maskedarray sometimes)"""
    def __getstate__(self):
        return ['5']

    def __setstate__(self, state):
        print state
        raise TypeError("I can be pickled, but not unpickled")

class TestingTheUnexpectedTests(PPGPerTest):

    def test_job_killing_python(self):
        def dies():
            import sys
            #logging.info("Now terminating child python")
            sys.exit(5)
        fg = ppg.FileGeneratingJob('out/A', dies)
        try:
            ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        self.assertFalse(os.path.exists('out/A'))
        self.assertTrue(isinstance(fg.exception, ppg.JobDiedException))
        self.assertEqual(fg.exception.exit_code, 5)

    def test_unpickle_bug_prevents_single_job_from_unpickling(self):
        def do_a():
            write('out/A', 'A')
            append("out/As", 'A')
        job_A = ppg.FileGeneratingJob('out/A', do_a)
        def do_b():
            write('out/B', 'A')
            append("out/Bs", 'A')
        job_B = ppg.FileGeneratingJob('out/B', do_b)
        cd = CantDepickle()
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd, ))
        job_B.depends_on(job_parameter_unpickle_problem)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), 'A')
        self.assertEqual(read('out/As'), 'A')
        self.assertEqual(read('out/B'), 'A')
        self.assertEqual(read('out/Bs'), 'A')
        ppg.new_pipegraph()

        job_A = ppg.FileGeneratingJob('out/A', do_a)
        job_B = ppg.FileGeneratingJob('out/B', do_b)
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd, ))
        job_B.depends_on(job_parameter_unpickle_problem)
        ppg.run_pipegraph()
        self.assertEqual(read('out/A'), 'A')
        self.assertEqual(read('out/As'), 'A')
        self.assertEqual(read('out/B'), 'A')
        self.assertEqual(read('out/Bs'), 'AA') #this one got rerun because we could not load the invariant...


class HTMLDumpTests(PPGPerTest):

    def test_html_dumping_on_failure(self):
        fg = ppg.FileGeneratingJob('out/A', lambda: write('out/A', 'A'))
        ppg.run_pipegraph()
        self.assertTrue(os.path.exists('logs/pipegraph_status.html'))


class NotYetImplementedTests(unittest.TestCase):

    def test_large_job_descriptions(self):
        #break the 65k barrier on the AMP values...
        raise NotImplementedError()
    
    def test_chained_dataloading_jobs(self):
        raise NotImplementedError()


    def test_spawn_slave_failure(self):
        raise NotImplementedError()


    def test_invariant_dumping_on_failure(self):
        """When is it ok to update them?"""
        raise NotImplementedError()

    def test_two_attribute_loading_jobs_sharing_lfg(self):
        #this will probably fail because they not only share the calculating job,
        #but also the loading job... Maybe refactor the lfg creation?
        return NotImplementedError()


    def test_prev_dataloading_jobs_not_done_if_there_is_a_non_dataloading_job_inbetween_that_is_done(self):
        # so, A = DataLoadingJob, B = FileGeneratingJob, C = DataLoadingJob, D = FileGeneratingJob
        # D.depends_on(C)
        # C.depends_on(B)
        # B.depends_on(A)
        # B is done.
        # D is not
        # since a would be loaded, and then cleaned up right away (because B is Done)
        # it should never be loaded
        raise NotImplementedError()

    def test_cached_jobs_get_depencies_only_on_the_lazy_filegenerator_not_on_the_loading_job(self):
        raise NotImplementedError()

    def test_generated_jobs_that_can_not_run_right_away_because_of_dataloading_do_not_crash(self):
        raise NotImplementedError()

    def test_jobs_that_need_all_cores_are_spawned_one_by_one(self):
        raise NotImplementedError()

    def test_multi_filegenerating_job_exepctions_are_preserved(self):
        raise NotImplementedError()

    def test_filegenerating_job_exceptions_are_preserved(self):
        raise NotImplementedError()

    def test_cached_attribute_job_does_not_load_its_preqs_on_cached():
        raise NotImplementedError()

    def test_cached_dataloading_job_does_not_load_its_preqs_on_cached():
        raise NotImplementedError()




if __name__ == '__main__':
    unittest.main()
    print' left unittest.main()'
