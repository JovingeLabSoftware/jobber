from q import Q
import mock
import unittest
import os
from subprocess import Popen
import time
from nose.tools import set_trace
from nose.tools import nottest

log = None;

def setup_module():
    global log
    log = open("tests.log", 'w')
    
def teardown_module():
    log.close()


class QTest(unittest.TestCase):
    def setUp(self):
        self.q = Q()
        self.q.throttle = {'settle': 1, 'pause': 2, 'maxjobs': 10} 
        self.q.script = "test.sh"
        self.q.pattern = "txt"
        self.q.template = "templates/job.qsub"
        for i in range(3):
            self.q.enqueue("file" + str(i) + ".txt")

    def mock_popen(*args, **kwargs):
        cwd = os.getcwd()
        cwd = cwd.replace("/tests", "") + "/tests/"
        args = list(args[0])
        p = Popen([cwd + "mock_popen.sh"] + args, stdout = log)
        return p
        
    def test_enqueue(self):
        self.assertEqual(self.q.size(), 3)
        self.q.enqueue("testpath.txt")
        self.assertEqual(self.q.size(), 4)
        self.q.enqueue("testpath.gz")
        self.assertEqual(self.q.size(), 4)

    @mock.patch("q.subprocess.Popen.poll")
    def test_dequeue(self, mock_subprocess_poll):
        self.q.dequeue("testpath.txt")
        self.assertEqual(len(self.q.items), 3)
        self.assertFalse(mock_subprocess_poll.called, "Unexpected call to subprocess.poll.")
        
    @nottest
    @mock.patch("q.subprocess.Popen", side_effect=mock_popen)
    @mock.patch("q.subprocess")
    def test_start(self, mock_popen, mock_subprocess):
        for i in range(4,14):
            self.q.enqueue("file" + str(i) + ".txt")
        self.q.process()

        time.sleep(1)
        self.assertEqual(self.q.running, 10)
        self.assertEqual(self.q.size(), 13)
        time.sleep(3)
        self.assertEqual(self.q.running, 0)
        self.assertEqual(self.q.size(), 3)
        time.sleep(2)
        self.assertEqual(self.q.running, 3)
        self.assertEqual(self.q.size(), 3)
        time.sleep(4)
        self.assertEqual(self.q.running, 0)
        self.assertEqual(self.q.size(), 0)
        self.assertFalse(mock_subprocess.Popen.called, "Subprocess not called")

        self.q.stop(wait=True)
    
    def mock_run(*args, **kwargs):
        c = args[0][0]
        if c is "qstat":
            # simulate return of qstat data
            fn = os.getcwd()
            fn = fn.replace("/tests", "") + "/tests/qstat.txt"
            with open(fn) as f:
                return(f.readlines())    
        if c is "qsub":
            # simulate qsub job starting
            return(["999999.master.cm.cluster"])

    @mock.patch("q.Q.runCmd", side_effect=mock_run)
    def test_check_qstat(self, mock_run):
        self.assertEqual(self.q.qsub_start([]), "9999989")
        self.assertTrue(self.q.check_qstat("99493"))
        self.assertTrue(mock_run.called, "Subprocess not called")
    

if __name__ == '__main__':
    unittest.main()