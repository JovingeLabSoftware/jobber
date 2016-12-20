import q
import mock
import unittest
import os
from subprocess import Popen
import time
from nose.tools import set_trace;

log = None;

def setup_module():
    global log
    log = open("tests.log", 'w')
    
def teardown_module():
    log.close()


class QTest(unittest.TestCase):
    def setUp(self):
        self.q = q.Q()
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
        
    @mock.patch("q.subprocess.Popen", side_effect=mock_popen)
    @mock.patch("q.subprocess")
    def test_start(self, mock_popen, mock_subprocess):
        for i in range(4,14):
            self.q.enqueue("file" + str(i) + ".txt")
        self.q.start()

        self.assertEqual(self.q.running, 10)
        time.sleep(1)

        self.assertEqual(self.q.running, 10)
        time.sleep(3)

        self.assertEqual(self.q.running, 0)
        self.assertEqual(self.q.status(), 3)

        time.sleep(5)
        self.assertEqual(self.q.running, 0)
        self.assertEqual(self.q.status(), 0)

        self.assertFalse(mock_subprocess.Popen.called, "Subprocess not called")
        self.q.stop(wait=False)
    
    

if __name__ == '__main__':
    unittest.main()