from q import Q
import mock, re, unittest, os
from subprocess import Popen
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

    def test_dequeue(self):
        self.q.dequeue("testpath.txt")
        self.assertEqual(len(self.q.items), 3)

    def mock_qsub(*args, **kwargs):
        id = "112543[].master.cm.cluster".split()
        id = re.sub("\..*", "", id[0])
        return id 

    @mock.patch("q.Q.qsub_start", side_effect=mock_qsub)
    def test_start(self, mock_qsub_start):
        self.q.start_server()
        self.assertEqual(self.q.get_job(), "file1.txt")
        for i in range(4,14):
            self.q.enqueue("file" + str(i) + ".txt")
        self.q.process()
        self.assertEqual(self.q.array_id, "112543[]")
        self.q.stop_server()

    @nottest
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
        self.assertEqual(self.q.qsub_start([]), "999999")
        self.assertTrue(self.q.check_qstat("99493"))
        self.assertTrue(mock_run.called, "Subprocess not called")
    

if __name__ == '__main__':
    unittest.main()