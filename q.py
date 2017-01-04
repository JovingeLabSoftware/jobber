"""
.. module:: Q
   :platform: Unix
   :synopsis: Throttle qsub job submission to play nicely with others while
              automating large number of jobs.

.. moduleauthor:: Eric Kort <eric.kort@vai.org>


"""

import pdb, sys, time, os, sched, time, subprocess, warnings, re, tempfile
import socket, select
from string import Template
from threading import Timer
import logging



dir = os.path.dirname(os.path.abspath(__file__))
TEMP_ROOT = os.path.join(dir, "temp" )
TEMPLATE_ROOT = os.path.join(dir, "./templates" )



class Q():
    def __init__(self):
        """
        The Q Class
        
        This module is designed to allow more granular control of job submission
        than qsub itself provides.  Specifically, you can submit jobs in 
        batches with set pauses in between batches and set size of batches.
        This can provide useful automation allowing submission of huge numbers
        of jobs without annoying other users.
        
        The following attributes can be get and set directly:
        
        * throttle: Dict with following slots: settle, pause, maxjobs
        * script: (Required) script that each qsub job should execute 
                  with the target file as the only argument
        * pattern: file pattern to filter discovered files with
        * template: path to qsub job file template.
        
        """         
        self.script = None
        self.pattern = ""
        self.template = "templates/array.qsub"
        self.items = {}
        self.maxjobs = 50
        self.keep_files = False
        self.array_id = None
        self.verbose = False
        
        # job related files
        self.qsub_file = None
        self.wrapper_file = None

        # TCP/IP spool
        self.connections = []
        self.recv_buffer_size = 4096
        self.port = 8002
        self.server_socket = None

        self.logger = self.initLogger()
        

    #     
    # Queue operations
    #

    def enqueue(self, path):
        """
        Add a file to process queue
        
        :param path: Complete path of file to add to queue
        :type path: str
        :returns: 0.  Called for side effect of adding work to the queue
        """
        if re.search(self.pattern, path):
            self.logger.info("queued " + path)
            if(path not in self.items.keys()):
                self.items[path] = {"job": None, "status": "waiting"}
        return 0

    def dequeue(self, path):
        """
        Remove a file to process queue.  
        
        
        If the job is already dispatched to the spooler, it is too late.  In 
        this case, dequeue will fail silently.
        
        TODO: Error handling for above case.

        :param path: Complete path of file to remove from queue
        :type path: str
        :returns: 0.  Called for side effect of removing work from the queue
        """
        if(path in self.items.keys()):
            i = self.items[path]
            if not i["job"]:
                del self.items[path]
        return 0
        

    def process(self):
        """
        Process jobs on the queue
        
        If we are within the bounds of the queue's :code:`throttle` limits,
        start jobs that are enqueued.
        :returns: 0.  Called for side effect of starting jobs
        """
        d = {'port': self.port}
        self.wrapper_file = self.temp_file("jobber.sh", d, ".sh", True)


        d = {'array_size': len(self.items),
             'wrapper': self.wrapper_file.name,
             'script': self.script,
             'chunk_size': min(len(self.items), self.maxjobs)}

        self.qsub_file = self.temp_file("array.qsub", d, ".qsub", False)
        self.logger.info("starting job: " + self.qsub_file.name)
        self.logger.info("total jobs queued: " + str(len(self.items)))
        self.logger.info("max simultaneous jobs: " + str(self.maxjobs))
        #self.array_id = self.qsub_start([self.qsub_file.name])

        return 0

    def running(self):
        """
        Check queue status
        
        Determine whether there are actively running jobs

        :returns: bool.  
        """
        if self.array_id:
            if self.verbose: print("Checking status of job id " + self.array_id)
            status = self.check_qstat(self.array_id)
            if not status:
                self.array_id = None
                return False
            else:
                return True
        else:
            return False


    def stop(self):
        """
        Stop the queue 

        Cancel the array job if running.
        :returns: 0.  Called for side effect of cancelling the job
        """
        if self.array_id:
            self.qdel(self.array_id)
            self.array_id = None
        return 0
        

 #
 # Interface to qsub ecosystem
 #
        

    def runCmd(self, command):
        """
        Run a command and capture the output 
        
        Got this from SO question 4760215: https://goo.gl/Ii2pdw. Kudos to 
        JF Sebastian and Max Ekman
        
        :returns: Iterator providing the command output
        """
        p = subprocess.Popen(command,
             stdout=subprocess.PIPE,
             stderr=subprocess.STDOUT)
        content = iter(p.stdout.readline, '')
        return content

    def check_qstat(self, job):
        """
        If -sync t option not available for our version of qsub, we 
        need to monitor our jobs the hard way by capturing qstat output
        
        Based on Hooting's answer to SO question 32598754: 
        https://goo.gl/UwsVRE

        :returns: True if job still running (bool)
        """ 
        jobs = self.runCmd(["qstat"])
        for line in jobs:
            columns = line.split()
            id = re.sub("\..*", "", columns[0])
            if id == job and (columns[4] == "Q" or columns[4] == "R"):
                return True
        return False            
        
    def qsub_start(self, args):
        """
        Start job array and capture the job id so we can monitor its progress 
        and also terminate it if needed.
        
        :returns: id of job
        """ 

        job = self.runCmd(["qsub"] + args)
        id = next(x for x in job)
        id = re.sub("\..*", "", id.rstrip())
        self.logger.info("started job array with id " + id)
        return id

    def qsub_del(self, id):
        """
        If -sync t option not available for our version of qsub, we 
        need to manually delete the jobs from the qsub queue 

        :returns: 0.  Called for side effect of terminating jobs
        """ 
        runCmd(["qdel", id])
        return 0


#
# Convenience functions
#

    def size(self):
        """
        Determine how large queue currently is
        
        :returns: size of queue (int).  If 0, queue has completed its work
        """
        return len(self.items)


    def temp_file(self, template, d, suffix, executable=True):
        src = Template(open(TEMPLATE_ROOT + "/" + template).read())
        t = src.substitute(d)
        tf = tempfile.NamedTemporaryFile(mode='w', suffix=suffix, dir=TEMP_ROOT, delete=not self.keep_files)
        tf.write(t)
        tf.flush()
        if executable:
            self.chmodx(tf.name)
        return tf

    def chmodx(self, path):
        mode = os.stat(path).st_mode
        mode |= (mode & 0o444) >> 2  
        os.chmod(path, mode)


    def initLogger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('q.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return(logger)
    

#
# TCP/IP server operations
#
    def start_server(self):
        if self.server_socket:
            self.server_socket.close()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("0.0.0.0", self.port))
        self.server_socket.listen(100)
        self.connections.append(self.server_socket)
 
        self.logger.info("Jobber Spool started on port " + str(self.port))

    def stop_server(self):
        if self.server_socket:
            self.server_socket.close()
        self.logger.info("spool shutdown as requested. ")

    def get_job(self):
        for k in self.items:
            if self.items[k]["status"] == "waiting":
                if self.verbose: print("Job started: " + k)
                self.logger.info("spool yielded file for processing: " + k)
                self.items[k]["status"] = "running"
                return k
        return None
        
        
    def poll(self):
        read_sockets, write_sockets, error_sockets = select.select(self.connections,[],[],1)
 
        for sock in read_sockets:
             
            if sock == self.server_socket:
                sockfd, addr = self.server_socket.accept()
                self.connections.append(sockfd)

            else:
                try:
                    data = sock.recv(self.recv_buffer_size)
                    reg = re.compile("getjob", re.MULTILINE)
                    if reg.findall(data):
                        sock.send(self.get_job())
                        sock.close()
                        self.connections.remove(sock)
                 
                except:
                    self.logger.error("spool shut down UNEXPECTEDLY. " + k)
                    sock.close()
                    self.connections.remove(sock)
                    continue

