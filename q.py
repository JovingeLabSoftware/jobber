"""
.. module:: Q
   :platform: Unix
   :synopsis: Throttle qsub job submission to play nicely with others while
              automating large number of jobs.

.. moduleauthor:: Eric Kort <eric.kort@vai.org>


"""

import pdb, sys, time, os, sched, time, subprocess, warnings, re, tempfile
import watchdog.events
from string import Template
from threading import Timer
from watchdog.events import FileSystemEventHandler

# does our version of qsub support -sync t ?
SYNC=False

class Q(FileSystemEventHandler):
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
        self.items = {}
        self.running = 0
        self.throttle = {'settle': 2, 'pause': 10, 'maxjobs': 10}
        self.timer = None
        self.script = None
        self.pattern = ""
        self.template = None
        super(Q, self).__init__()


    def reset(self, t="settle", next="update"):
        """
        Restart the throttling timer
        
        :param t: What kind of throtte: "settle" or "pause"
        :type t: str ["settle", "pause"]
        :param next: What to do when timer fires: "update" or "process"
        :type next: str ["update", "process"]
        :returns: 0.  Called for side effect of resetting timer
        """ 
        if(self.timer):
            self.timer.cancel()
        if next == "update":
            self.timer = Timer(self.throttle[t], self.update)
        elif next == "process":
            self.timer = Timer(self.throttle[t], self.process)
        self.timer.start()
        return 0

    def enqueue(self, path):
        """
        Add a file to process queue
        
        :param path: Complete path of file to add to queue
        :type path: str
        :returns: 0.  Called for side effect of adding work to the queue
        """
        if re.search(self.pattern, path):
            if(path not in self.items.keys()):
                self.items[path] = {"job": None, "exec": os.path.abspath(self.script), "running": False}
                # specificying exec for every job is redundant since it is the 
                # same for all jobs, but that could change in the future.
        return 0

    def dequeue(self, path):
        """
        Remove a file to process queue.  
        
        
        Checks to see if there is an actively running process for this item and 
        if so attempts to terminate the process before removing item from queue.

        :param path: Complete path of file to remove from queue
        :type path: str
        :returns: 0.  Called for side effect of removing work from the queue
        """
        if(path in self.items.keys()):
            i = self.items[path]
            if SYNC:
                if i["job"]:
                    if i["job"].poll():
                        i["job"].terminate()
            else:
                if i["job"]:
                    if self.check_qstat(i["job"]):
                        self.qsub_del(i["job"])
            del self.items[path]
        return 0
    
    def size(self):
        """
        Determine how large queue currently is
        
        :returns: size of queue (int).  If 0, queue has completed its work
        """
        return len(self.items)

    def on_created(self, event):
        """
        FileSystemEventHandler hook for file creation
        :param event: Event object describind file creation event
        :type event: object
        :returns: 0.  Called for side effect of adding work to the queue
        :Example:
        
        from q import Q
        from watchdog.observers import Observer
        q = Q()
        observer = Observer()
        observer.schedule(q, path, recursive=True)
        observer.start()
        """
        path = os.path.abspath(event.src_path)
        if not self.script:
            warnings.warn("No script specified so skipping newly added files")
        elif re.match(self.pattern, event.src_path):
            self.enqueue(path, self.script)
            self.reset("settle")
        return 0

    def on_deleted(self, event):
        """
        FileSystemEventHandler hook for file deletion
        :param event: Event object describing file deletion event
        :type event: object

        Results in  a call to dequeue and therefore will attempt to gracefully
        terminate active job, if any, prior to removing item from queue.
        :returns: 0.  Called for side effect of removing work from the queue
        """
        self.dequeue(event.src_path)
        return 0

    def process(self):
        """
        Process jobs on the queue
        
        If we are within the bounds of the queue's :code:`throttle` limits,
        start jobs that are on queued.
        :returns: 0.  Called for side effect of starting jobs
        """
        for k in self.items:
            if not self.items[k]["running"]:
                if self.running >= self.throttle["maxjobs"]:
                    break

                i = self.items[k]

                src = Template(open(self.template).read())
                d = {'script': i["exec"],
                     'dir': os.path.dirname(k),
                     'file': k}
                qsub = src.substitute(d)

                f = tempfile.NamedTemporaryFile(mode='w', suffix='.qsub', dir='temp')
                f.write(qsub)

                i["qsub"] = f
                if SYNC:
                    i["job"] = subprocess.Popen(["echo", "-sync", "y", f.name])
                else:
                    i["job"] = self.qsub_start([f.name])
                i["running"] = True
                self.running = self.running + 1

        self.reset()
        return 0
    
    def status(self):
        return(self.size())
        

    #
    # if wait, will complete queued jobs before
    # exiting.  Otherwise will terminate immediately
    #
    def stop(self, wait=False):
        """
        Stop the queue 

        Stop the queue, optionally after all jobs complete.  Otherwise, 
        running jobs will be terminated.
        :param wait: whether to wait for jobs to complete.
        :type wait: bool
        :returns: 0.  Called for side effect of stopping the queue
        """
        if wait and self.size > 0:
            Timer(self.throttle["settle"], self.stop, [True])
        else:
            if(self.timer):
                self.timer.cancel()
            for k in list(self.items):
                if self.items[k]["job"]:
                    i = self.items[k]
                    if SYNC:
                        i["job"].terminate()
                    else:
                        self.qsub_del(i["job"])
                    i["running"] = False
                    self.running = self.running - 1
                self.dequeue(k)
        return 0
        

    def update(self):
        """
        Update the queue 

        Purges completed jobs, and resets timers depending on whether there 
        are more active jobs (settle) or not (pause).
        :returns: 0.  Called for side effect of updating the queue
        """
        dq = []
        for k in self.items:
            i = self.items[k]
            if i["job"]:
                if SYNC:
                    if i["job"].poll() is 0:
                        dq.append(k)
                        self.running = self.running - 1
                else:
                    if not self.check_qstat(i["job"]):
                        dq.append(k)
                        self.running = self.running - 1
                i["qsub"].close()
        for k in dq:
            self.dequeue(k)
        if self.running == 0 and self.size() > 0:
            self.reset(t="pause", next="process")
        elif self.running > 0:
            self.reset(t="settle", next="update")
        
    def runCmd(self, command):
        """
        Run a command and capture the output 
        
        Got this from SO question 4760215: https://goo.gl/Ii2pdw. Kudos to 
        JF Sebastian and Max Ekman
        
        :returns: Iterator providing the command output
        """
        print(command)
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
            if id == job:
                return True
        return False            
        
    def qsub_start(self, args):
        """
        If -sync t option not available for our version of qsub, we 
        need to capture the job id so we can monitor its progress and 
        also terminate it if needed.
        
        :returns: id of job
        """ 
        job = self.runCmd(["qsub"] + args)
        id = next(x for x in job)
        print(id)
        id = id.split()[0]
        print(id)
        id = re.sub("\..*", "", id[0])
        return id

    def qsub_del(self, id):
        """
        If -sync t option not available for our version of qsub, we 
        need to manually delete the jobs from the qsub queue 

        :returns: 0.  Called for side effect of terminating jobs
        """ 
        runCmd(["qdel", id])
        return 0
