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
            if i["job"]:
                if i["job"].poll():
                    i["job"].terminate()
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
                i["job"] = subprocess.Popen(["qsub -sync y", f.name, k])
                i["running"] = True
                self.running = self.running + 1

        self.reset()
        return 0
    
    def status(self):
        return(self.size())
        
    def start(self):
        self.process()
        return 0

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
            for k in self.items:
                if self.items[k]["job"]:
                    i = self.items[k]
                    i["job"].terminate()
                    i["running"] = False
                    self.running = self.running - 1
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
                if i["job"].poll() is 0:
                    dq.append(k)
                    i["qsub"].close()
                    self.running = self.running - 1
        for k in dq:
            self.dequeue(k)
        if self.running == 0 and self.size() > 0:
            self.reset(t="pause", next="process")
        elif self.running > 0:
            self.reset(t="settle", next="update")
        