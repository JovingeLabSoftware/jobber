import pdb, sys, time, os, sched, time, subprocess, warnings, re
import watchdog.events
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Timer

#
# How many seconds to allow system to "settle" after queue
# changes.  This helps ensure a sane execution environment
# when multiple (and potentially interdependant) files are 
# moved into processing directory.  It also reduces the load
# of the queue itself by only updating every Q_SETTLE seconds.
#
Q_SETTLE = 3

#
# How many simultaneous jobs to allow
#
Q_MAXJOBS = 3

class Q(FileSystemEventHandler):
    def __init__(self):
        self.items = {}
        self.running = 0
        self.settled = None
        self.script = None
        self.pattern = ""
        super(Q, self).__init__()

    def isEmpty(self):
        return self.items == []

    def enqueue(self, path):
        if re.search(self.pattern, path):
            if(path not in self.items.keys()):
                self.items[path] = {"job": None, "exec": os.path.abspath(self.script), "running": False}
                # specificying exec for every job is redundant since it is the 
                # same for all jobs, but that could change in the future.

    def dequeue(self, path):
        if(path in self.items.keys()):
            del self.items[path]

    def size(self):
        return len(self.items)

    def on_created(self, event):
        path = os.path.abspath(event.src_path)
        if not self.script:
            warnings.warn("No script specified so skipping newly added files")
        elif re.match(self.pattern, event.src_path):
            self.enqueue(path, self.script)
            if(self.settled):
                self.settled.cancel()
            self.settled = Timer(Q_SETTLE, self.process)
            self.settled.start()
        return 0

    def on_deleted(self, event):
        self.dequeue(event.src_path)
        return 0

    def process(self):
        self.update()
        for k in self.items:
            if not self.items[k]["running"]:
                if self.running >= Q_MAXJOBS:
                    break
                i = self.items[k]
                i["job"] = subprocess.Popen([i["exec"], k])
                i["runnning"] = True
                self.running = self.running + 1

        if(self.settled):
            self.settled.cancel()
        self.settled = Timer(Q_SETTLE, self.process)
        self.settled.start()

        return 0
    
    def status(self):
        self.update()
        return(self.size())
        
    def start(self):
        self.process()
        return 0

    #
    # if wait, will complete queued jobs before
    # exiting.  Otherwise will terminate immediately
    #
    def stop(self, wait=False):
        if wait and self.size > 0:
            Timer(Q_SETTLE, self.stop, [True])
        else:
            if(self.settled):
                self.settled.cancel()
            for k in self.items:
                if self.items[k]["running"]:
                    i = self.items[k]
                    i["job"].terminate()
                    i["running"] = False
                    self.running = self.running + 1
        return 0
        

    def update(self):
        dq = []
        for k in self.items:
            i = self.items[k]
            if i["job"]:
                if i["job"].poll() is 0:
                    dq.append(k)
                    self.running = self.running - 1
        for k in dq:
            self.dequeue(k)
            
                
            

        
