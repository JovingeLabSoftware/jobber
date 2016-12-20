import pdb, sys, time, os, sched, time, subprocess, warnings, re
import watchdog.events
import tempfile
import atexit
from string import Template
from threading import Timer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from nose.tools import set_trace;
#
# How many seconds to allow system to "settle" after queue
# changes.  This helps ensure a sane execution environment
# when multiple (and potentially interdependant) files are 
# moved into processing directory.  It also reduces the load
# of the queue itself by only updating every Q_SETTLE seconds.
#


class Q(FileSystemEventHandler):
    def __init__(self):
        _items = {}
        self.items = {}
        self.running = 0
        self.throttle = {'settle': 2, 'pause': 10, 'maxjobs': 10}
        self.settled = None
        self.script = None
        self.pattern = ""
        self.template = None
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
            i = self.items[path]
            if i["job"]:
                if i["job"].poll():
                    i["job"].terminate()
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
            self.settled = Timer(self.throttle["settle"], self.update)
            self.settled.start()
        return 0

    def on_deleted(self, event):
        self.dequeue(event.src_path)
        return 0

    def process(self):
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

        if(self.settled):
            self.settled.cancel()
        self.settled = Timer(self.throttle["settle"], self.update)
        self.settled.start()

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
        if wait and self.size > 0:
            Timer(self.throttle["settle"], self.stop, [True])
        else:
            if(self.settled):
                self.settled.cancel()
            for k in self.items:
                if self.items[k]["job"]:
                    i = self.items[k]
                    i["job"].terminate()
                    i["running"] = False
                    self.running = self.running - 1
        return 0
        

    def update(self):
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
        if(self.settled):
            self.settled.cancel()
        if self.running == 0 and self.size() > 0:
            self.settled = Timer(self.throttle["pause"], self.process)
            self.settled.start()
        elif self.running > 0:
            self.settled = Timer(self.throttle["settle"], self.update)
            self.settled.start()
        