#!/usr/bin/python
#
# Submit qsub jobs in batches with throttling and limits
#
# You must provide a script to run on each file which must accept
# exactly one argument (the data file to process)
#
# Note that the directory holding the data files is searched 
# recursively and all files are processed regardless of "depth"
#


import pdb, sys, getopt, time, logging, os, re
import watchdog.events

from q import Q
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

def main(argv):
    watch = False
    pattern = ""
    script = None
    f = lambda x: bool(re.search(pattern, x))
    q = Q()

    try:
        opts, args = getopt.getopt(argv, "wp:s:")
    except getopt.GetoptError:
        print 'Usage: jobber.py -s /your/script.sh [-m maxjobs] [-P port] [-p pattern] [/path/to/data/directory]'
        sys.exit(0)
        
    q.template = os.path.abspath("templates/job.qsub")
    throttle = {'settle': 2, 'pause': 1, 'maxjobs': 5}
    for opt, arg in opts:
        if opt == '-w':
            watch = True
        if opt == '-P':
            q.port = arg
        if opt == '-p':
            q.pattern = arg
        if opt == '-s':
            q.script = arg
        if opt == '-m':
            q.maxjobs =  arg

    if not q.script:
        print 'No script specified (-s options is required).  Exiting...'
        sys.exit(0)

    q.throttle = throttle

    path = args[0] if len(args) > 0 else '.'

    filelist = []

    for root, dirs, files in os.walk(os.path.abspath(path)):
        for file in files: 
            filelist.append(os.path.abspath(os.path.join(root, file)))
        
    for f in filelist:
        q.enqueue(f)

    q.start_server()
    q.process()

    while q.running():
        q.poll()

    q.stop_server();

    return(0)
    

if __name__ == "__main__":
    main(sys.argv[1:])






