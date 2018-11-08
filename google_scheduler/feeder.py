from scheduler.config_loader import *
import os
import sys
class Feeder(object):
    def __init__(self):
        self.file_path = os.path.join(get_parent_path(),config["feeder_file_path"])

    def execute(self):
        count=0
        print self.file_path
        with open(self.file_path, "r") as f:
            for l in f:
                count+=1
                if count%1000000==0:
                    print count
                    sys.stdout.flush()
                lst = l.split(",")
                if float(lst[3])>0 and float(lst[4])>0:
                    yield (lst[0],lst[1],lst[2],lst[3],lst[4],lst[5])

