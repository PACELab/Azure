from scheduler.config_loader import *
import os
import sys
class Feeder(object):
    def __init__(self,offset=0,max_time=float('inf')):
        self.file_path = os.path.join(get_parent_path(),config["feeder_file_path"])
        self.offset=offset
        self.max_time=max_time

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
                    lst[2]=float(lst[2])+self.offset
                    if lst[2]>self.max_time:
                        break
                    yield (lst[0],lst[1],lst[2],lst[3],lst[4],lst[5])

