from scheduler.config_loader import *

class Feeder(object):
    def __init__(self):
        self.file_path = os.path.join(get_parent_path(),config["feeder_file_path"])

    def execute(self):
      with open(self.file_path, "r") as f:
          for l in f:
            lst = l.split(",")
            if(lst[0]=="S13_D1_V547"):
                break
            yield lst
