from scheduler.config_loader import *


class Feeder(object):
    def __init__(self):
        self.file_path = config["feeder_file_path"]

    def execute(self):
        with open(self.file_path, "r") as f:
            for l in f:
                lst = l.split(",")
                vm_id, time_stamp, num_cores, ram_needed, c_d = lst
                time_stamp = float(time_stamp)
                if time_stamp < 172800:
                    yield lst
                else:
                    return
