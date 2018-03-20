from scheduler.config import *


class Feeder(object):
    def __init__(self):
        self.file_path = feeder_file_path

    def execute(self):
        with open(self.file_path, "r") as f:
            for l in f:
                lst = l.split(",")
                yield lst
