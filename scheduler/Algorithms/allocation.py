from scheduler.config import *


class Allocation(object):
    def __init__(self):
        self.num_cores_left = max_cores_per_server
        self.ram_left = max_ram_per_server
