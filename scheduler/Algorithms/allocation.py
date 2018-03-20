from scheduler.config import *


class Allocation(object):
    def __init__(self):
        self.num_cores_allocated = 0
        self.ram_allocated = 0
        self.total_cores = max_cores_per_server
        self.total_ram = max_ram_per_server
