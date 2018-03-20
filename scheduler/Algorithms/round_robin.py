from scheduler.config import *
from scheduler.Algorithms.allocation import Allocation


class Algorithm(object):
    def __init__(self):
        self.allocation_dict = {}
        self.server_num = 0
        self.num_servers = number_of_servers
        self.vm_server_mapper = {}

    def execute(self, tup):
        c_d, vm_id, time_stamp, num_cores, ram_needed = tup
        if self.server_num not in self.allocation_dict:
            self.allocation_dict[self.server_num] = Allocation()
        if c_d == "c":
            self.allocation_dict[self.server_num].num_cores_allocated += num_cores
            self.allocation_dict[self.server_num].ram_allocated += ram_needed
            self.vm_server_mapper[vm_id] = self.server_num
            self.server_num = (self.server_num + 1) % self.num_servers
        else:
            server_num = self.vm_server_mapper[vm_id]
            self.allocation_dict[server_num].num_cores_allocated -= num_cores
            self.allocation_dict[server_num].ram_allocated -= ram_needed
