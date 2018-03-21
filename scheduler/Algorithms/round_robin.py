from scheduler.config import *
from scheduler.Algorithms.allocation import Allocation
from scheduler.exception import OutOfResourceException


class Algorithm(object):
    def __init__(self):
        self.allocation_dict = {}
        self.server_num = 0
        self.num_servers = number_of_servers
        self.vm_server_mapper = {}
        self.num_cores_used = 0
        self.ram_used = 0
        self.number_servers_allocated = 0
        self.num_cores = []
        self.amount_ram = []
        self.servers_used = 0
        self.avg_ram_usage = 0
        self.avg_cpu_usage = 0
        self.avg_ram_usage_lst = []
        self.avg_cpu_usage_lst = []

    def execute(self, tup):
        vm_id, time_stamp, num_cores, ram_needed,c_d = tup
        if self.server_num not in self.allocation_dict:
            self.allocation_dict[self.server_num] = Allocation()
            self.servers_used += 1
        if c_d == "c":
            for _ in xrange(number_of_servers):
                if self.allocation_dict[self.server_num].num_cores_left > num_cores and \
                        self.allocation_dict[self.server_num].num_ram_left > ram_needed:
                    self.allocation_dict[self.server_num].num_cores_left -= num_cores
                    self.allocation_dict[self.server_num].ram_left -= ram_needed
                    self.num_cores_used += num_cores
                    self.num_cores.append(self.num_cores_used)
                    self.allocation_dict[self.server_num].ram_left -= ram_needed
                    self.ram_used += ram_needed
                    self.amount_ram.append(self.ram_used)
                    self.vm_server_mapper[vm_id] = self.server_num
                    self.server_num = (self.server_num + 1) % self.num_servers
                    self.avg_cpu_usage = self.num_cores_used/self.servers_used
                    self.avg_ram_usage = self.ram_used/self.servers_used
                    return
                else:
                    self.server_num = (self.server_num + 1) % self.num_servers
            else:
                raise OutOfResourceException()

        else:
            server_num = self.vm_server_mapper[vm_id]
            self.allocation_dict[server_num].num_cores_left += num_cores
            self.allocation_dict[server_num].ram_left += ram_needed
            self.num_cores_used -= num_cores
            self.num_cores.append(self.num_cores_used)
            self.ram_used -= ram_needed
            self.amount_ram.append(self.ram_used)
            if self.allocation_dict[server_num].num_cores_left == 0 and self.allocation_dict[server_num].ram_left == 0:
                del self.allocation_dict[server_num]
                self.servers_used -= 1
            self.avg_cpu_usage = self.num_cores_used / self.servers_used
            self.avg_ram_usage = self.ram_used / self.servers_used
