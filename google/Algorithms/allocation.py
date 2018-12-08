from scheduler.config_loader import *


class Allocation(object):
    def __init__(self,type):
        d = config["servers"]["types"][type]
        self.num_cores_left = d["max_cores_per_server"]
        self.ram_left = d["max_ram_per_server"]
        self.total_num_cores = d["max_cores_per_server"]
        self.total_ram = d["max_ram_per_server"]
        self.servers_list = []
        
    
class VM(object):
    def __init__(self,name,cores,ram):
        self.name = name
        self.cores = cores
        self.ram = ram
