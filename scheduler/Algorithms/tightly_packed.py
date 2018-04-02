import dill
from scheduler.Algorithms.allocation import Allocation
from scheduler.exception import *
from scheduler.config_loader import *
from collections import defaultdict


class Algorithm(object):
    def __init__(self):
        self.allocation_dict = defaultdict(dict)
        self.server_num = 0
        self.num_servers = config["servers"]["number_of_servers"]
        self.vm_server_mapper = defaultdict(lambda: None)
        self.cores_histogram = defaultdict(lambda: 0)
        self.ram_histogram = defaultdict(lambda: 0)
        self.num_cores_used = 0
        self.ram_used = 0
        self.number_servers_allocated = 1  # to do : make 0
        self.num_cores = []
        self.num_cores_time = []
        self.amount_ram = []
        self.amount_ram_time = []
        self.servers_used = 0
        self.avg_ram_usage = 0
        self.avg_cpu_usage = 0
        self.avg_ram_usage_lst = []
        self.avg_ram_usage_time = []
        self.used_servers_lst = []
        self.used_servers_time = []
        self.avg_cpu_usage_lst = []
        self.avg_cpu_usage_time = []
        self.max_cores = float("-inf")
        self.max_ram = float("-inf")
        self.c = 0
        self.cores_ratio_sum = 0
        self.ram_ratio_sum = 0
        self.ignore_vms_list = set()
        
        self.prev_time_stamp_c = 0
        self.prev_c_cores = 0
        self.prev_c_ram = 0
        self.cores_creation_lst = []
        self.cores_creation_time = []
        self.ram_creation_lst = []
        self.ram_creation_time = []
        
        self.prev_time_stamp_d = 0
        self.prev_d_cores = 0
        self.prev_d_ram = 0
        self.cores_deletion_lst = []
        self.cores_deletion_time = []
        self.ram_deletion_lst = []
        self.ram_deletion_time = []
        if config["mode"] == "debug" and os.path.exists(config["actual_output_path"]):
            os.remove(config["actual_output_path"])
        

    def execute(self, tup):
        vm_id, time_stamp, num_cores, ram_needed, c_d = tup
        num_cores, ram_needed, time_stamp = float(num_cores), float(ram_needed), float(time_stamp)
        self.max_cores, self.max_ram = max(self.max_cores, num_cores), max(self.max_ram, ram_needed)
        if c_d.strip() == "c":
            if vm_id in self.ignore_vms_list:
                return
            for server_pool_type, value in config["servers"]["types"].iteritems():
                # print "s", "v", server_pool_type, value
                number_of_servers = value["number"]
                if ram_needed > value["max_ram_per_server"] or num_cores > value["max_cores_per_server"]:
                    continue
                # print "num servers", number_of_servers
                for _ in xrange(number_of_servers):
                    c_f = 0
                    # print "server number is ", self.server_num
                    if value["server_number"] not in self.allocation_dict[server_pool_type]:
                        # print "in allocation"
                        self.allocation_dict[server_pool_type][value["server_number"]] = Allocation(
                            type=server_pool_type)
                        c_f = 1
                        # print "num severs used", self.servers_used

                    if self.allocation_dict[server_pool_type][
                        value["server_number"]].num_cores_left - num_cores >= 0 and \
                            self.allocation_dict[server_pool_type][value["server_number"]].ram_left - ram_needed >= 0:
                        # print "in allocation dict"
                        if c_f == 1:
                            self.servers_used += 1
                        tmp_num_cores = self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left
                        tmp_ram = self.allocation_dict[server_pool_type][value["server_number"]].ram_left
                        self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left -= num_cores
                        self.allocation_dict[server_pool_type][value["server_number"]].ram_left -= ram_needed
                        self.num_cores_used += num_cores
                        self.num_cores.append(self.num_cores_used)
                        self.num_cores_time.append(time_stamp)
                        self.ram_used += ram_needed
                        self.amount_ram.append(self.ram_used)
                        self.amount_ram_time.append(time_stamp)
                        self.vm_server_mapper[vm_id] = (server_pool_type, value["server_number"])

                        # compute average
                        v = self.allocation_dict[server_pool_type][value["server_number"]]
                        self.cores_ratio_sum = self.cores_ratio_sum + (
                                (v.total_num_cores - v.num_cores_left) / float(v.total_num_cores)) - (
                                                     (v.total_num_cores - tmp_num_cores) / float(v.total_num_cores))
                        self.ram_ratio_sum = self.ram_ratio_sum + (
                                    (v.total_ram - v.ram_left) / float(v.total_ram)) - (
                                                       (v.total_ram - tmp_ram) / float(v.total_ram))
                        prev_timestamp = None
                        if len(self.avg_cpu_usage_time)>0:
                          prev_timestamp = self.avg_cpu_usage_time[-1]
                        if prev_timestamp != None and time_stamp == prev_timestamp:
                          del self.avg_cpu_usage_lst[-1]
                          del self.avg_cpu_usage_time[-1]
                          del self.avg_ram_usage_lst[-1]
                          del self.avg_ram_usage_time[-1]
                          del self.used_servers_lst[-1]
                          del self.used_servers_time[-1]
                          
                        self.avg_cpu_usage_lst.append(100 * (self.cores_ratio_sum / self.servers_used))
                        self.avg_cpu_usage_time.append(time_stamp)
                        self.avg_ram_usage_lst.append(100 * (self.ram_ratio_sum / self.servers_used))
                        self.avg_ram_usage_time.append(time_stamp)
                        self.used_servers_lst.append(self.servers_used)
                        self.used_servers_time.append(time_stamp)
                        self.cores_histogram[num_cores]+=1
                        self.ram_histogram[ram_needed]+=1
                        
                        if self.prev_time_stamp_c==time_stamp:
                          self.prev_c_cores+=num_cores
                          self.prev_c_ram+=ram_needed
                        else:
                          self.cores_creation_lst.append(self.prev_c_cores)
                          self.cores_creation_time.append(self.prev_time_stamp_c)
                          self.ram_creation_lst.append(self.prev_c_ram)
                          self.ram_creation_time.append(self.prev_time_stamp_c)
                          ##
                          self.prev_time_stamp_c = time_stamp
                          self.prev_c_cores = num_cores
                          self.prev_c_ram = ram_needed
                          
                        #value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        return
                    else:
                        # print "num severs used else", self.servers_used
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
            else:
                self.c += 1
                print "ram", ram_needed
                print "core", num_cores
                print "n_servers", self.servers_used
                # # with open("debug.txt", "w") as o_f:
                # #     s = ""
                # #     for k, v in self.allocation_dict.iteritems():
                # #         s += "server number : {num}; ram left :: {ram}; cores left: {cores}\n".format(num=k,
                # #                                                                                       cores=v.num_cores_left,
                # #                                                                                       ram=v.ram_left)
                # #         o_f.write(s)
                # print "cores", self.allocation_dict[self.server_num].num_cores_left - num_cores
                # print "ram", self.allocation_dict[self.server_num].ram_left - ram_needed
                raise OutOfResourceException()

        else:
            data = self.vm_server_mapper[vm_id]
            if not data:
              self.ignore_vms_list.add(vm_id)
              return
            pool_type,server_num = data[0],data[1]
            tmp_num_cores = self.allocation_dict[pool_type][server_num].num_cores_left
            tmp_ram = self.allocation_dict[pool_type][server_num].ram_left
            self.allocation_dict[pool_type][server_num].num_cores_left += num_cores
            self.allocation_dict[pool_type][server_num].ram_left += ram_needed
            self.num_cores_used -= num_cores
            self.num_cores.append(self.num_cores_used)
            self.num_cores_time.append(time_stamp)
            self.ram_used -= ram_needed
            self.amount_ram.append(self.ram_used)
            self.amount_ram_time.append(time_stamp)
            if self.allocation_dict[pool_type][server_num].num_cores_left == 0 and self.allocation_dict[pool_type][
                server_num].ram_left == 0:
                del self.allocation_dict[pool_type][server_num]
                self.servers_used -= 1
            # compute average
            v = self.allocation_dict[pool_type][server_num]
            self.cores_ratio_sum = self.cores_ratio_sum + (
                        (v.total_num_cores - v.num_cores_left) / float(v.total_num_cores)) - (
                                             (v.total_num_cores - tmp_num_cores) / float(v.total_num_cores))
            self.ram_ratio_sum = self.ram_ratio_sum + (
            (v.total_ram - v.ram_left) / float(v.total_ram)) - (
                        (v.total_ram - tmp_ram) / float(v.total_ram))
            
            prev_timestamp = self.avg_cpu_usage_time[-1]
            if time_stamp == prev_timestamp:
              del self.avg_cpu_usage_lst[-1]
              del self.avg_cpu_usage_time[-1]
              del self.avg_ram_usage_lst[-1]
              del self.avg_ram_usage_time[-1]
              del self.used_servers_lst[-1]
              del self.used_servers_time[-1]
              
            self.avg_cpu_usage_lst.append(100 * (self.cores_ratio_sum / self.servers_used))
            self.avg_cpu_usage_time.append(time_stamp)
            self.avg_ram_usage_lst.append(100 * (self.ram_ratio_sum / self.servers_used))
            self.avg_ram_usage_time.append(time_stamp)
            self.used_servers_lst.append(self.servers_used)
            self.used_servers_time.append(time_stamp)
            
            if self.prev_time_stamp_d==time_stamp:
              self.prev_d_cores+=num_cores
              self.prev_d_ram+=ram_needed
            else:
              self.cores_deletion_lst.append(self.prev_d_cores)
              self.cores_deletion_time.append(self.prev_time_stamp_d)
              self.ram_deletion_lst.append(self.prev_d_ram)
              self.ram_deletion_time.append(self.prev_time_stamp_d)
              ##
              self.prev_time_stamp_d = time_stamp
              self.prev_d_cores = num_cores
              self.prev_d_ram = ram_needed
        if config["mode"] == "debug":
            with open(output_file_path, "a") as f:
                s = ""
                for server_pool_type, value in config["servers"]["types"].iteritems():
                    for num in xrange(value["server_number"]):
                        c = self.allocation_dict[pool_type][num].num_cores_left
                        r = self.allocation_dict[pool_type][num].ram_left
                        n_s = "{c}_{r}".format(c=c, r=r)
                        s = s + n_s + ","
                s = s+str(self.servers_used)+","
                s = s + str(self.avg_ram_usage) + ","
                s = s + str(self.avg_cpu_usage)
                f.write(s)


    def final(self):
        self.cores_creation_lst.append(self.prev_c_cores)
        self.cores_creation_time.append(self.prev_time_stamp_c)
        self.ram_creation_lst.append(self.prev_c_ram)
        self.ram_creation_time.append(self.prev_time_stamp_c)
        
        self.cores_deletion_lst.append(self.prev_d_cores)
        self.cores_deletion_time.append(self.prev_time_stamp_d)
        self.ram_deletion_lst.append(self.prev_d_ram)
        self.ram_deletion_time.append(self.prev_time_stamp_d)
