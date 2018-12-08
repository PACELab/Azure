from scheduler.Algorithms.allocation import Allocation
from scheduler.Algorithms.allocation import VM
from scheduler.exception import *
from scheduler.config_loader import *
from collections import defaultdict
from collections import OrderedDict
from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error
import dill
import os
import scipy.signal as signal
import numpy as np
import math
import traceback


class Algorithm(object):
    def __init__(self):
        self.allocation_dict = defaultdict(dict)
        self.vm_server_mapper = defaultdict(lambda: None)
        self.start_dict = {}
        self.num_cores_used = 0
        self.ram_used = 0
        self.life_time_list = []
        #self.effective_cores_used = 0  # This is obtained by multiplying no. of cores requested by a server and its avg cpu utilization
        self.servers_used = 0
        self.cores_ratio_sum = 0
        self.ram_ratio_sum = 0
        self.num_cores = []
        self.amount_ram = []
        self.effective_cores_lst = []
        self.avg_cpu_usage_lst = []
        self.avg_ram_usage_lst = []
        self.used_servers_lst = []
        self.stats_time = []

        self.avg_cores_hour = []
        self.p95_cores_hour = []
        self.max_cores_hour = []
        self.stats_hour_time = []
        self.hour_index = 0
        self.temp_lst = []

        self.cores_histogram = defaultdict(lambda: 0)
        self.ram_histogram = defaultdict(lambda: 0)

        self.max_cores = float("-inf")
        self.max_ram = float("-inf")

        self.ignore_vms_list = set()
        self.ignore_servers_list = set()

        self.prev_time_stamp_c = 0
        self.prev_c_cores = 0
        self.prev_c_ram = 0
        self.cores_creation_lst = []
        self.ram_creation_lst = []
        self.creation_stats_time = []

        self.prev_time_stamp_d = 0
        self.prev_d_cores = 0
        self.prev_d_ram = 0
        self.cores_deletion_lst = []
        self.ram_deletion_lst = []
        self.deletion_stats_time = []

        self.net_cores_dict = defaultdict(lambda: 0)
        self.net_ram_dict = defaultdict(lambda: 0)

        # self.prediction_max = None #Used for dynamic vm placement algo.
        path = os.path.join(get_parent_path(), config["actual_output_path"])
        if config["mode"] == "debug" and path:
            open(path, 'w').close()
        # self.B = None    #Used for dynamic vm placement algorithm.
        # self.A = None

        # Keeping this to be consistent for all algos.
        self.delayed_vm_count = None
        self.normal_vm_count = 0
        self.neglected_count = 0

    def execute(self, tup):
        time_stamp, num_cores, ram_needed, c_d = map(float,tup[2:])
        task_id, vm_id = tup[0], tup[1]
        vm_id = str(vm_id) +'_'+str(task_id)
        self.max_cores, self.max_ram = max(self.max_cores, num_cores), max(self.max_ram, ram_needed)  # not needed.

        if int(c_d) == 1:  # c inidcates create request
            # According to the feeder.csv built, delete and create requests would come at the same time and delete requests would fall before
            # create requests, therefore those create requests are to ignored.
            self.start_dict[vm_id] = time_stamp
            #if vm_id in self.ignore_vms_list:
            #    self.neglected_count+=1
            #    return

            for server_pool_type, value in config["servers"]["types"].iteritems():
                # print "s", "v", server_pool_type, value
                # print json.dumps(config,indent=4)
                number_of_servers = value["number"]  # number of servers present of this pool-type.
                # print "num sevrers",number_of_servers
                if ram_needed > value["max_ram_per_server"] or num_cores > value["max_cores_per_server"]:
                    continue
                p_f = 0  # this is just a print flag.
                # print "num servers", number_of_servers
                for _ in xrange(number_of_servers):
                    c_f = 0
                    # print "server number is ", self.server_num

                    server_tup = (server_pool_type, value["server_number"])
                    # When migrating, server will be added to ignore_servers_list to make sure it is not allocated on the same server
                    # Therefore, checking here if thats not the same server
                    if server_tup in self.ignore_servers_list:
                        # print "Server: "+tup+" ignored because of migration.."
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        continue

                    # Allocation dict contains the server object and when a server object does not exist, this indicates that an object is not created
                    # yet and it would be rejected.
                    if value["server_number"] not in self.allocation_dict[server_pool_type]:
                        # print "in allocation"
                        self.allocation_dict[server_pool_type][value["server_number"]] = Allocation(
                            type=server_pool_type)
                        c_f = 1  # this is just a flag to indicate if the server is just switched on to print debug statements.

                    # If server object exists, check if the current create request could be served on this server (ram and cores).
                    if self.allocation_dict[server_pool_type][
                        value["server_number"]].num_cores_left - num_cores >= 0 and \
                            self.allocation_dict[server_pool_type][value["server_number"]].ram_left - ram_needed >= 0:
                        # print "in allocation dict"
                        if c_f == 1:
                            # print "Switching On Server: ",server_tup
                            self.servers_used += 1
                            
                        self.normal_vm_count += 1

                        tmp_num_cores = self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left
                        tmp_ram = self.allocation_dict[server_pool_type][value["server_number"]].ram_left
                        self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left -= num_cores
                        self.allocation_dict[server_pool_type][value["server_number"]].ram_left -= ram_needed

                        # print "Creating VM: "+vm_id+" on Server: ",server_tup,"Available Resources on Server after creation is: ",self.allocation_dict[server_pool_type][value["server_number"]].ram_left," RAM ",self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left," Cores"

                        # To plot the graph of how the total number of cores and ram is being used of the datacenter varies over time.
                        self.num_cores_used += num_cores
                        self.ram_used += ram_needed

                        # This code is to handle if multiple requests come at the same, to make sure we are calculating correct stats
                        prev_timestamp = None
                        if self.stats_time:
                            prev_timestamp = self.stats_time[-1]

                        # Average CPU Usage, RAM Usage, Number of Servers switched on, Total number of cores and ram used in the data center,
                        if prev_timestamp != None and time_stamp == prev_timestamp:
                            self.num_cores[-1] = (self.num_cores_used)
                            self.amount_ram[-1] = (self.ram_used)
                        else:
                            self.num_cores.append(self.num_cores_used)
                            self.amount_ram.append(self.ram_used)
                            self.stats_time.append(time_stamp)

                        # To find the number of core creation requests arriave at each timestamp.
                        if self.prev_time_stamp_c == time_stamp:
                            self.prev_c_cores += num_cores
                            self.prev_c_ram += ram_needed
                        else:
                            self.cores_creation_lst.append(self.prev_c_cores)
                            self.ram_creation_lst.append(self.prev_c_ram)
                            self.creation_stats_time.append(self.prev_time_stamp_c)
                            self.prev_time_stamp_c = time_stamp
                            self.prev_c_cores = num_cores
                            self.prev_c_ram = ram_needed

                        period = 0  # period and period_timestamp are not needed, just passing some dummy values, but needed for dynamic placement algo.
                        period_timestamp = None
                        vm_object = VM(vm_id, num_cores, ram_needed)
                        #self.allocation_dict[server_pool_type][value["server_number"]].servers_list.append(vm_object)
                        self.vm_server_mapper[vm_id] = (server_pool_type, value["server_number"], vm_object)

                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        #self.write_to_file(pool_type=server_pool_type)
                        return
                    else:
                        if p_f == 0:
                            # print "Searching server for VM: "+vm_id+" from server number - ", server_tup
                            p_f = 1
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
            else:
                print "Could not find a server to place the VM"
                print "ram", ram_needed
                print "core", num_cores
                print "n_servers", self.servers_used
                raise OutOfResourceException()

        elif int(c_d) == 8:
            time_stamp, num_cores, ram_needed, c_d = map(float, tup[2:])
            task_id, vm_id = tup[0],tup[1]
            del_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed,  4)
            create_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed,1)
            #print "Starting Migration..."
            self.execute(del_tup)
            self.execute(create_tup)
            #print "Finished Migration..."

        else:
            if vm_id in self.start_dict:
                v = time_stamp - self.start_dict[vm_id]
                self.life_time_list.append(v/(1000*1000))
            data = self.vm_server_mapper[vm_id]
            del self.vm_server_mapper[vm_id]
            if not data:
                #self.ignore_vms_list.add(vm_id)
                #print "Missing Data Found..",vm_id
                return
            self.normal_vm_count -= 1
            pool_type, server_num = data[0], data[1]
            tmp_num_cores = self.allocation_dict[pool_type][server_num].num_cores_left
            tmp_ram = self.allocation_dict[pool_type][server_num].ram_left
  
            num_cores = data[2].cores
            ram_needed = data[2].ram
            self.allocation_dict[pool_type][server_num].num_cores_left += num_cores
            self.allocation_dict[pool_type][server_num].ram_left += ram_needed
                #print self.allocation_dict[pool_type]
            # print "Deleting VM: "+vm_id+" from Server: ",data,"Available Resources on Server after deletion is: ",self.allocation_dict[pool_type][server_num].ram_left," RAM ",self.allocation_dict[pool_type][server_num].num_cores_left," Cores ","Servers Used: ",self.servers_used

            # Updating all the stats like, total number of cores,ram used in the data center, core and ram utilization in the datacenter and the
            # number of servers switched on in the datacenter.
            self.num_cores_used -= num_cores
            self.ram_used -= ram_needed
            #self.effective_cores_used -= (num_cores * avg_cpu) / 100

            if self.allocation_dict[pool_type][server_num].num_cores_left == config["servers"]["types"][pool_type][
                "max_cores_per_server"] and self.allocation_dict[pool_type][
                server_num].ram_left == config["servers"]["types"][pool_type]["max_ram_per_server"]:
                del self.allocation_dict[pool_type][server_num]
                self.servers_used -= 1
                # print "Switching Off Server: ",data

            if self.servers_used == 0:
                return

            prev_timestamp = None
            if len(self.stats_time) > 0:
                prev_timestamp = self.stats_time[-1]

            if prev_timestamp != None and time_stamp == prev_timestamp:
                self.num_cores[-1] = (self.num_cores_used)
                self.amount_ram[-1] = (self.ram_used)
            else:
                self.num_cores.append(self.num_cores_used)
                self.amount_ram.append(self.ram_used)
                self.stats_time.append(time_stamp)

            if self.prev_time_stamp_d == time_stamp:
                self.prev_d_cores += num_cores
                self.prev_d_ram += ram_needed
            else:
                self.cores_deletion_lst.append(self.prev_d_cores)
                self.ram_deletion_lst.append(self.prev_d_ram)
                self.deletion_stats_time.append(self.prev_time_stamp_d)
                self.prev_time_stamp_d = time_stamp
                self.prev_d_cores = num_cores
                self.prev_d_ram = ram_needed
            self.write_to_file(pool_type=pool_type)


    def write_to_file(self, pool_type):
        if config["mode"] == "debug":
            output_file_path = config["actual_output_path"]
            output_file_path = os.path.join(get_parent_path(), output_file_path)
            with open(output_file_path, "a") as f:
                s = ""
                for server_pool_type, value in config["servers"]["types"].iteritems():
                    for num in xrange(value["number"]):
                        if num in self.allocation_dict[pool_type]:
                            c = self.allocation_dict[pool_type][num].num_cores_left
                        else:
                            c = float(config["servers"]["types"][pool_type]["max_cores_per_server"])

                        if num in self.allocation_dict[pool_type]:
                            r = self.allocation_dict[pool_type][num].ram_left
                        else:
                            r = float(config["servers"]["types"][pool_type]["max_ram_per_server"])
                        n_s = "{c}_{r}".format(c=c, r=r)
                        s = s + n_s + ","
                s = s + str(self.servers_used) + ","
                s = s + "{0:.4f}".format(round((self.cores_ratio_sum / self.servers_used), 4)) + ","
                s = s + "{0:.4f}".format(round((self.ram_ratio_sum / self.servers_used), 4)) + "\n"
                f.write(s)
        else:
            return

    def final(self):
        self.cores_creation_lst.append(self.prev_c_cores)
        self.ram_creation_lst.append(self.prev_c_ram)
        self.creation_stats_time.append(self.prev_time_stamp_c)

        self.cores_deletion_lst.append(self.prev_d_cores)
        self.ram_deletion_lst.append(self.prev_d_ram)
        self.deletion_stats_time.append(self.prev_time_stamp_d)
        
        print "Normal VM Count: ", self.normal_vm_count, "Neglected VM Count: ", self.neglected_count









