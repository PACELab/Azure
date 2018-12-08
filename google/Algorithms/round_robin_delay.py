from scheduler.Algorithms.allocation import Allocation
from scheduler.Algorithms.allocation import VM
from scheduler.exception import *
from scheduler.config_loader import *
from collections import defaultdict
from collections import OrderedDict
from collections import deque
from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error
import dill
import os
import sys
import scipy.signal as signal
import numpy as np
import math

#This code needs to be fixed using rr_delay_v2 which is almost similar expect that vm is delayed based on hard limit here instead of avg+stddev

"""

"delay_time" needs to be set, which would indicate the time by which the vm is to be delayed if it has not hit the hard limit of max_delay_time.
"max_delay_time" needs to be set, which would indicate the max hard limit by which VM can be delayed.
"cores_hard_limit" needs to be set, which would indicate that VM should be delayed if it exceeds "cores_hard_limit"

In this algorithm, vm would be delayed only by "delay_time" and when the time expires it would check if VM can deployed, if yes it would be deployed and
if it cannot be deployed it would further be delayed by "delay_time" until it hits "max_delay_time"

"""

class Algorithm(object):
    def __init__(self):
        self.allocation_dict = defaultdict(dict)
        self.vm_server_mapper = defaultdict(lambda: None)

        self.num_cores_used = 0
        self.ram_used = 0
        self.effective_cores_used = 0  #This is obtained by multiplying no. of cores requested by a server and its avg cpu utilization
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
        
        self.net_cores_dict = defaultdict(lambda : 0)
        self.net_ram_dict = defaultdict(lambda : 0)
        
        self.stdmultiplier = float(config["stdMultiplier"])
        self.window_size = float(config["window_size"])
        self.delay_time = float(config["delay_time"])
        self.max_delay_time = float(config["max_delay_time"])
        self.delay_time_stamp = None
        self.delayed_vm_reqs = deque()
        self.delayed_vms = set()
        self.duplicate_vms = defaultdict(lambda : 0)
        self.delayed_vm_count = 0
        self.core_hard_limit = config["cores_hard_limit"]
        
        self.normal_vm_count = 0
        self.vm_delay_histogram = defaultdict(lambda : 0)
        self.vm_delay_time = defaultdict(lambda : 0)
        
        #self.prediction_max = None #Used for dynamic vm placement algo.
        path = os.path.join(get_parent_path(),config["actual_output_path"])
        if config["mode"] == "debug" and path:
            open(path, 'w').close()
        #self.B = None    #Used for dynamic vm placement algorithm.
        #self.A = None


    def execute(self, tup,run_delayed_tasks=True,can_delay=True,original_time_stamp=None):
        time_stamp, num_cores, ram_needed, c_d = map(float,tup[2:])
        task_id, vm_id = tup[0], tup[1]
        orig_vm_id = vm_id
        vm_id = str(vm_id) +'_'+str(task_id)
        if original_time_stamp==None and int(c_d)==1:
            task_id = str(task_id)+'_'+str(self.duplicate_vms[vm_id])
            self.duplicate_vms[vm_id]+=1
            vm_id = str(orig_vm_id)+'_'+str(task_id)
        self.max_cores, self.max_ram = max(self.max_cores, num_cores), max(self.max_ram, ram_needed) #not needed.

        if self.delay_time_stamp <= time_stamp and run_delayed_tasks==True and self.delay_time_stamp!=None:
            x = self.delay_time_stamp
            while x<=time_stamp and x!=None:
                waiting_tup = self.delayed_vm_reqs.popleft()
                temp_tup = (waiting_tup[0],waiting_tup[1],waiting_tup[2],waiting_tup[3],waiting_tup[4],waiting_tup[5])
                orig_time_stamp = float(waiting_tup[6])
                self.execute(temp_tup,run_delayed_tasks=False,can_delay=True,original_time_stamp=orig_time_stamp)
                if self.delayed_vm_reqs:
                    x = self.delayed_vm_reqs[0][2]
                else:    
                    x = None
                    break
            self.delay_time_stamp = x

        if int(c_d) == 1:  #c inidcates create request
            
            
            #According to the feeder.csv built, delete and create requests would come at the same time and delete requests would fall before
            #create requests, therefore those create requests are to ignored.
            if vm_id in self.ignore_vms_list:
                return
                
            if (self.num_cores_used+num_cores>self.core_hard_limit and can_delay==True):
                if original_time_stamp==None:
                    delayed_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                    self.delayed_vm_count+=1
                    self.delayed_vms.add(vm_id)
                else:
                    delayed_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, original_time_stamp)
                if not self.delayed_vm_reqs:
                    self.delay_time_stamp = time_stamp+self.delay_time
                self.delayed_vm_reqs.append(delayed_tup)
                return
            """elif self.delayed_vms[vm_id]>0 and can_delay==True:
                if self.vm_delay_time[vm_id]==0:
                    if original_time_stamp==None:
                        delayed_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                        if not self.delayed_vm_reqs:
                            self.delay_time_stamp = time_stamp+self.delay_time
                        self.delayed_vm_reqs.append(delayed_tup)
                        self.delayed_vms[vm_id]+=1
                        self.delayed_vm_count+=1
                        return
                else:
                    if original_time_stamp==None:
                        delayed_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                        self.delayed_vms[vm_id]+=1
                        self.delayed_vm_count+=1
                    else:
                        delayed_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, original_time_stamp)
                    if not self.delayed_vm_reqs:
                        self.delay_time_stamp = time_stamp+self.delay_time
                    self.delayed_vm_reqs.append(delayed_tup)
                    return"""
                
            if original_time_stamp != None:    
                self.vm_delay_time[vm_id] = time_stamp-original_time_stamp
            else:
                self.vm_delay_time[vm_id] = 0
                original_time_stamp = time_stamp
                
            for server_pool_type, value in config["servers"]["types"].iteritems():
                # print "s", "v", server_pool_type, value
                # print json.dumps(config,indent=4)
                number_of_servers = value["number"]  #number of servers present of this pool-type.
                # print "num sevrers",number_of_servers
                if ram_needed > value["max_ram_per_server"] or num_cores > value["max_cores_per_server"]:
                    continue
                p_f=0 #this is just a print flag.
                # print "num servers", number_of_servers
                for _ in xrange(number_of_servers):
                    c_f = 0
                    # print "server number is ", self.server_num
                    
                    
                    server_tup = (server_pool_type,value["server_number"])
                    #When migrating, server will be added to ignore_servers_list to make sure it is not allocated on the same server
                    #Therefore, checking here if thats not the same server
                    if server_tup in self.ignore_servers_list:
                        #print "Server: "+tup+" ignored because of migration.."
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        continue
                        

                    #Allocation dict contains the server object and when a server object does not exist, this indicates that an object is not created
                    #yet and it would be rejected.
                    if value["server_number"] not in self.allocation_dict[server_pool_type]:
                        # print "in allocation"
                        self.allocation_dict[server_pool_type][value["server_number"]] = Allocation(
                            type=server_pool_type)
                        c_f = 1 #this is just a flag to indicate if the server is just switched on to print debug statements.


                    #If server object exists, check if the current create request could be served on this server (ram and cores).
                    if self.allocation_dict[server_pool_type][
                        value["server_number"]].num_cores_left - num_cores >= 0 and \
                            self.allocation_dict[server_pool_type][value["server_number"]].ram_left - ram_needed >= 0:
                        # print "in allocation dict"
                        if c_f == 1:
                            #print "Switching On Server: ",server_tup
                            self.servers_used += 1
                            
                        self.normal_vm_count+=1
                        self.vm_delay_histogram[time_stamp-original_time_stamp]+=1
                            
                        tmp_num_cores = self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left
                        tmp_ram = self.allocation_dict[server_pool_type][value["server_number"]].ram_left
                        self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left -= num_cores
                        self.allocation_dict[server_pool_type][value["server_number"]].ram_left -= ram_needed
                        
                        
                        #print "Creating VM: "+vm_id+" on Server: ",server_tup,"Available Resources on Server after creation is: ",self.allocation_dict[server_pool_type][value["server_number"]].ram_left," RAM ",self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left," Cores"
                        
                        
                        #To plot the graph of how the total number of cores and ram is being used of the datacenter varies over time.
                        self.num_cores_used += num_cores
                        self.ram_used += ram_needed
                        
                        #This code is to handle if multiple requests come at the same, to make sure we are calculating correct stats
                        prev_timestamp = None
                        if self.stats_time:
                            prev_timestamp = self.stats_time[-1]
                        
                        #Average CPU Usage, RAM Usage, Number of Servers switched on, Total number of cores and ram used in the data center,
                        if prev_timestamp != None and time_stamp == prev_timestamp:
                            self.num_cores[-1]=(self.num_cores_used)
                            self.amount_ram[-1]=(self.ram_used)
                        else:
                            self.num_cores.append(self.num_cores_used)
                            self.amount_ram.append(self.ram_used)
                            self.stats_time.append(time_stamp)
                        
                        #To find the number of core creation requests arriave at each timestamp.
                        if self.prev_time_stamp_c == time_stamp:
                            self.prev_c_cores += num_cores
                            self.prev_c_ram += ram_needed
                        else:
                            self.cores_creation_lst.append(self.prev_c_cores)
                            self.ram_creation_lst.append(self.prev_c_ram)
                            self.creation_stats_time.append(self.prev_time_stamp_c)
                            if self.net_cores_dict[self.prev_time_stamp_c] == None:
                                self.net_cores_dict[self.prev_time_stamp_c] = self.prev_c_cores
                                self.net_ram_dict[self.prev_time_stamp_c] = self.prev_c_ram
                            else:
                                self.net_cores_dict[self.prev_time_stamp_c] += self.prev_c_cores
                                self.net_ram_dict[self.prev_time_stamp_c] += self.prev_c_ram
                            ##
                            self.prev_time_stamp_c = time_stamp
                            self.prev_c_cores = num_cores
                            self.prev_c_ram = ram_needed

                        #period = 0 #period and period_timestamp are not needed, just passing some dummy values, but needed for dynamic placement algo.
                        #period_timestamp = None
                        #period,period_timestamp = self.periodicity(vm_id) #needed for dynamic VM placement alogorithm
                        vm_object = VM(vm_id,num_cores,ram_needed)
                        #self.allocation_dict[server_pool_type][value["server_number"]].servers_list.append(vm_object)
                        self.vm_server_mapper[vm_id] = (server_pool_type, value["server_number"], vm_object)
                        
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        return
                    else:
                        if p_f==0:
                          #print "Searching server for VM: "+vm_id+" from server number - ", server_tup
                          p_f=1
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
            else:
                print "Could not find a server to place the VM"
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
            if can_delay==True:
                if original_time_stamp == None and self.duplicate_vms[vm_id]>0:
                    vm_id = vm_id +'_'+str(self.duplicate_vms[vm_id]-1)
                    task_id = task_id+'_'+str(self.duplicate_vms[vm_id]-1)
                if vm_id in self.delayed_vms:
                    if self.vm_delay_time[vm_id]==0:
                        if original_time_stamp==None:
                            delayed_del_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                        else:
                            delayed_del_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, original_time_stamp)
                        if not self.delayed_vm_reqs:
                            self.delay_time_stamp = time_stamp+self.delay_time
                        self.delayed_vm_reqs.append(delayed_del_tup)
                        return
                    else:
                        if original_time_stamp == None:
                            original_time_stamp = time_stamp
                        temp_delay_time = original_time_stamp+self.vm_delay_time[vm_id]
                        if time_stamp < temp_delay_time:
                            delayed_del_tup = (task_id, orig_vm_id, time_stamp+self.delay_time, num_cores, ram_needed, c_d, original_time_stamp)
                            if not self.delayed_vm_reqs:
                                self.delay_time_stamp = time_stamp+self.delay_time
                            self.delayed_vm_reqs.append(delayed_del_tup)
                            return
                        else:
                            can_delay=False
                    
            if can_delay==False:
                self.delayed_vms.remove(vm_id) 
            
            data = self.vm_server_mapper[vm_id]
            self.vm_server_mapper.pop(vm_id)
            if not data:
                self.ignore_vms_list.add(vm_id)
                return
            self.vm_delay_time.pop(vm_id)
            self.normal_vm_count -= 1
            pool_type, server_num = data[0], data[1]
            num_cores = data[2].cores
            ram_needed = data[2].ram
            tmp_num_cores = self.allocation_dict[pool_type][server_num].num_cores_left
            tmp_ram = self.allocation_dict[pool_type][server_num].ram_left
            
            self.allocation_dict[pool_type][server_num].num_cores_left += num_cores
            self.allocation_dict[pool_type][server_num].ram_left += ram_needed
            #print "Deleting VM: "+vm_id+" from Server: ",data,"Available Resources on Server after deletion is: ",self.allocation_dict[pool_type][server_num].ram_left," RAM ",self.allocation_dict[pool_type][server_num].num_cores_left," Cores ","Servers Used: ",self.servers_used
            
            
            #Updating all the stats like, total number of cores,ram used in the data center, core and ram utilization in the datacenter and the
            #number of servers switched on in the datacenter.
            self.num_cores_used -= num_cores
            self.ram_used -= ram_needed

            if self.allocation_dict[pool_type][server_num].num_cores_left == config["servers"]["types"][pool_type]["max_cores_per_server"] and self.allocation_dict[pool_type][
                server_num].ram_left == config["servers"]["types"][pool_type]["max_ram_per_server"]:
                del self.allocation_dict[pool_type][server_num]
                self.servers_used -= 1
                #print "Switching Off Server: ",data
                
            if self.servers_used==0:
                return
                
            prev_timestamp = None
            if self.stats_time:
                prev_timestamp = self.stats_time[-1]
                
            if prev_timestamp != None and time_stamp == prev_timestamp:
                self.num_cores[-1]=(self.num_cores_used)
                self.amount_ram[-1]=(self.ram_used)
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
                if self.net_cores_dict[self.prev_time_stamp_d] == None:
                    self.net_cores_dict[self.prev_time_stamp_d] = -(self.prev_d_cores)
                    self.net_ram_dict[self.prev_time_stamp_d] = -(self.prev_d_ram)
                else:
                    self.net_cores_dict[self.prev_time_stamp_d] -= self.prev_d_cores
                    self.net_ram_dict[self.prev_time_stamp_d] -= self.prev_d_ram
                ##
                self.prev_time_stamp_d = time_stamp
                self.prev_d_cores = num_cores
                self.prev_d_ram = ram_needed
            self.write_to_file(pool_type=pool_type)
            

    def write_to_file(self,pool_type):
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
                s = s+str(self.servers_used)+","
                s = s + "{0:.4f}".format(round((self.cores_ratio_sum / self.servers_used),4))+","
                s = s + "{0:.4f}".format(round((self.ram_ratio_sum / self.servers_used), 4))+"\n"
                f.write(s)
        else:
            return

    def final(self):
        x = self.delay_time_stamp
        orig_final_time = self.stats_time[-1]
        max_events_delay = 0
        print "Reached Final..","Cores Used is: ", self.num_cores_used
        sys.stdout.flush()
        
        while x!=None:
            max_events_delay = max(max_events_delay,x-orig_final_time)
            waiting_tup = self.delayed_vm_reqs.popleft()
            vm_id = waiting_tup[1]+"_"+waiting_tup[0]
            print waiting_tup
            print "Cores: ", self.num_cores_used
            temp_tup = (waiting_tup[0],waiting_tup[1],waiting_tup[2],waiting_tup[3],waiting_tup[4],waiting_tup[5])
            orig_time_stamp = float(waiting_tup[6])
            self.execute(temp_tup,run_delayed_tasks=False,can_delay=True,original_time_stamp=orig_time_stamp)
            if self.delayed_vm_reqs:
                x = self.delayed_vm_reqs[0][2]
            else:    
                x = None
                break
        self.delay_time_stamp = x
                
        self.cores_creation_lst.append(self.prev_c_cores)
        self.ram_creation_lst.append(self.prev_c_ram)
        self.creation_stats_time.append(self.prev_time_stamp_c)

        self.cores_deletion_lst.append(self.prev_d_cores)
        self.ram_deletion_lst.append(self.prev_d_ram)
        self.deletion_stats_time.append(self.prev_time_stamp_d)
        
        print "VM's not deleted: ", self.normal_vm_count
        print "Simulation time Delayed by: ", max_events_delay
        sys.stdout.flush()
        
        self.vm_delay_histogram = OrderedDict(sorted(self.vm_delay_histogram.iteritems(),key = lambda t:t[0]))
        
        