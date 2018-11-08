from scheduler.Algorithms.allocation import Allocation
from scheduler.Algorithms.allocation import VM
from scheduler.exception import *
from scheduler.config_loader import *
from collections import *
import dill
import os
import sys
from math import *
from heapq import *


class Algorithm(object):
    def __init__(self):
        self.allocation_dict = defaultdict(dict)
        self.vm_server_mapper = defaultdict(lambda: None)
        self.num_cores_used = 0
        self.ram_used = 0
        self.servers_used = 0
        self.ram_ratio_sum = 0
        self.num_cores = []
        self.stats_time = []
        self.ignore_vms_list = set()
        self.delay_time = float(config["delay_time"])
        self.delay_time_stamp = None
        self.delayed_vm_reqs = deque()
        self.delayed_vms = set()
        self.delayed_delete_map = {}
        self.delayed_heap = []
        self.duplicate_vms = defaultdict(lambda: 0)
        self.delayed_vm_count = 0
        self.core_hard_limit = config["cores_hard_limit"]
        self.vm_delay_time = defaultdict(lambda: 0)
        self.vm_delay_histogram = defaultdict(lambda: 0)
        self.max_cores_used = 0
        self.vm = 0
        self.mig = 0
        self.total_time = 0
        self.total_w_cores = 0
        self.start_t = 0
        self.actual_t = 0
        self.flag = False
        self.min_delay = float(config["delay_time"])
        # self.prediction_max = None #Used for dynamic vm placement algo.
        path = os.path.join(get_parent_path(), config["actual_output_path"])
        if config["mode"] == "debug" and path:
            open(path, 'w').close()

    def execute(self, tup, run_delayed_tasks=True, can_delay=True, original_time_stamp=None, migrate=False):
        time_stamp, num_cores, ram_needed, c_d = map(float, tup[2:])
        if not self.flag:
            self.start_t = time_stamp
            self.flag = True
        task_id, vm_id = tup[0], tup[1]
        orig_vm_id = vm_id
        vm_id = str(vm_id) + '_' + str(task_id)
        if original_time_stamp == None and int(c_d) == 1:
            task_id = str(task_id) + '_' + str(self.duplicate_vms[vm_id])
            self.duplicate_vms[vm_id] += 1
            vm_id = str(orig_vm_id) + '_' + str(task_id)
            # print vm_id

        # check for 1st elment in queue and serve it.
        if run_delayed_tasks == True:
            y = None
            if self.delayed_heap:
                y = self.delayed_heap[0][0]
            while (y != None and y <= time_stamp):
                del_req = heappop(
                    self.delayed_heap)  # (time_stamp,task_id, vm_id, num_cores, ram_needed, c_d,orig_time_stamp)
                del_tup = (del_req[1], del_req[2], del_req[0], del_req[3], del_req[4], del_req[5])
                self.execute(del_tup, run_delayed_tasks=False, can_delay=False, original_time_stamp=del_req[6])
                if self.delayed_heap:
                    y = self.delayed_heap[0][0]
                else:
                    y = None

            if self.delayed_vm_reqs:
                flag = True
                if self.num_cores_used > 0.5 * self.core_hard_limit:
                    if time_stamp - self.delayed_vm_reqs[0][-1]> self.min_delay:
                        flag = True
                    else:
                        flag = False

                while flag and (self.num_cores_used + float(self.delayed_vm_reqs[0][3]) <= self.core_hard_limit):
                    waiting_tup = self.delayed_vm_reqs.popleft()
                    temp_tup = (
                    waiting_tup[0], waiting_tup[1], time_stamp, waiting_tup[3], waiting_tup[4], waiting_tup[5])
                    orig_time_stamp = float(waiting_tup[6])
                    self.execute(temp_tup, run_delayed_tasks=False, can_delay=False,
                                 original_time_stamp=orig_time_stamp)
                    if not self.delayed_vm_reqs:
                        break
                    else:
                        if self.num_cores_used > 0.5 * self.core_hard_limit:
                            if time_stamp - float(self.delayed_vm_reqs[0][-1]) > self.min_delay:
                                flag = True
                            else:
                                flag = False

        if int(c_d) == 1:  # c inidcates create request
            # According to the feeder.csv built, delete and create requests would come at the same time and delete requests would fall before
            # create requests, therefore those create requests are to ignored.
            # if vm_id in self.ignore_vms_list:
            #     return

            if (can_delay == True) and ((self.num_cores_used + num_cores > self.core_hard_limit) or (
                    self.num_cores_used > 0.5 * self.core_hard_limit)):
                if original_time_stamp == None:
                    delayed_tup = (
                        task_id, orig_vm_id, time_stamp + self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                    self.delayed_vm_count += 1
                    self.delayed_vms.add(vm_id)
                else:
                    delayed_tup = (
                        task_id, orig_vm_id, time_stamp + self.delay_time, num_cores, ram_needed, c_d,
                        original_time_stamp)
                if not self.delayed_vm_reqs:
                    self.delay_time_stamp = time_stamp + self.delay_time
                self.delayed_vm_reqs.append(delayed_tup)
                return

            if original_time_stamp != None:
                self.vm_delay_time[vm_id] = time_stamp - original_time_stamp
            else:
                self.vm_delay_time[vm_id] = 0
                original_time_stamp = time_stamp

            # check hash map and put in heapq
            if vm_id in self.delayed_delete_map:
                del_tup = self.delayed_delete_map[vm_id]
                del_time = float(del_tup[0]) + self.vm_delay_time[vm_id]
                new_del_tup = (del_time, del_tup[1], del_tup[2], del_tup[3], del_tup[4], del_tup[5], del_tup[6])
                heappush(self.delayed_heap, new_del_tup)
                self.delayed_delete_map.pop(vm_id)

            # deployment
            if migrate == False:
                self.vm += 1
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

                        t = ceil((time_stamp - original_time_stamp) / 1000000)
                        t = int(t)
                        self.vm_delay_histogram[t] += 1
                        self.total_time += t
                        self.total_w_cores += t * num_cores
                        tmp_num_cores = self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left
                        tmp_ram = self.allocation_dict[server_pool_type][value["server_number"]].ram_left
                        self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left -= num_cores
                        self.allocation_dict[server_pool_type][value["server_number"]].ram_left -= ram_needed

                        # print "Creating VM: "+vm_id+" on Server: ",server_tup,"Available Resources on Server after creation is: ",self.allocation_dict[server_pool_type][value["server_number"]].ram_left," RAM ",self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left," Cores"

                        # To plot the graph of how the total number of cores and ram is being used of the datacenter varies over time.
                        self.num_cores_used += num_cores

                        # This code is to handle if multiple requests come at the same, to make sure we are calculating correct stats
                        prev_timestamp = None
                        if self.stats_time:
                            prev_timestamp = self.stats_time[-1]

                        # Average CPU Usage, RAM Usage, Number of Servers switched on, Total number of cores and ram used in the data center,
                        if prev_timestamp != None and time_stamp == prev_timestamp:
                            self.num_cores[-1] = (self.num_cores_used)

                        else:
                            self.num_cores.append(self.num_cores_used)

                            self.stats_time.append(time_stamp)

                        self.max_cores_used = max(self.max_cores_used, self.num_cores_used)
                        # period = 0 #period and period_timestamp are not needed, just passing some dummy values, but needed for dynamic placement algo.
                        # period_timestamp = None
                        # period,period_timestamp = self.periodicity(vm_id) #needed for dynamic VM placement alogorithm
                        vm_object = VM(vm_id, num_cores, ram_needed)
                        # self.allocation_dict[server_pool_type][value["server_number"]].servers_list.append(vm_object)
                        self.vm_server_mapper[vm_id] = (server_pool_type, value["server_number"], vm_object)

                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
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
            self.mig += 1
            time_stamp, num_cores, ram_needed, c_d = map(float, tup[2:])
            task_id, vm_id = tup[0], tup[1]
            del_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed, 4)
            create_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed, 1)
            # print "Starting Migration..."
            self.execute(del_tup, True)
            self.execute(create_tup, True)
            # print "Finished Migration..."

        else:
            if can_delay == True:
                if original_time_stamp == None and self.duplicate_vms[vm_id] > 0:
                    # print vm_id
                    # print task_id
                    task_id = task_id + '_' + str(self.duplicate_vms[vm_id] - 1)
                    vm_id = vm_id + '_' + str(self.duplicate_vms[vm_id] - 1)
                    # print vm_id
                    # print task_id
                if vm_id in self.delayed_vms:
                    if self.vm_delay_time[vm_id] == 0:
                        if original_time_stamp == None:
                            delayed_del_tup = (time_stamp, task_id, orig_vm_id, num_cores, ram_needed, c_d, time_stamp)
                            self.delayed_delete_map[vm_id] = delayed_del_tup
                        else:
                            print "Some Serious Problem.."
                            exit(-1)
                        return
                    else:
                        temp_delay_time = time_stamp + self.vm_delay_time[vm_id]
                        delayed_del_tup = (temp_delay_time, task_id, orig_vm_id, num_cores, ram_needed, c_d, time_stamp)
                        heappush(self.delayed_heap, delayed_del_tup)
                        self.vm_delay_time.pop(vm_id)
                        if original_time_stamp == None:
                            return

            data = self.vm_server_mapper[vm_id]
            self.vm_server_mapper.pop(vm_id)
            if not data:
                # self.ignore_vms_list.add(vm_id)
                return
            if can_delay == False:
                self.delayed_vms.remove(vm_id)

            pool_type, server_num = data[0], data[1]
            num_cores = data[2].cores
            ram_needed = data[2].ram
            tmp_num_cores = self.allocation_dict[pool_type][server_num].num_cores_left
            tmp_ram = self.allocation_dict[pool_type][server_num].ram_left

            self.allocation_dict[pool_type][server_num].num_cores_left += num_cores
            self.allocation_dict[pool_type][server_num].ram_left += ram_needed
            # print "Deleting VM: "+vm_id+" from Server: ",data,"Available Resources on Server after deletion is: ",self.allocation_dict[pool_type][server_num].ram_left," RAM ",self.allocation_dict[pool_type][server_num].num_cores_left," Cores ","Servers Used: ",self.servers_used

            # Updating all the stats like, total number of cores,ram used in the data center, core and ram utilization in the datacenter and the
            # number of servers switched on in the datacenter.
            self.num_cores_used -= num_cores

            if self.allocation_dict[pool_type][server_num].num_cores_left == config["servers"]["types"][pool_type][
                "max_cores_per_server"] and self.allocation_dict[pool_type][
                server_num].ram_left == config["servers"]["types"][pool_type]["max_ram_per_server"]:
                del self.allocation_dict[pool_type][server_num]
                self.servers_used -= 1
                # print "Switching Off Server: ",data

            if self.servers_used == 0:
                return

            prev_timestamp = None
            if self.stats_time:
                prev_timestamp = self.stats_time[-1]

            if prev_timestamp != None and time_stamp == prev_timestamp:
                self.num_cores[-1] = (self.num_cores_used)

            else:
                self.num_cores.append(self.num_cores_used)
                self.stats_time.append(time_stamp)

            if self.delayed_vm_reqs:
                while (self.num_cores_used + float(self.delayed_vm_reqs[0][3]) <= self.core_hard_limit):
                    waiting_tup = self.delayed_vm_reqs.popleft()
                    temp_tup = (
                        waiting_tup[0], waiting_tup[1], time_stamp, waiting_tup[3], waiting_tup[4], waiting_tup[5])
                    orig_time_stamp = float(waiting_tup[6])
                    self.execute(temp_tup, run_delayed_tasks=False, can_delay=False,
                                 original_time_stamp=orig_time_stamp)
                    if not self.delayed_vm_reqs:
                        break

    def final(self):
        orig_final_time = self.stats_time[-1]
        max_events_delay = 0
        print "Reached Final..", "Cores Used is: ", self.num_cores_used
        print "Original Final Time: ", orig_final_time
        sys.stdout.flush()

        y = None
        if self.delayed_heap:
            y = self.delayed_heap[0][0]
        while (y != None):
            del_req = heappop(self.delayed_heap)  # (time_stamp,task_id, vm_id, num_cores, ram_needed, c_d)
            del_tup = (del_req[1], del_req[2], del_req[0], del_req[3], del_req[4], del_req[5])
            self.execute(del_tup, run_delayed_tasks=False, can_delay=False, original_time_stamp=del_req[6])
            if self.delayed_heap:
                y = self.delayed_heap[0][0]
            else:
                y = None
        final_time = self.stats_time[-1]
        print "Simulation time Delayed by: ", final_time - orig_final_time
        print "Number of Ignored Requests at end: ", len(self.delayed_vm_reqs)
        print "Number of Unique Delay Items: ", len(self.vm_delay_histogram)
        print "Total Number of VMs Delayed: ", self.delayed_vm_count
        print "Max Cores Used: ", self.max_cores_used
        sys.stdout.flush()

        del self.allocation_dict
        del self.vm_server_mapper
        del self.ignore_vms_list
        del self.delayed_vm_reqs
        del self.delayed_vms
        del self.delayed_delete_map
        del self.delayed_heap
        del self.duplicate_vms
        del self.vm_delay_time
        self.vm_delay_histogram = OrderedDict(sorted(self.vm_delay_histogram.iteritems(), key=lambda t: t[0]))

