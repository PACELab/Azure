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
    """
    This algorithm object is created in executory.py and this object is used to execute each request from the feeder.py.
    """
    def __init__(self):
        self.allocation_dict = defaultdict(dict)
        self.vm_server_mapper = defaultdict(lambda: None)
        self.num_cores_used = 0
        self.ram_used = 0
        self.servers_used = 0
        self.ram_ratio_sum = 0
        self.num_cores = []
        self.queue = deque()
        self.calculation_time = (86400000000*float(config["window_size"]))
        self.cum_sum = 0.00000
        self.cum_sum_sqr = 0.00000
        self.total_samples = 0
        self.mean = 0
        self.standard_deviation = 0
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
        self.min_delay = float(config["delay_time"])
        # self.prediction_max = None #Used for dynamic vm placement algo.
        path = os.path.join(get_parent_path(), config["actual_output_path"])
        if config["mode"] == "debug" and path:
            open(path, 'w').close()

    def execute(self, tup, run_delayed_tasks=True, can_delay=True, original_time_stamp=None, migrate=False):
        """
        This function is called from executor.py by using the Algorithm object created.
        
        tup is a tuple with 6 items of the form (task_id,job_id,timestamp,cores,ram,typeofrequest)
            typeofrequest can be any integer from 1 to 8. 1 indicates create, 8 indicates modify, other integers indicate delete
            
        run_delayed_tasks is a parameter which indicates if any delayed tasks can be run or not.(default value = true)
        
        can_delay is a parameter which indicates if current request can be delayed or not.(default value = true)
        
        original_time_stamp is the time at which the current request(tup) arrived. (default value = None if this request is being served without delay)
        
        migrate indicates if the current execute call is being made because of modification request. (default value = false)
        """
        
        time_stamp, num_cores, ram_needed, c_d = map(float, tup[2:])
        task_id, vm_id = tup[0], tup[1]
        orig_vm_id = vm_id
        
        """This part of code ensures that unique vm_id is assigned for each request as there can be multiple request with same task_id and job_id.
        
        Explanation:
          Reason for multiple requests with same task_id and job_id is, according to google trace data, any VM which ends abruptly is restarted.
          Since we delay some requests based on some conditions, there is a chance that we end up with a new req with same task_id and job_id before
          we deploy the older one.
          
        Hence a count is maintained for each unique task_id and vm_id and it is appened at the end when we get a create request.
        """
        vm_id = str(vm_id) + '_' + str(task_id)
        if original_time_stamp == None and int(c_d) == 1:
            task_id = str(task_id) + '_' + str(self.duplicate_vms[vm_id])
            self.duplicate_vms[vm_id] += 1
            vm_id = str(orig_vm_id) + '_' + str(task_id)
            # print vm_id

        """
        If run_delayed tasks is set to true, then we check if there are any delayed requests that needs to be served before this request is served.
        Reason being, there might be some delayed request which might have crossed the max_delay, so we check if they can be served at this point of time.Two kinds of request would be waiting and they are create requests and delete requests. Therefore we check both the queues and decide which one needs to be served first if they need to be served before the current request.
        """
        if run_delayed_tasks == True:
            while True:
                delete_head = None
                run_flag = False
                if self.delayed_heap:  #get oldest awaiting delete request
                    if self.delayed_heap[0][0] <= time_stamp:
                        delete_head = self.delayed_heap[0][0]
                        run_flag = True

                create_head = None
                if self.delayed_vm_reqs:  #get oldest awaiting create request
                    if time_stamp - self.delayed_vm_reqs[0][-1] >= self.min_delay and (
                            self.num_cores_used + float(self.delayed_vm_reqs[0][3]) <= self.core_hard_limit):
                        create_head = self.delayed_vm_reqs[0][2]
                        run_flag = True

                if run_flag:
                    #decide which one needs to be served, i.e create req or delete req
                    if not create_head:
                        min_stamp, typ = delete_head, "d"

                    elif not delete_head:
                        min_stamp, typ = create_head, "c"
                    else:
                        min_stamp = min(float(create_head), float(delete_head))
                        if min_stamp == delete_head:
                            typ = "d"
                        else:
                            typ = "c"

                    if typ == "d":
                        del_req = heappop(
                            self.delayed_heap)  # (time_stamp,task_id, vm_id, num_cores, ram_needed, c_d,orig_time_stamp)
                        del_tup = (del_req[1], del_req[2], del_req[0], del_req[3], del_req[4], del_req[5])
                        self.execute(del_tup, run_delayed_tasks=False, can_delay=False, original_time_stamp=del_req[6])

                    if typ == "c":
                        waiting_tup = self.delayed_vm_reqs.popleft()
                        temp_tup = (
                            waiting_tup[0], waiting_tup[1], waiting_tup[2], waiting_tup[3], waiting_tup[4],
                            waiting_tup[5])
                        orig_time_stamp = float(waiting_tup[6])
                        self.execute(temp_tup, run_delayed_tasks=False, can_delay=False,
                                     original_time_stamp=orig_time_stamp)
                else:
                    break

        if int(c_d) == 1:  # c inidcates create request
            # According to the feeder.csv built, delete and create requests would come at the same time and delete requests would fall before
            # create requests, therefore those create requests are to ignored.

            if (can_delay == True) and ((self.num_cores_used + num_cores > self.core_hard_limit) or (
                    self.num_cores_used > self.mean+(float(config["multiplier"])*self.standard_deviation))): #conditions which would decide if this req should be delayed.
                if original_time_stamp == None:
                    delayed_tup = (
                        task_id, orig_vm_id, time_stamp + self.delay_time, num_cores, ram_needed, c_d, time_stamp)
                    self.delayed_vm_count += 1
                    self.delayed_vms.add(vm_id) #adding the current vm_id to delayed_vms set to find out if vm is delayed or not in O(1)
                else:
                    delayed_tup = (
                        task_id, orig_vm_id, time_stamp + self.delay_time, num_cores, ram_needed, c_d,
                        original_time_stamp)
                if not self.delayed_vm_reqs:
                    self.delay_time_stamp = time_stamp + self.delay_time
                self.delayed_vm_reqs.append(delayed_tup) #we add it to the delayed_vm_reqs deque.all the delayed create request are stored in the order they arrive.
                return #and then return

            #if code execution has reached this point, then it means that the req can be served right now.
            
            #We store the delay time of VMs in vm_delay_time dict which would be used at the end to build stats.
            if original_time_stamp != None:
                self.vm_delay_time[vm_id] = time_stamp - original_time_stamp
            else:
                self.vm_delay_time[vm_id] = 0
                original_time_stamp = time_stamp

            #If this vm_id is already in delayed_delete_map then it means that the corresponding delete request has arrived before its create req has been served and even the delete req has been delayed, but it does not by what time the delete must be delayed. Since, now we know by what time VM is delayed, we would delay the delete request also by the same time.
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
                
            """current we assume there is only one kind of servers in the datacenter. To play around with this you can change the config file, indicating how many servers of each type are present.
            Also, round robin scheme is used to deploy."""
            for server_pool_type, value in config["servers"]["types"].iteritems():
                # print "s", "v", server_pool_type, value
                # print json.dumps(config,indent=4)
                number_of_servers = value["number"]  # number of servers present of this pool-type.
                # print "num sevrers",number_of_servers
                
                """If there is enough ram and cores on this server, then we deploy otherwise we continue"""
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

                        t = ceil(float(time_stamp - original_time_stamp) / 1000000)
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
                            self.cum_sum -= self.num_cores[-1]
                            self.cum_sum_sqr -= self.num_cores[-1] * self.num_cores[-1]
                            self.total_samples -= 1
                            self.num_cores[-1] = (self.num_cores_used)
                            self.stats_update(time_stamp)
                        else:
                            if prev_timestamp != None and prev_timestamp > time_stamp:
                                print prev_timestamp, time_stamp
                                print "Some Serious Problem..."
                                print "Cores Used: ", self.num_cores_used
                                1 / 0
                                exit(-1)
                            self.num_cores.append(self.num_cores_used)
                            self.stats_update(time_stamp)
                            self.stats_time.append(time_stamp)

                        self.max_cores_used = max(self.max_cores_used, self.num_cores_used)
                        vm_object = VM(vm_id, num_cores, ram_needed)
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
            """Modification request is treated as a delete request and create request, as it is deleting the old instance and creating the new instance.
            """
            self.mig += 1
            time_stamp, num_cores, ram_needed, c_d = map(float, tup[2:])
            task_id, vm_id = tup[0], tup[1]
            del_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed, 4)
            create_tup = (task_id, vm_id, time_stamp, num_cores, ram_needed, 1)
            # print "Starting Modification..."
            self.execute(del_tup, migrate=True) #recurisve calls
            self.execute(create_tup, migrate=True) #recursive calls
            # print "Finished Modification..."

        else:
            """This block simulates the delete request behaviour."""
            if can_delay == True:
                """
                If original_time_stamp is None, then it means that the delete of VM is requested for the first time. So, we try to frame the unique vm_id from the count maintained.
                """
                if original_time_stamp == None and self.duplicate_vms[vm_id] > 0:
                    # print vm_id
                    # print task_id
                    task_id = task_id + '_' + str(self.duplicate_vms[vm_id] - 1)
                    vm_id = vm_id + '_' + str(self.duplicate_vms[vm_id] - 1)
                    # print vm_id
                    # print task_id
                """
                If vm_id in delayed_vms, then it indicates that create request is delayed.
                """
                if vm_id in self.delayed_vms:
                    """
                    If vm_delay_time of this vm is zero, then it means that create request of VM is not served yet, so we insert into delayed_delete_map as we donot by how much time this delete request must be delayed.
                    """
                    if self.vm_delay_time[vm_id] == 0:
                        if original_time_stamp == None:
                            delayed_del_tup = (time_stamp, task_id, orig_vm_id, num_cores, ram_needed, c_d, time_stamp)
                            self.delayed_delete_map[vm_id] = delayed_del_tup
                        else:
                            print "Some Serious Problem.."
                            exit(-1)
                        return
                    """
                    If we know by how much the create req is delayed, then we delay the delay the delete req by same amount of time and insert it into delayed_heap.
                    """
                    else:
                        temp_delay_time = time_stamp + self.vm_delay_time[vm_id]
                        delayed_del_tup = (temp_delay_time, task_id, orig_vm_id, num_cores, ram_needed, c_d, time_stamp)
                        heappush(self.delayed_heap, delayed_del_tup)
                        self.vm_delay_time.pop(vm_id)
                        if original_time_stamp == None:
                            return

            #If we reach this point of execution, then it means this delete req needs to be served right now.
            
            """
            Now we do necessary book keeping to release the cores and ram that VM was using.
            """
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
                self.cum_sum -= self.num_cores[-1]
                self.cum_sum_sqr -= self.num_cores[-1] * self.num_cores[-1]
                self.total_samples -= 1
                self.num_cores[-1] = (self.num_cores_used)
                self.stats_update(time_stamp)

            else:
                if prev_timestamp != None and prev_timestamp > time_stamp:
                    print "Some Serious Problem.."
                    exit(-1)
                self.num_cores.append(self.num_cores_used)
                self.stats_update(time_stamp)
                self.stats_time.append(time_stamp)

            """
            Since, we delete a VM, extra space might be created which would be enough to deploy the VMs which were delayed earlier because of space constraints. Thus we check if there are any such VMs and deploy them right away.
            """
            if self.delayed_vm_reqs:
                flag = True
                # if time_stamp - self.delayed_vm_reqs[0][-1]>= self.min_delay:
                #     flag = True
                # else:
                #     flag = False

                if num_cores >= float(self.delayed_vm_reqs[0][3]) or time_stamp - self.delayed_vm_reqs[0][
                    -1] >= self.min_delay:
                    flag = True
                else:
                    flag = False

                while flag and (self.num_cores_used + float(self.delayed_vm_reqs[0][3]) <= self.core_hard_limit):
                    num_cores -= float(self.delayed_vm_reqs[0][3])
                    waiting_tup = self.delayed_vm_reqs.popleft()
                    temp_tup = (
                        waiting_tup[0], waiting_tup[1], time_stamp, waiting_tup[3], waiting_tup[4], waiting_tup[5])
                    orig_time_stamp = float(waiting_tup[6])
                    self.execute(temp_tup, run_delayed_tasks=False, can_delay=False,
                                 original_time_stamp=orig_time_stamp)
                    if not self.delayed_vm_reqs:
                        break
                    else:
                        # if time_stamp - float(self.delayed_vm_reqs[0][-1]) > self.min_delay:
                        if num_cores >= float(self.delayed_vm_reqs[0][3]) or time_stamp - self.delayed_vm_reqs[0][
                            -1] >= self.min_delay:
                            flag = True
                        else:
                            flag = False

    def stats_update(self,present_time_stamp):
        while self.queue and int(present_time_stamp)-int(self.queue[0])>self.calculation_time:
            cores = self.queue.popleft()
            self.cum_sum -= cores
            self.cum_sum_sqr -= cores * cores
            self.total_samples -= 1
        self.cum_sum += self.num_cores_used
        self.cum_sum_sqr += self.num_cores_used * self.num_cores_used
        self.total_samples += 1
        self.queue.append(self.num_cores_used)
        self.mean = self.cum_sum/self.total_samples
        self.standard_deviation = sqrt(self.cum_sum_sqr/self.total_samples - self.mean*self.mean)

    def final(self, flag=True):
        """
        This final function is used to print some stats required and delete unnecessary data structures before we start creating graphs as create graphs are high memory consumption tasks.
        """
        orig_final_time = self.stats_time[-1]
        max_events_delay = 0
        print "Reached Final..", "Cores Used is: ", self.num_cores_used
        print "Original Final Time: ", orig_final_time
        sys.stdout.flush()

        """
        By the end some delayed request might be left out, if we want to simulate them as well, them we need to comment out the below block.
        """
        
        """
        while True:
            delete_head = None
            run_flag = False
            if self.delayed_heap:
                delete_head = self.delayed_heap[0][0]
                run_flag = True

            create_head = None
            if self.delayed_vm_reqs:
                if (self.num_cores_used + float(self.delayed_vm_reqs[0][3]) <= self.core_hard_limit):
                    create_head = self.delayed_vm_reqs[0][2]
                    run_flag = True

            if run_flag:
                if not create_head:
                    min_stamp, typ = delete_head, "d"

                elif not delete_head:
                    min_stamp, typ = create_head, "c"
                else:
                    min_stamp = min(float(create_head), float(delete_head))
                    if min_stamp == delete_head:
                        typ = "d"
                    else:
                        typ = "c"

                if typ == "d":
                    del_req = heappop(
                        self.delayed_heap)  # (time_stamp,task_id, vm_id, num_cores, ram_needed, c_d,orig_time_stamp)
                    del_tup = (del_req[1], del_req[2], del_req[0], del_req[3], del_req[4], del_req[5])
                    self.execute(del_tup, run_delayed_tasks=False, can_delay=False, original_time_stamp=del_req[6])

                if typ == "c":
                    waiting_tup = self.delayed_vm_reqs.popleft()
                    temp_tup = (
                        waiting_tup[0], waiting_tup[1], waiting_tup[2], waiting_tup[3], waiting_tup[4], waiting_tup[5])
                    orig_time_stamp = float(waiting_tup[6])
                    self.execute(temp_tup, run_delayed_tasks=False, can_delay=False,
                                 original_time_stamp=orig_time_stamp)
            else:
                break

        final_time = self.stats_time[-1]
        for vm_id in (key for key in self.vm_server_mapper):
            del_tup = (0, vm_id, final_time, 1, 1, 4)
            self.execute(del_tup, run_delayed_tasks=False, can_delay=False, original_time_stamp=0)

        """
        print "Cores Used: ", self.num_cores_used
        final_time = self.stats_time[-1]

        if flag:
            print "Simulation time Delayed by: ", float(final_time - orig_final_time) / 60000000
            print "Number of Ignored Requests at end: ", len(self.delayed_vm_reqs)
            print "Number of Unique Delay Items: ", len(self.vm_delay_histogram)
            print "Total Number of VMs Delayed: ", self.delayed_vm_count
            print "Max Cores Used: ", self.max_cores_used
            print "number of ignored vms :", len(self.delayed_heap), len(self.delayed_vm_reqs)
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
        else:
            return final_time

