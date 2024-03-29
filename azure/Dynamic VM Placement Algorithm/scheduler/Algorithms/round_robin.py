from scheduler.Algorithms.allocation import Allocation
from scheduler.Algorithms.allocation import VM
from scheduler.exception import *
from scheduler.config_loader import *
from collections import defaultdict
from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error
import dill
import os
import scipy.signal as signal
import numpy as np
import math

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
        self.ignore_servers_list = set()
        
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
        self.prediction_max = None
        path = os.path.join(get_parent_path(),config["actual_output_path"])
        if config["mode"] == "debug" and path:
            open(path, 'w').close()
        self.B = None
        self.A = None


    def execute(self, tup):
        vm_id, time_stamp, num_cores, ram_needed, c_d = tup
        num_cores, ram_needed, time_stamp = float(num_cores), float(ram_needed), float(time_stamp)
        self.max_cores, self.max_ram = max(self.max_cores, num_cores), max(self.max_ram, ram_needed)


        if c_d.strip() == "c":
            if vm_id in self.ignore_vms_list:
                return
            for server_pool_type, value in config["servers"]["types"].iteritems():
                # print "s", "v", server_pool_type, value
                # print json.dumps(config,indent=4)
                number_of_servers = value["number"]
                # print "num sevrers",number_of_servers
                if ram_needed > value["max_ram_per_server"] or num_cores > value["max_cores_per_server"]:
                    continue
                p_f=0
                # print "num servers", number_of_servers
                for _ in xrange(number_of_servers):
                    c_f = 0
                    # print "server number is ", self.server_num
                    server_tup = (server_pool_type,value["server_number"])
                    if server_tup in self.ignore_servers_list:
                        print "Server: "+tup+" ignored because of migration.."
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        continue
                    if value["server_number"] not in self.allocation_dict[server_pool_type]:
                        # print "in allocation"
                        self.allocation_dict[server_pool_type][value["server_number"]] = Allocation(
                            type=server_pool_type)
                        c_f = 1 

                    if self.allocation_dict[server_pool_type][
                        value["server_number"]].num_cores_left - num_cores >= 0 and \
                            self.allocation_dict[server_pool_type][value["server_number"]].ram_left - ram_needed >= 0:
                        # print "in allocation dict"
                        if c_f == 1:
                            print "Switching On Server: ",server_tup
                            self.servers_used += 1
                        tmp_num_cores = self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left
                        tmp_ram = self.allocation_dict[server_pool_type][value["server_number"]].ram_left
                        self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left -= num_cores
                        self.allocation_dict[server_pool_type][value["server_number"]].ram_left -= ram_needed
                        print "Creating VM: "+vm_id+" on Server: ",server_tup,"Available Resources on Server after creation is: ",self.allocation_dict[server_pool_type][value["server_number"]].ram_left," RAM ",self.allocation_dict[server_pool_type][value["server_number"]].num_cores_left," Cores"
                        self.num_cores_used += num_cores
                        self.num_cores.append(self.num_cores_used)
                        self.num_cores_time.append(time_stamp)
                        self.ram_used += ram_needed
                        self.amount_ram.append(self.ram_used)
                        self.amount_ram_time.append(time_stamp)

                        """
                        Dynamic code starts here..
                        """
                        period = 0
                        period_timestamp = None
                        period,period_timestamp = self.periodicity(vm_id)
                        vm_object = VM(vm_id,num_cores,ram_needed,period,period_timestamp)
                        self.allocation_dict[server_pool_type][value["server_number"]].servers_list.append(vm_object)
                        
                        """
                        Dynamic code ends here..
                        """
                        self.vm_server_mapper[vm_id] = (server_pool_type, value["server_number"],vm_object) #

                        # compute average
                        v = self.allocation_dict[server_pool_type][value["server_number"]]
                        self.cores_ratio_sum = self.cores_ratio_sum + (
                                (v.total_num_cores - v.num_cores_left) / float(v.total_num_cores)) - (
                                                       (v.total_num_cores - tmp_num_cores) / float(v.total_num_cores))
                        self.ram_ratio_sum = self.ram_ratio_sum + (
                                (v.total_ram - v.ram_left) / float(v.total_ram)) - (
                                                     (v.total_ram - tmp_ram) / float(v.total_ram))
                        prev_timestamp = None
                        if len(self.avg_cpu_usage_time) > 0:
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
                        self.cores_histogram[num_cores] += 1
                        self.ram_histogram[ram_needed] += 1

                        if self.prev_time_stamp_c == time_stamp:
                            self.prev_c_cores += num_cores
                            self.prev_c_ram += ram_needed
                        else:
                            self.cores_creation_lst.append(self.prev_c_cores)
                            self.cores_creation_time.append(self.prev_time_stamp_c)
                            self.ram_creation_lst.append(self.prev_c_ram)
                            self.ram_creation_time.append(self.prev_time_stamp_c)
                            ##
                            self.prev_time_stamp_c = time_stamp
                            self.prev_c_cores = num_cores
                            self.prev_c_ram = ram_needed

                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
                        self.write_to_file(pool_type=server_pool_type)
                        return
                    else:
                        if p_f==0:
                          print "Searching server for VM: "+vm_id+" from server number - ", server_tup
                          p_f=1
                        value["server_number"] = (value["server_number"] + 1) % number_of_servers
            else:
                print "inside delete"
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

        elif c_d.strip() == "d":
            data = self.vm_server_mapper[vm_id]
            if not data:
                self.ignore_vms_list.add(vm_id)
                return
            pool_type, server_num,vm_object = data[0], data[1]
            tmp_num_cores = self.allocation_dict[pool_type][server_num].num_cores_left
            tmp_ram = self.allocation_dict[pool_type][server_num].ram_left
            self.allocation_dict[pool_type][server_num].num_cores_left += num_cores
            self.allocation_dict[pool_type][server_num].ram_left += ram_needed
            print "Deleting VM: "+vm_id+" from Server: ",data,"Available Resources on Server after deletion is: ",self.allocation_dict[pool_type][server_num].ram_left," RAM ",self.allocation_dict[pool_type][server_num].num_cores_left," Cores ","Servers Used: ",self.servers_used
            self.num_cores_used -= num_cores
            self.num_cores.append(self.num_cores_used)
            self.num_cores_time.append(time_stamp)
            self.ram_used -= ram_needed
            self.amount_ram.append(self.ram_used)
            self.amount_ram_time.append(time_stamp)
            # compute average
            v = self.allocation_dict[pool_type][server_num]
            self.cores_ratio_sum = self.cores_ratio_sum + (
                    (v.total_num_cores - v.num_cores_left) / float(v.total_num_cores)) - (
                                           (v.total_num_cores - tmp_num_cores) / float(v.total_num_cores))
            self.ram_ratio_sum = self.ram_ratio_sum + (
                    (v.total_ram - v.ram_left) / float(v.total_ram)) - (
                                         (v.total_ram - tmp_ram) / float(v.total_ram))

            if self.allocation_dict[pool_type][server_num].num_cores_left == config["servers"]["types"][pool_type]["max_cores_per_server"] and self.allocation_dict[pool_type][
                server_num].ram_left == config["servers"]["types"][pool_type]["max_ram_per_server"]:
                del self.allocation_dict[pool_type][server_num]
                self.servers_used -= 1
                print "Switching Off Server: ",data
                
            if self.servers_used==0:
                return
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

            if self.prev_time_stamp_d == time_stamp:
                self.prev_d_cores += num_cores
                self.prev_d_ram += ram_needed
            else:
                self.cores_deletion_lst.append(self.prev_d_cores)
                self.cores_deletion_time.append(self.prev_time_stamp_d)
                self.ram_deletion_lst.append(self.prev_d_ram)
                self.ram_deletion_time.append(self.prev_time_stamp_d)
                ##
                self.prev_time_stamp_d = time_stamp
                self.prev_d_cores = num_cores
                self.prev_d_ram = ram_needed
            self.write_to_file(pool_type=pool_type)
            
        elif c_d.strip() == "m":
            vm_id, time_stamp, num_cores, ram_needed, c_d = tup
            del_tup = (vm_id, time_stamp, num_cores, ram_needed, "d")
            create_tup = (vm_id, time_stamp, num_cores, ram_needed, "c")
            data = self.vm_server_mapper[vm_id]
            #if not data:
            #    self.ignore_vms_list.add(vm_id)    ### Need to handle this case elegantly...
            #    return
            print "Starting Migration..."
            self.ignore_servers_list.add(data)
            self.execute(del_tup)
            self.execute(create_tup)
            self.ignore_servers_list.remove(data)
            print "Finished Migration..."
        else:
            print c_d.strip(),"*******************************"
            

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
        self.cores_creation_lst.append(self.prev_c_cores)
        self.cores_creation_time.append(self.prev_time_stamp_c)
        self.ram_creation_lst.append(self.prev_c_ram)
        self.ram_creation_time.append(self.prev_time_stamp_c)

        self.cores_deletion_lst.append(self.prev_d_cores)
        self.cores_deletion_time.append(self.prev_time_stamp_d)
        self.ram_deletion_lst.append(self.prev_d_ram)
        self.ram_deletion_time.append(self.prev_time_stamp_d)
        
    def lowpass_filter(self,data,period):
        # First, design the Buterworth filter
        N  = 5    # Filter order
        Wn = 0.1 # Cutoff frequency
        self.B, self.A = signal.butter(N, Wn, output='ba')
        dataf = signal.filtfilt(self.B,self.A,data)
        print "Period: ",period,"Cutoff Freq: ",Wn,"***"
        print "Actual Data: ",data,"***"
        print "Filtered Data: ",dataf,"***"
        return dataf
        
    def periodicity(self,vm_id):
        sub,dep,vm = vm_id.split('_')
        vm_file = open("/mnt/azure_data/new_vm_data/"+sub+"/"+sub+"_"+dep+"/"+vm_id+".csv",'r+')
        time_count=0
        utilization_data = []
        for line in vm_file.readlines():
            data = line.split(',')
            utilization_data.append(data[4])  #need to check this is the 95th percentile
            time_count+=1
        vm_file.close()
        utilization_data = [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,]+[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,]+[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2]
        time_count=145
        period=0
        period_timestamp = None
        for i in range(12,time_count):    #assumption that atleast after one hour we observe the perio
            data = np.asarray(utilization_data[:i])
            W = np.fft.fft(data)
            freq = np.fft.fftfreq(len(data),300)
            threshold = abs(np.percentile(freq, 99))
            idx = np.where(abs(W)>threshold)[0][-1]
            temp_period = (int)(1/abs(freq[idx]))
            if((abs(temp_period-i*300)>5) and temp_period<(i*300)):
                print temp_period,i*300
                period=temp_period
                period_timestamp = i*300
        print "Period: ",period,"Period TS: ",period_timestamp,"VM ID: ",vm_id
        return period,period_timestamp
        
    def ARmodel(self,data):
        print "Length of Data to AR model: ",len(data)
        model = AR(data)
        model_fit = model.fit()
        predictions = model_fit.predict(start=len(data), end=len(data)+12, dynamic=True)   #predicting data for atleast 3 hrs.. i.e 12 * 3 = 36
        return predictions
    
    def consolidation(self,time_stamp):
        prediction_max = defaultdict(lambda:None)
        self.periodicVms = 0
        for vm in self.vm_server_mapper:
            pool,server,vm_object = self.vm_server_mapper[vm]
            sub,dep,vm1 = vm.split('_')
            vm_file = open("/mnt/azure_data/new_vm_data/"+sub+"/"+sub+"_"+dep+"/"+vm+".csv",'r+')
            time_data = []
            utilization_data = []
            timestamp_count=0
            for line in vm_file.readlines():
                data = line.split(',')
                utilization_data.append(data[4])  #needs to know which data to consolidate on
                timestamp_count+=1
                if(data[0]>time_stamp):
                  break
            vm_file.close()
            vm_period = vm_object.period
            vm_p_time_stamp = vm_object.period_timestamp
            
            #utilization_data = [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,]+[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,]+[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,48,46,44,42,40,38,36,34,32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2]
            timestamp_count=145
            
            if(vm_period==0 or vm_p_time_stamp==None or vm_p_time_stamp > time_stamp):
                prediction_max[vm] = 100
                continue
            self.periodicVms+=1
            filtered_data = self.lowpass_filter(utilization_data,vm_period)
            avg_period_data = [0 for i in range(int(math.floor(vm_period)/300)+1)]
            residual_data = [0 for i in range(timestamp_count)]
            start = (vm_p_time_stamp)+300
            end = timestamp_count*300
            i_period = vm_period/300
            n_count=0;
            while(start+vm_period<end):
                n_count+=1
                index = start%300
                window = i_period
                window_index = 0;
                while(windows_index<window):
                    avg_period_data[window_index]+=filtered_data[index]
                    window_index+=1
                    index+=1
                start+=vm_period
                    
            end = (vm_p_time_stamp)+(n_count*vm_period)
            start = vm_p_time_stamp
            while(start-vm_period>0):
                n_count+=1
                index = start%300
                window = i_period
                window_index = window;
                while(0<=window_index):
                    avg_period_data[window_index]+=filtered_data[index]
                    window_index-=1
                    index-=1
                start-=vm_period
            for i in range(i_period):
                avg_period_data[i]/=n_count
            for i in range(timestamp_count):
                m = i%i_period
                residual_data[i] = utilization_data[i]- avg_period_data[m]
            #print avg_period_data
            predicted_residual_data = self.ARmodel(residual_data)
            #print predicted_residual_data
            neglect_value_count = 0
            end = end+300
            while(end<=(timestamp_count*300)):
                neglect_value_count+=1
                end+=300
            index = 0
            print neglect_value_count
            predicted_total_data = [0 for i in range(36)]
            for i in range(neglect_value_count,2*i_period):
                 print i
                 predicted_total_data[index] = avg_period_data[i%i_period]+residual_data[index]
                 prediction_max[vm]=max(prediction_max[vm],predicted_total_data[index])
                 prediction_max[vm]=min(prediction_max[vm],100)
                 index+=1   
                 if(index==36):
                   break
        print prediction_max
        self.prediction_max = prediction_max
        
    def migration(self,time_stamp):
          prediction_list = [(self.prediction_max[vm],vm) for vm in self.prediction_max]
          prediction_list = sorted(prediction_list,reverse=True)
          
          """number_of_servers = config["servers"]["types"]["B"]["number"]
          server_number = 0
          server_pool_type = "B"
          for i in range(number_of_servers):
              if self,.allocation_dict[server_pool_type][i]:
                  self.allocation_dict[server_pool_type][i].num_cores_left = self.allocation_dict[server_pool_type][i].total_num_cores
                  self.allocation_dict[server_pool_type][i].ram_left = self.allocation_dict[server_pool_type][i].total_ram
                  self.allocation_dict[server_pool_type][i].servers_list = []
                  
          for pred,vm in prediction_list:
              pool,server,vm_object = self.vm_server_mapper[vm]
              sub,dep,vm1 = vm.split('_')
              least_overflow = float('inf')
              allocated = False
              best_machine=None
              for i in range(number_of_servers):
                  if (self.allocation_dict[server_pool_type][i].num_cores_left>((pred*vm_object.cores)/float(100))) and (self.allocation_dict[server_pool_type][i].ram_left>vm_object.ram):
                        self.allocation_dict[server_pool_type][i].servers_list.append(vm_object)
                        self.allocation_dict[server_pool_type][i].num_cores_left-= ((pred*vm_object.cores)/float(100))
                        self.allocation_dict[server_pool_type][i].ram_left-=vm_object.ram
                        allocated=True
                        self.vm_server_mapper[vm]= (pool,i,vm_object)
                        break
                  else:
                      overflow = abs(self.allocation_dict[server_pool_type][i].num_cores_left-((pred*vm_object.cores)/float(100)))
                      if overflow<least_overflow:
                          best_machine = i
                          least_overflow = overflow
              if(allocated==False):
                  self.allocation_dict[server_pool_type][best_machine].servers_list.append(vm_object)
                  self.allocation_dict[server_pool_type][best_machine].num_cores_left-= ((pred*vm_object.cores)/float(100))
                  self.allocation_dict[server_pool_type][best_machine].ram_left-=vm_object.ram
                  self.vm_server_mapper[vm]= (pool,i,vm_object)
                  
          print "Consolidation of all VMs Done.. Switching off whatever servers are free.."
          
          self.servers_used =0
          for i in range(number_of_servers):
                if self,.allocation_dict[server_pool_type][i]:
                  if self.allocation_dict[server_pool_type][i].servers_list == []:
                      del self.allocation_dict[server_pool_type][i]
                      print "Switched Off Server ",i
                  else:
                      self.servers_used += 1
                      
          print "Total Number of Servers Used at this point: ",servers_used """  
          
          create_request_list = []
          for pred,vm in prediction_list:
              num_cores = self.vm_server_mapper[vm].cores
              ram_needed = self.vm_server_mapper[vm].ram
              vm_id = vm
              del_tup = (vm_id, time_stamp, num_cores, ram_needed, "d")
              create_tup = (vm_id, time_stamp, (pred/float(100)*num_cores), ram_needed, "c")
              create_request_list.append(create_tup)
              self.execute(del_tup)
              
          for request in create_request_list:
              self.execute(request)
              
    
              
          
      
      
      
        
    
            
            
            
            
            

            
          
        
        
        
