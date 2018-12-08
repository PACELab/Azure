import matplotlib
import numpy as np

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig
from scheduler.config_loader import *
from collections import defaultdict
from collections import OrderedDict
from collections import namedtuple
import os
import cPickle as pickle
import traceback
from itertools import *
import sys


class DataVisualizer(object):
    def __init__(self, obj=None):
        if obj:
            self.data_obj = obj
            """
            To create custom locations for each variant of the same algorithm, we can add that here so that it creates unique path for each variant.
            """
            if config["algorithm"]=='round_robin_delay_v2':
                self.final_path = 'Graphs/{algo}/{cf}_d{delay}_md{max_delay}_sm{stdmultiplier}'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], stdmultiplier=config['stdMultiplier'])
            elif config["algorithm"]=='round_robin_delay_v3':
                self.final_path = 'Graphs/{algo}/{cf}_d{delay}_ds{delay_step}'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'],delay_step=config['delay_step'])
            elif config["algorithm"]=='round_robin_delay':
                self.final_path = 'Graphs/{algo}/{cf}_d{delay}_md{max_delay}_cl{cores_limit}'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], cores_limit=config['cores_hard_limit'])
            elif config["algorithm"]=='round_robin_delay_v1':
                self.final_path = 'Graphs/{algo}/{cf}_d{delay}_sm{stdmultiplier}'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], stdmultiplier=config['stdMultiplier'])
            elif config["algorithm"]=='round_robin_delay_v4':
                self.final_path = 'Graphs/{algo}/{cf}_d{delay}_md{max_delay}_m{multiplier}'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], multiplier=config['multiplier'])
            else:
                self.final_path = 'Graphs/{algo}/{cf}_75std_5day'.format(algo=config["algorithm"], cf=config_file)
            if not os.path.isdir(self.final_path):
                os.makedirs(self.final_path)
        else:
            """
            Initially we were using python pickle dumps to store algorith objects, but later we moved on to store data into txt files as python dumps consuming lot of memory.
            """
            fp = "/mnt/google_data/scheduler/Graphs/round_robin_new/config_16/data_store.file1"
            fp2 = "/mnt/google_data/scheduler/Graphs/round_robin_delay_efficient/config_16/data_store.file2"
            # with open(fp, "rb") as f:
            #     OBJ = namedtuple('OBJ',['stats_time', 'num_cores', 'vm_delay_histogram'])
            #     self.data_obj = OBJ._make(pickle.load(f))

            with open(fp, "rb") as f:
                OBJ = namedtuple('OBJ',['stats_time', 'num_cores'])
                self.data_obj = OBJ._make(pickle.load(f))
            print "Pickle Load Complete..."
            sys.stdout.flush()
            self.final_path = 'Graphs/{algo}/{cf}'.format(algo=config["algorithm"], cf=config_file)
            if not os.path.isdir(self.final_path):
                os.makedirs(self.final_path)
        self.values = self.data_obj.vm_delay_histogram.values()
        self.cdf = np.cumsum(np.array(self.values), dtype=np.float64)
        self.keys = self.data_obj.vm_delay_histogram.keys()
        self.stats_time = self.data_obj.stats_time
        self.num_cores = self.data_obj.num_cores
        del self.data_obj
        try:
          self.store_to_files()
        except Exception as e:
          print e.message
          traceback.print_exc()

    def visualize(self):
        """
        This function uses core stats and delay stats to plot necessary Grpahs.
        
        Initially we used to plot a lot more graphs, but because of memory constraints we had to remove all unnecesary Grpahs which would provide an overview on many other things. For now, only cores_usage and cdf of delay of VMs is being plotted. Might need additional modification to plot other things.
        """
        plt.figure()
        plt.plot(self.stats_time, self.num_cores)
        plt.ylabel('total num cores used')
        savefig('{fp}/cores_used.jpeg'.format(fp=self.final_path))
        plt.close()
        del self.stats_time
        del self.num_cores

        print "25th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 25) / 47657184) * 100
        print "50th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 50) / 47657184) * 100
        print "75th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 75) / 47657184) * 100
        print "90th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 90) / 47657184) * 100
        print "95th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 95) / 47657184) * 100
        print "99th Percentile of Delayed VMs: ", float(np.percentile(self.cdf, 99) / 47657184) * 100

        print "Number of Delayed VMs: ", self.cdf[-1]-self.cdf[0]
        print "Number of max delayed VMs: ", self.values[-1]
        print "Max Delay Time of a VM: ", self.keys[-1]

        plt.figure()
        plt.plot(self.keys, self.cdf)
        plt.ylabel('CDF (in number of vms delayed)')
        plt.xlabel('Delay (in seconds)')
        savefig('{fp}/cdf_vms.png'.format(fp=self.final_path), dpi=1000)
        plt.close()

        plt.figure()
        plt.plot(self.keys, (self.cdf / float(self.cdf[-1])) * 100)
        plt.ylabel('CDF (in percentage)')
        plt.xlabel('Delay (in seconds)')
        savefig('{fp}/cdf_vms_percentage.png'.format(fp=self.final_path), dpi=1000)
        plt.close()

        # plt.bar not working with large inputs.
        """plt.figure()
        plt.bar(keys,values,width=1.0, color='g')
        plt.ylabel('Number of Vms')
        plt.xlabel('Delay in seconds')
        savefig('{fp}/histogram_vms.png'.format(fp=self.final_path),dpi=1000)
        plt.close()"""

        #####
        """
        plt.figure()
        plt.plot(self.data_obj.stats_time, self.data_obj.amount_ram)
        plt.ylabel('total amount of ram used')
        savefig('{fp}/ram_used.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        
        plt.plot(self.data_obj.creation_stats_time, self.data_obj.cores_creation_lst)
        plt.ylabel('Cores Requested')
        savefig('{fp}/cores_requested.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        plt.plot(self.data_obj.creation_stats_time, self.data_obj.ram_creation_lst)
        plt.ylabel('RAM Requested')
        savefig('{fp}/ram_requested.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        plt.plot(self.data_obj.deletion_stats_time, self.data_obj.cores_deletion_lst)
        plt.ylabel('Cores Deleted')
        savefig('{fp}/cores_deletion.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        plt.plot(self.data_obj.deletion_stats_time, self.data_obj.ram_deletion_lst)
        plt.ylabel('RAM Deleted')
        savefig('{fp}/ram_deletion.jpeg'.format(fp=self.final_path))
        plt.close()

        del self.data_obj.creation_stats_time[0]
        del self.data_obj.cores_creation_lst[0]
        del self.data_obj.ram_creation_lst[0]
        plt.figure()
        plt.plot(self.data_obj.creation_stats_time, self.data_obj.cores_creation_lst)
        plt.ylabel('Cores Requested')
        savefig('{fp}/cores_requested_new.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        plt.plot(self.data_obj.creation_stats_time, self.data_obj.ram_creation_lst)
        plt.ylabel('RAM Requested')
        savefig('{fp}/ram_requested_new.jpeg'.format(fp=self.final_path))
        plt.close()

        del self.data_obj.deletion_stats_time[-1]
        del self.data_obj.cores_deletion_lst[-1]
        del self.data_obj.ram_deletion_lst[-1]
        plt.figure()
        plt.plot(self.data_obj.deletion_stats_time, self.data_obj.cores_deletion_lst)
        plt.ylabel('Cores Deleted')
        savefig('{fp}/cores_deletion_new.jpeg'.format(fp=self.final_path))
        plt.close()
        plt.figure()
        plt.plot(self.data_obj.deletion_stats_time, self.data_obj.ram_deletion_lst)
        plt.ylabel('RAM Deleted')
        savefig('{fp}/ram_deletion_new.jpeg'.format(fp=self.final_path))
        plt.close()
        """
        ######

    def store_to_files(self):
        """
        This function writes to files both the core_stats and delay_stats.
        """
        core_stats = "{}/core_stats.txt".format(self.final_path)
        with open(core_stats, 'w') as f:
            for time, cores in izip(self.stats_time, self.num_cores):
                f.write(str(time) + ',' + str(cores) + '\n')

        delay_stats = "{}/delay_stats.txt".format(self.final_path)
        with open(delay_stats, 'w') as f:
            for key, value in izip(self.keys, self.values):
                f.write(str(key) + ',' + str(value) + '\n')

    def plot_two_schemes(self, normal_obj):
        diff_cores_used = defaultdict(lambda: 0)
        # diff_effective_cores_used = defaultdict(lambda : 0)

        for time, cores, eff_cores in zip(self.data_obj.stats_time, self.data_obj.num_cores,
                                          self.data_obj.effective_cores_lst):
            # if time==0 or time==self.data_obj.stats_time[-1]:
            #    continue
            diff_cores_used[time] -= cores
            # diff_effective_cores_used[time] -= eff_cores

        for time, cores, eff_cores in zip(normal_obj.stats_time, normal_obj.num_cores, normal_obj.effective_cores_lst):
            # if time==0 or time==normal_obj.stats_time[-1]:
            #    continue
            diff_cores_used[time] += cores
            diff_cores_used[time] = (float(diff_cores_used[time]) * 100) / float(cores)

            # diff_effective_cores_used[time] += eff_cores
            # diff_effective_cores_used[time] = (float(diff_effective_cores_used[time])*100)/float(cores)

        for time in diff_cores_used.keys():
            if abs(diff_cores_used[time]) > 100:
                diff_cores_used[time] = -100
        # for time in diff_effective_cores_used.keys():
        #     if abs(diff_effective_cores_used[time])>100:
        #         diff_effective_cores_used[time]=-100

        diff_cores_used = OrderedDict(sorted(diff_cores_used.iteritems(), key=lambda t: t[0]))
        # diff_effective_cores_used = OrderedDict(sorted(diff_effective_cores_used.items(),key = lambda t:t[0]))

        plt.figure()
        plt.plot(diff_cores_used.keys(), diff_cores_used.values())
        plt.ylabel('Normal Cores - Delayed Cores')
        plt.xlabel('time in seconds')
        plt.ylim(-7.5, 7.5)
        savefig('{fp}/compare_diff_cores_used.png'.format(fp=self.final_path), dpi=1000)
        plt.close()

        # plt.figure()
        # plt.plot(diff_effective_cores_used.keys(), diff_effective_cores_used.values())
        # plt.ylabel('Normal Eff Cores - Delayed Eff Cores')
        # plt.xlabel('time in seconds')
        # plt.ylim(-7.5,7.5)
        # savefig('{fp}/compare_eff_diff_cores_used.png'.format(fp=self.final_path),dpi=1000)
        # plt.close()

        plt.figure()
        plt.plot(self.data_obj.stats_time, self.data_obj.num_cores)
        plt.plot(normal_obj.stats_time, normal_obj.num_cores)
        plt.ylabel('total num cores used')
        plt.xlabel('time in seconds')
        plt.legend(['Delayed', 'Normal'])
        savefig('{fp}/compare_cores_used.png'.format(fp=self.final_path), dpi=1000)
        plt.close()

        # plt.figure()
        # plt.plot(self.data_obj.stats_time, self.data_obj.effective_cores_lst)
        # plt.plot(normal_obj.stats_time, normal_obj.effective_cores_lst)
        # plt.ylabel('total effective cores used')
        # plt.xlabel('time in seconds')
        # plt.legend(['Delayed','Normal'])
        # savefig('{fp}/compare_effective_cores_used.jpeg'.format(fp=self.final_path),dpi=1000)
        # plt.close()

        plt.figure()
        plt.plot(self.data_obj.stats_hour_time, self.data_obj.avg_cores_hour)
        plt.plot(normal_obj.stats_hour_time, normal_obj.avg_cores_hour)
        plt.ylabel('Avg Cores Per Hour')
        plt.xlabel('time in seconds')
        plt.legend(['Delayed', 'Normal'])
        savefig('{fp}/compare_avg_cores_hour.jpeg'.format(fp=self.final_path), dpi=1000)
        plt.close()

        plt.figure()
        plt.plot(self.data_obj.stats_hour_time, self.data_obj.max_cores_hour)
        plt.plot(normal_obj.stats_hour_time, normal_obj.max_cores_hour)
        plt.ylabel('Max Cores Per Hour')
        plt.xlabel('time in seconds')
        plt.legend(['Delayed', 'Normal'])
        savefig('{fp}/compare_max_cores_hour.jpeg'.format(fp=self.final_path), dpi=1000)
        plt.close()

        plt.figure()
        plt.plot(self.data_obj.stats_hour_time, self.data_obj.p95_cores_hour)
        plt.plot(normal_obj.stats_hour_time, normal_obj.p95_cores_hour)
        plt.ylabel('p95 Cores Per Hour')
        plt.xlabel('time in seconds')
        plt.legend(['Delayed', 'Normal'])
        savefig('{fp}/compare_p95_cores_hour.jpeg'.format(fp=self.final_path), dpi=1000)
        plt.close()

    def check_path(self, final_path):
        if not os.path.isdir(final_path):
            os.makedirs(final_path)

    def common_visualiser(self, f_d):
        base_path = '/mnt/azure_data/AZURE/scheduler/Graphs/common'
        c = 0
        print  "fd is", f_d
        for key, value in f_d.iteritems():
            plt.figure(c)
            plt.ylabel('Total Servers Used')
            p = os.path.join(base_path, key)
            self.check_path(p)
            for v in value:
                algo_name, algo_obj = v
                plt.plot(algo_obj.used_servers_time, algo_obj.used_servers_lst)
            c += 1
            savefig('{fp}/servers_used.jpeg'.format(fp=p))

    def tmp_visualize(self):
        """
        This function is used to plot all the graphs of diff algorithms into the same graph for comparision. Graphs might look clumsy. Try to plot with dpi=1000 for high quality images.
        """
        base_line_path = "/mnt/google_data/scheduler/Graphs/round_robin_new/config_16_latest_fixed/core_stats.txt"
        greedy_delay = "/mnt/google_data/scheduler/Graphs/round_robin_min_delay/config_16_greedy_10min/core_stats.txt"
        fixed_delay = "/mnt/google_data/scheduler/Graphs/round_robin_min_delay/config_16_fixed_10min/core_stats.txt"
        delay_efficient = "/mnt/google_data/scheduler/Graphs/round_robin_delay_efficient/config_16/core_stats.txt"
        combined_path = "/mnt/google_data/scheduler/Graphs/combined"

        plt.figure()
        # baseline
        with open(base_line_path, "rb") as f:
            x,y = [],[]
            for l in f:
                x1,y1 = l.split(",")
                x.append(x1)
                y.append(y1)
            print "loaded obj1"
            plt.plot(x,y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()


        # # 2thresholds
        with open(greedy_delay, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(x1)
                y.append(y1)
            print "loaded obj2"
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()

        # 2thresholds_w
        with open(fixed_delay, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(x1)
                y.append(y1)
            print "loaded obj3"
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()

        # 1threshold
        with open(delay_efficient, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(x1)
                y.append(y1)
            print "loaded obj4"
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()

        plt.legend(("baseline", "greedy", "fixed","max"))
        #plt.legend(("baseline",  "1threshold"))
        plt.ylabel('total num cores used')
        plt.xlabel('time')
        plt.ylim(6000, 9000)
        savefig('{fp}/combined_core_usage.png'.format(fp=combined_path),dpi=1000)
        plt.close()
        
        """greedy_delay = "/mnt/google_data/scheduler/Graphs/round_robin_min_delay/config_16_greedy_10min/delay_stats.txt"
        fixed_delay = "/mnt/google_data/scheduler/Graphs/round_robin_min_delay/config_16_fixed_10min/delay_stats.txt"
        delay_efficient = "/mnt/google_data/scheduler/Graphs/round_robin_delay_efficient/config_16/delay_stats.txt"
        combined_path = "/mnt/google_data/scheduler/Graphs/combined"

        plt.figure()

        # # 2thresholds
        
        # 2thresholds_w
        with open(fixed_delay, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(float(x1))
                y.append(float(y1))
            print "loaded obj3"
            y = np.cumsum(np.array(y), dtype=np.float64)
            print "Fixed: "
            y = (y/float(y[-1]))*100
            step=80
            for j,i in zip(x,y):
                if i>=step:
                    print step,":",j
                    step+=2
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()
        
        with open(greedy_delay, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(float(x1))
                y.append(float(y1))
            print "loaded obj2"
            y = np.cumsum(np.array(y), dtype=np.float64)
            print "Greedy: "
            y = (y/float(y[-1]))*100
            step=80
            for j,i in zip(x,y):
                if i>=step:
                    print step,":",j
                    step+=2
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()

        # 1threshold
        with open(delay_efficient, "rb") as f:
            x, y = [], []
            for l in f:
                x1, y1 = l.split(",")
                x.append(float(x1))
                y.append(float(y1))
            print "loaded obj4"
            y = np.cumsum(np.array(y), dtype=np.float64)
            print "Normal Delay: "
            y = (y/float(y[-1]))*100
            step=80
            for j,i in zip(x,y):
                if i>=step:
                    print step,":",j
                    step+=2
            plt.plot(x, y, marker='', linewidth=2)
            del x
            del y
            sys.stdout.flush()

        plt.legend(("fixed","greedy","1threshold"))
        plt.ylabel('CDF(in percentage)')
        plt.xlabel('time')
        plt.ylim(0, 140)
        plt.yticks([0,10,20,30,40,50,60,70,80,90,100,110])
        plt.grid()
        savefig('{fp}/combined_cdf.png'.format(fp=combined_path),dpi=1000)
        plt.ylim(80, 107)
        plt.yticks([i for i in range(80,105,2)])
        plt.grid()
        savefig('{fp}/combined_cdf_80_100.png'.format(fp=combined_path),dpi=1000)
        plt.ylim(90, 104)
        plt.yticks([i for i in range(90,105,1)])
        plt.grid()
        savefig('{fp}/combined_cdf_90_100.png'.format(fp=combined_path),dpi=1000)
        plt.close()"""
