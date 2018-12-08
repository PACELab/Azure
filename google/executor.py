import sys
import os.path
import os
import json
from collections import OrderedDict, defaultdict
import traceback
parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
sys.path.append(parent_dir)
from scheduler.data_vizualiser import DataVisualizer
from scheduler.config_loader import *
from scheduler.store import *
import importlib
import matplotlib.pyplot as plt
import pickle

from scheduler.feeder import Feeder

class Executor(object):
    def __init__(self, mode="execute",feeder_path = None):
        self.algo_name = config["algorithm"]
        self.mode = mode
        self.algo_obj = None

    def plot(self):
        import numpy as np
        import matplotlib.pyplot as plt
        sorted_data = np.sort(self.algo_obj.life_time_list)
        yvals = np.arange(len(sorted_data)) / float(len(sorted_data) - 1)
        plt.plot(sorted_data, yvals)
        plt.show()
        plt.savefig('/mnt/google_data/scheduler/cdf.png', dpi=1000)
        plt.close()

    def execute(self):
        path = "Algorithms." + config["algorithm"]
        algo = importlib.import_module(path)
        self.algo_obj = algo.Algorithm()
        feeder_gen = Feeder().execute()
        prev_time_stamp, prev_action, pres_cores_create, pres_ram_create, pres_cores_delete, pres_ram_delete = 0, 'd', 0, 0, 0, 0
        for tup in feeder_gen:
            self.algo_obj.execute(tup)
        # #consolidation_time = 2591700
        # #algo_obj.consolidation(consolidation_time) #needed for dynamic VM placement algorithm
        self.algo_obj.final()
        # # algo_obj.remove_duplicate_times()
        try:
            Store1(self.algo_obj)
            Store2(self.algo_obj)
        except Exception as e:
            print e.message
            traceback.print_exc()
        #self.load_previous_run_object()
        self.data_obj = DataVisualizer(self.algo_obj)
        #self.compare_delay_schemes()
        self.data_obj.visualize()
        #self.data_obj.tmp_visualize()
        
    def compare_delay_schemes(self):
        normal_scheme_file = "/mnt/azure_data/rakshith/scheduler/Graphs/round_robin/config_16/data_store.file"
        with open(normal_scheme_file,'rb') as f:
            self.normal_obj = pickle.load(f)
        self.data_obj.plot_two_schemes(self.normal_obj)
        
    def load_previous_run_object(self):
        delay_scheme_file = "/mnt/google_data/scheduler/Graphs/round_robin_new/config_16_unique/data_store.file"
        with open(delay_scheme_file,'rb') as f:
            self.algo_obj = pickle.load(f)
        

    def common_visualize(self):
        root_dir = "/mnt/azure_data/AZURE/scheduler/Graphs"
        f_d = defaultdict(list)
        for dir_name, sub_dir_list, file_list in os.walk(root_dir):
            print "dir",dir_name
            for fname in file_list:
                print "fname is",fname,"dirname",dir_name
                if fname == "data_store.file":
                    l = dir_name.split("/")
                    path = dir_name + "/data_store.file"
                    f_d[l[-1]].append((l[-2], Store().load_from_file(path)))
        DataVisualizer().common_visualiser(f_d)
        
    def compare_diff_schemes(self):
        #base_path = "/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v2"
        #dirs = ['config_16_d300_md1800_sm1', 'config_16_d300_md1800_sm2', 'config_16_d300_md3600_sm1', 'config_16_d300_md3600_sm2']
        #base_path = "/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v3"
        #dirs = ['config_16_d1200_ds2400', 'config_16_d600_ds1200', 'config_16_d1800_ds3600']
        base_path = "/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v4"
        dirs = ['config_16_d300_md3600_m0.5', 'config_16_d300_md3600_m0.6', 'config_16_d300_md3600_m0.7', 'config_16_d300_md3600_m0.8', 'config_16_d300_md3600_m0.9']
        objs = []
        for dir_name in dirs:
            path = os.path.join(base_path,dir_name)
            file_name = os.path.join(path,'data_store.file')
            with open(file_name,'rb') as f:
              objs.append(pickle.load(f))
        
        plt.figure()
        for obj in objs:
            plt.plot(obj.stats_time, obj.num_cores,'-')
        plt.xlabel('time in seconds')
        plt.ylabel('Cores Used')
        #plt.legend(['d300_md1800_sm1', 'd300_md1800_sm2', 'd300_md3600_sm1', 'd300_md3600_sm2'])
        #plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v2/comparing_core_schemes.png',dpi=1000)
        #plt.legend(['d1200_ds2400', 'd600_ds1200', 'd1800_ds3600'])
        #plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v3/comparing_core_schemes.png',dpi=1000)
        plt.legend(['d300_md3600_m0.5', 'd300_md3600_m0.6', 'd300_md3600_m0.7', 'd300_md3600_m0.8', 'd300_md3600_m0.9'])
        plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v4/comparing_core_schemes.png',dpi=1000)
        plt.close()
        
        plt.figure()
        for obj in objs:
            plt.plot(obj.stats_time, obj.effective_cores_lst,'-')
        plt.xlabel('time in seconds')
        plt.ylabel('Effective Cores Used')
        #plt.legend(['d300_md1800_sm1', 'd300_md1800_sm2', 'd300_md3600_sm1', 'd300_md3600_sm2'])
        #plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v2/comparing_eff_core_schemes.png',dpi=1000)
        #plt.legend(['d1200_ds2400', 'd600_ds1200', 'd1800_ds3600'])
        #plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v3/comparing_eff_core_schemes.png',dpi=1000)
        plt.legend(['d300_md3600_m0.5', 'd300_md3600_m0.6', 'd300_md3600_m0.7', 'd300_md3600_m0.8', 'd300_md3600_m0.9'])
        plt.savefig('/mnt/azure_data/rakshith/scheduler/Graphs/round_robin_delay_v4/comparing_eff_core_schemes.png',dpi=1000)
        plt.close()
        


# <creaated/destroyed,vm_id,time_stamp,num_cores,ram>
if __name__ == "__main__":
    obj = Executor()
    obj.execute()
    #obj.common_visualize()
    # obj.compare_diff_schemes()
