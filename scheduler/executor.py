import sys
import os.path
import os
import json
from collections import OrderedDict, defaultdict

parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
sys.path.append(parent_dir)
from scheduler.data_vizualiser import DataVisualizer
from scheduler.config_loader import *
from scheduler.store import Store
import importlib

from scheduler.feeder import Feeder
from scheduler.config_loader import *




class Executor(object):
    def __init__(self, mode="execute",feeder_path = None):
        self.algo_name = config["algorithm"]
        self.mode = mode


    def execute(self):
        path = "Algorithms." + config["algorithm"]
        algo = importlib.import_module(path)
        algo_obj = algo.Algorithm()
        feeder_gen = Feeder().execute()
        prev_time_stamp, prev_action, pres_cores_create, pres_ram_create, pres_cores_delete, pres_ram_delete = 0, 'd', 0, 0, 0, 0
        for tup in feeder_gen:
            algo_obj.execute(tup)
        algo_obj.final()
        # algo_obj.remove_duplicate_times()

        data_obj = DataVisualizer(algo_obj)
        print "count is", algo_obj.c
        data_obj.visualize()
        Store(algo_obj)

    def common_visualize(self):
        root_dir = "/mnt/azure_data/Azure/scheduler/Graphs"
        f_d = defaultdict(list)
        for dir_name, sub_dir_list, file_list in os.walk(root_dir):
            for fname in file_list:
                if fname == "data_store.file":
                    l = dir_name.split("/")
                    path = dir_name + "/data_store.file"
                    f_d[l[-1]].append((l[-2], Store().load_from_file(path)))
        DataVisualizer().common_visualiser(f_d)


# <creaated/destroyed,vm_id,time_stamp,num_cores,ram>
if __name__ == "__main__":
    obj = Executor()
    # obj.execute()
    obj.common_visualize()
