import sys
import os.path

from scheduler.data_vizualiser import DataVisualizer

parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
sys.path.append(parent_dir)
from scheduler.config import *
import importlib

from scheduler.feeder import Feeder

path = "Algorithms." + algorithm
algo = importlib.import_module(path)


class Executor(object):
    def __init__(self):
        self.algo_name = algorithm

    def execute(self):
        algo_obj = algo.Algorithm()
        feeder_gen = Feeder().execute()
        for tup in feeder_gen:
            algo_obj.execute(tup)
        data_obj = DataVisualizer(algo_obj)
        data_obj.visualize()


# <creaated/destroyed,vm_id,time_stamp,num_cores,ram>
if __name__ == "__main__":
    obj = Executor()
    obj.execute()
