from scheduler.config_loader import *
import pickle
import os

if config["algorithm"]=='round_robin_delay_v2':
    fp = 'scheduler/Graphs/{algo}/{cf}_d{delay}_md{max_delay}_sm{stdmultiplier}/data_store.file'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], stdmultiplier=config['stdMultiplier'])
elif config["algorithm"]=='round_robin_delay_v3':
    fp = 'scheduler/Graphs/{algo}/{cf}_d{delay}_ds{delay_step}/data_store.file'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'],delay_step=config['delay_step'])
elif config["algorithm"]=='round_robin_delay':
    fp = 'scheduler/Graphs/{algo}/{cf}_d{delay}_md{max_delay}_cl{cores_limit}/data_store.file'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], cores_limit=config['cores_hard_limit'])
elif config["algorithm"]=='round_robin_delay_v1':
    fp = 'scheduler/Graphs/{algo}/{cf}_d{delay}_sm{stdmultiplier}/data_store.file'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], stdmultiplier=config['stdMultiplier'])
elif config["algorithm"]=='round_robin_delay_v4':
    fp = 'scheduler/Graphs/{algo}/{cf}_d{delay}_md{max_delay}_m{multiplier}/data_store.file'.format(algo=config["algorithm"], cf=config_file, delay=config['delay_time'], max_delay=config['max_delay_time'], multiplier=config['multiplier'])
else:
    fp = 'scheduler/Graphs/{algo}/{cf}/data_store.file'.format(algo=config["algorithm"], cf=config_file)
    
fp  = os.path.join(get_parent_path(),fp)

if not os.path.isdir(os.path.dirname(fp)):
    os.makedirs(os.path.dirname(fp))


class Store(object):

    def __init__(self, obj=None):
        if obj:
            self.obj = obj
            self.store_to_file()

    def store_to_file(self):
        with open(fp, "wb") as f:
            pickle.dump(self.obj, f, pickle.HIGHEST_PROTOCOL)

    def load_from_file(self, fp):
        with open(fp, "rb") as f:
            return pickle.load(f)
            
class Store1(object):
    
    def __init__(self, obj=None):
        if obj:
            self.lst = [obj.stats_time, obj.num_cores]  
            self.store_to_file()
            
    def store_to_file(self):
        with open(fp+'1', "wb") as f:
            pickle.dump(self.lst, f, pickle.HIGHEST_PROTOCOL)
            
    def load_from_file(self, fp):
        with open(fp+'1', "rb") as f:
            return pickle.load(f)
            
class Store2(object):
    
    def __init__(self, obj=None):
        if obj:
            self.lst = [ obj.vm_delay_histogram]  
            self.store_to_file()
            
    def store_to_file(self):
        with open(fp+'2', "wb") as f:
            pickle.dump(self.lst, f, pickle.HIGHEST_PROTOCOL)
            
    def load_from_file(self, fp):
        with open(fp+'2', "rb") as f:
            return pickle.load(f)
