from scheduler.config_loader import *
import pickle

fp = 'Graphs/{algo}/{cf}/data_store.file'.format(algo=config["algorithm"], cf=config_file)


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
