import os
import sys
import json
from collections import OrderedDict
from scheduler import exception

print "Intializing Config Loader.."
config = None
config_file = "config_16"
dir_path = os.path.dirname(__file__)
config_path = "{d_p}/config/{cf}.json".format(d_p=dir_path, cf=config_file)
print config_path
if not config:
    with open(config_path, "r") as conf:
        config = json.load(conf, object_pairs_hook=OrderedDict)
        


def get_parent_path():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
