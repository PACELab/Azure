import json
from collections import OrderedDict

config = None
config_file = "config_16"
config_path = "config/{cf}.json".format(cf=config_file)
if not config:
    with open(config_path, "r") as conf:
        config = json.load(conf, object_pairs_hook=OrderedDict)
