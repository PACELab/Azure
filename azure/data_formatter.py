import csv
import json
from collections import defaultdict

import numpy as np
import pandas as pd

headers = ['old_vmid', 'subscriptionid', 'deploymentid', 'vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu',
           'vmcategory', 'vmcorecount', 'vmmemory']
data_path = '../vm_cpu_readings/vmtable.csv'


def generate_vm_file():
    trace_data = pd.read_csv(data_path, header=None, index_col=False, names=headers, delimiter=',')
    old_vm_id = trace_data["subscriptionid"]
    old_vm_id = pd.DataFrame(old_vm_id.unique())
    new_vm_id = pd.DataFrame(np.arange(len(old_vm_id.index)))
    final_df = pd.concat([old_vm_id, new_vm_id], axis=1)
    final_df.columns = ["old_vm_id", "new_vm_id"]
    final_df.to_csv("new_vm_mapping.csv", sep=',', columns=["old_vm_id", "new_vm_id"])


def generate_sub_file():
    trace_data = pd.read_csv(data_path, header=None, index_col=False, names=headers, delimiter=',')
    old_sub_id = trace_data["subscriptionid"]
    old_sub_id = pd.DataFrame(old_sub_id.unique())
    new_sub_id = pd.DataFrame(np.arange(len(old_sub_id.index)))
    final_df = pd.concat([old_sub_id, new_sub_id], axis=1)
    final_df.columns = ["old_sub_id", "new_sub_id"]
    final_df.to_csv("new_sub.csv", sep=',', columns=["old_sub_id", "new_sub_id"])


def generate_dep_file():
    trace_data = pd.read_csv(data_path, header=None, index_col=False, names=headers, delimiter=',')
    old_dep_id = trace_data["deploymentid"]
    old_dep_id = pd.DataFrame(old_dep_id.unique())
    new_dep_id = pd.DataFrame(np.arange(len(old_dep_id.index)))
    final_df = pd.concat([old_dep_id, new_dep_id], axis=1)
    final_df.columns = ["old_dep_id", "new_dep_id"]
    final_df.to_csv("new_dep.csv", sep=',', columns=["old_dep_id", "new_dep_id"])


# generate_vm_file()
# generate_sub_file()
# generate_dep_file()

o_vm_list = []
new_vm_list = []
convertor_json = {}

o_sub_list = []
new_sub_list = []

o_dep_list = []
new_dep_list = []


def new_mapped_csv():
    new_mapper()
    #vm id mapper
    with open('vm_mapper.csv', 'wb') as vm_map:
        csv_out = csv.writer(vm_map)
        csv_out.writerow(['old_vm_id', 'new_vm_id'])
        for old, new in zip(o_vm_list, new_vm_list):
            csv_out.writerow((old, new))

    #subscripton mapper
    with open('sub_mapper.csv', 'wb') as sub_map:
        csv_out = csv.writer(sub_map)
        csv_out.writerow(['old_sub_id', 'new_sub_id'])
        for old, new in zip(o_sub_list, new_sub_list):
            csv_out.writerow((old, new))


    #deployment mapper
    with open('dep_mapper.csv', 'wb') as dep_map:
        csv_out = csv.writer(dep_map)
        csv_out.writerow(['old_dep_id', 'new_dep_id'])
        for old, new in zip(o_dep_list, new_dep_list):
            csv_out.writerow((old, new))

    with open("mapper.json", "w") as f:
        f.write(json.dumps(convertor_json))


def new_mapper():
    with open(data_path, 'r') as df:
        rows = csv.reader(df, delimiter=',')
        for row in rows:
            o_vm, o_sub, o_dep = row[0], row[1], row[2]
            create_entry(o_vm, o_sub, o_dep)


def create_entry(o_vm, o_sub, o_dep):
    new_name = ""
    # level 1
    if o_sub not in convertor_json:
        if "cnt" in convertor_json:
            convertor_json["cnt"] += 1
        else:
            convertor_json["cnt"] = 1
        convertor_json[o_sub] = {}
        convertor_json[o_sub]["name"] = "S" + str(convertor_json["cnt"])
        o_sub_list.append(o_sub)
        new_sub_list.append(convertor_json[o_sub]["name"])
    new_name += convertor_json[o_sub]["name"]

    # level 2
    sub_json = convertor_json[o_sub]
    if o_dep not in sub_json:
        if "cnt" in sub_json:
            sub_json["cnt"] += 1
        else:
            sub_json["cnt"] = 1
        sub_json[o_dep] = {}
        sub_json[o_dep]["name"] = "D" + str(sub_json["cnt"])
        o_dep_list.append(o_dep)
        new_dep_list.append(new_name+"_"+sub_json[o_dep]["name"])
    new_name += "_" + sub_json[o_dep]["name"]

    # level 3
    dep_json = sub_json[o_dep]
    if o_vm not in dep_json:
        if "cnt" in dep_json:
            dep_json["cnt"] += 1
        else:
            dep_json["cnt"] = 1
        dep_json[o_vm] = {}
        dep_json[o_vm]["name"] = "V" + str(dep_json["cnt"])
    new_name += "_" + dep_json[o_vm]["name"]
    o_vm_list.append(o_vm)
    new_vm_list.append(new_name)


new_mapped_csv()
