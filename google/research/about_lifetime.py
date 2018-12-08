import matplotlib
import numpy as np
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig

def get_min_lifetime_count():
    min_1count = 0
    min_5count = 0
    total_created_count = 0
    total_deleted_count = 0
    dummy_update_count = 0
    migrate_count = 0
    max_cores_used = 0
    vm_dict = {}
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if int(lst[5]) == 1:
                    vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                    total_created_count+=1
                elif int(lst[5]) == 8:
                    migrate_count+=1
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            min_1count+=1
                        if lifetime<=300:
                            min_5count+=1
                        total_deleted_count+=1
                        vm_dict.pop(vm_id)
                    if (float(lst[3])>0 and float(lst[4])>0):
                        vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                        total_created_count+=1
                    else:
                        dummy_update_count+=1
                else:
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            min_1count+=1
                        if lifetime<=300:
                            min_5count+=1
                        total_deleted_count+=1
                        vm_dict.pop(vm_id)
    print "1 min Vms: ",min_1count
    print "5 min Vms: ", min_5count
    print "Total Vms Created Count:", total_created_count
    print "Total Vms Deleted:", total_deleted_count
    print "Percentage of 1min Vms: ", float(min_1count)/float(total_created_count)
    print "Percentage of 5min Vms: ", float(min_5count)/float(total_created_count)
    print "Migrate Requests: ", migrate_count
    print "Dummy Update Count", dummy_update_count
    
    not_freed_ram=0
    for vm_id in vm_dict:
        not_freed_ram+=vm_dict[vm_id][1]
    
    print "Not Freed Cores by end of trace period: ", not_freed_ram
    line_count=0
    not_freed_cores = 0
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            line_count+=1
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) and int(lst[5])==1:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if vm_id in vm_dict:
                    vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                    not_freed_cores+=float(lst[3])
            elif int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if vm_id in vm_dict:
                    not_freed_cores += (float(lst[3])-vm_dict[vm_id][1])
                    vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
            else:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if vm_id in vm_dict:
                    not_freed_cores -= float(lst[3])
            if line_count%10000000==0:
                print "Not Freed Cores at", line_count,": ", not_freed_cores
    
    print "Not Freed Cores by end of trace period: ", not_freed_cores
    
    
#get_min_lifetime_count()

def get_max_cores():
    min_1count = 0
    min_5count = 0
    total_created_count = 0
    total_deleted_count = 0
    dummy_update_count = 0
    migrate_count = 0
    cores_used = 0
    max_cores_used = 0
    vm_dict = {}
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if int(lst[5]) == 1:
                    vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                    total_created_count+=1
                    cores_used = cores_used+float(lst[3])
                elif int(lst[5]) == 8:
                    migrate_count+=1
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            min_1count+=1
                        if lifetime<=300:
                            min_5count+=1
                        total_deleted_count+=1
                        cores_used = cores_used-vm_dict[vm_id][1]
                        vm_dict.pop(vm_id)
                    if (float(lst[3])>0 and float(lst[4])>0):
                        vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                        total_created_count+=1
                        cores_used = cores_used+float(lst[3])
                    else:
                        dummy_update_count+=1
                else:
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            min_1count+=1
                        if lifetime<=300:
                            min_5count+=1
                        total_deleted_count+=1
                        cores_used = cores_used-vm_dict[vm_id][1]
                        vm_dict.pop(vm_id)
                max_cores_used = max(cores_used,max_cores_used)
                
    print "1 min Vms: ",min_1count
    print "5 min Vms: ", min_5count
    print "Total Vms Created Count:", total_created_count
    print "Total Vms Deleted:", total_deleted_count
    print "Percentage of 1min Vms: ", float(min_1count)/float(total_created_count)
    print "Percentage of 5min Vms: ", float(min_5count)/float(total_created_count)
    print "Migrate Requests: ", migrate_count
    print "Dummy Update Count", dummy_update_count
    print "Global Max Cores Used: ", max_cores_used
                
def get_variance_removing_dummys():
    min_1count = 0
    min_5count = 0
    total_created_count = 0
    total_deleted_count = 0
    dummy_update_count = 0
    migrate_count = 0
    cores_used = 0
    max_cores_used = 0
    vm_dict = {}
    dummys_1 = set()
    dummys_5 = set()
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if int(lst[5]) == 1:
                    vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                    cores_used = cores_used+float(lst[3])
                elif int(lst[5]) == 8:
                    migrate_count+=1
                    if vm_id in vm_dict:
                        cores_used = cores_used-vm_dict[vm_id][1]
                    if (float(lst[3])>0 and float(lst[4])>0):
                        vm_dict[vm_id]=(vm_dict[vm_id][0],float(lst[3]),float(lst[4]))
                        cores_used = cores_used+float(lst[3])
                    else:
                        dummy_update_count+=1
                else:
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            min_1count+=1
                            dummys_1.add((vm_id,vm_dict[vm_id][0]))
                        if lifetime<=300:
                            min_5count+=1
                            dummys_5.add((vm_id,vm_dict[vm_id][0]))
                        cores_used = cores_used-vm_dict[vm_id][1]
                        vm_dict.pop(vm_id)
                max_cores_used = max(cores_used,max_cores_used)
    
    max_cores_used = max(cores_used,max_cores_used)
    
    flag=True
    for dummy_set in [dummys_1,dummys_5]:
        stats_time = []
        cores = []
        ignore_vms = set()
        cores_used=0
        with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
            for l in f:
                lst = l.split(",")
                if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                    vm_id = lst[1]+'_'+lst[0]
                    if (vm_id,float(lst[2])) in dummy_set:
                        ignore_vms.add(vm_id)
                        continue
                    timestamp = float(lst[2])/(1000000)
                    if int(lst[5]) == 1:
                        vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                        cores_used = cores_used+float(lst[3])
                    elif int(lst[5]) == 8:
                        if vm_id in ignore_vms:
                            continue
                        if vm_id in vm_dict:
                            cores_used = cores_used-vm_dict[vm_id][1]
                            vm_dict.pop(vm_id)
                        if (float(lst[3])>0 and float(lst[4])>0):
                            vm_dict[vm_id]=(timestamp,float(lst[3]),float(lst[4]))
                            cores_used = cores_used+float(lst[3])
                        else:
                            dummy_update_count+=1
                    else:
                        if vm_id in ignore_vms:
                            ignore_vms.remove(vm_id)
                            continue
                        if vm_id in vm_dict:
                            cores_used = cores_used-vm_dict[vm_id][1]
                            vm_dict.pop(vm_id)
                    
                    if not stats_time:
                        stats_time.append(float(lst[2]))
                        cores.append(float(lst[3]))
                    else:
                        if stats_time[-1]==float(lst[2]):
                            cores[-1]=cores_used
                        else:
                            stats_time.append(float(lst[2]))
                            cores.append(float(lst[3]))
                    
        plt.figure()
        plt.plot(stats_time,cores)
        plt.xlabel('Time in micro seconds')
        plt.ylabel('Cores Used')
        if flag:
            savefig('ignornig_1min.png',dpi=1000)
            flag=False
        else:
            savefig('ignornig_5min.png',dpi=1000)
        plt.close()
                
get_variance_removing_dummys()