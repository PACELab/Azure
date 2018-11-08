import matplotlib
import numpy as np
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig
import numpy as np
from collections import *

def get_relaunch_vm_count():
    vm_dict = defaultdict(int)
    delete_dict = defaultdict(int)
    delete_sum = 0
    delete_count = 0
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if int(lst[5]) == 1:
                    vm_dict[vm_id]+=1
                    if delete_dict[vm_id]!=0:
                        delete_sum+=timestamp-delete_dict[vm_id]
                        delete_count+=1
                elif int(lst[5]) == 8:
                    pass
                else:
                    if vm_id in vm_dict:
                        delete_dict[vm_id]=timestamp
                    pass
    
    relaunch_dict = defaultdict(int)
    for item in vm_dict:
        count = vm_dict[item]
        if count>1:
            relaunch_dict[count]+=1
    relaunch_dict = OrderedDict(sorted(relaunch_dict.iteritems(), key=lambda t: t[0]))
    print delete_sum, delete_count
    print "Average Inter Launch Duration of a VM: ", delete_sum/float(delete_count), "seconds"
    values = relaunch_dict.values()
    cdf = np.cumsum(np.array(values), dtype=np.float64)
    keys = relaunch_dict.keys()
    print "25th Percentile of number of VM Relaunches: ", np.percentile(cdf, 25)
    print "50th Percentile of number of VM Relaunches: ", np.percentile(cdf, 50)
    print "75th Percentile of number of VM Relaunches: ", np.percentile(cdf, 75)
    print "90th Percentile of number of VM Relaunches: ", np.percentile(cdf, 90)
    print "95th Percentile of number of VM Relaunches: ", np.percentile(cdf, 95)
    print "99th Percentile of number of VM Relaunches: ", np.percentile(cdf, 99)
    
    print "Total Number of Relaunched VMs", cdf[-1]
    print "Number of VMs relaunched max number of times:", values[-1]
    print "Max Times a VM Relaunched: ", keys[-1]
    
    plt.figure()
    plt.plot(keys, (cdf/float(cdf[-1]))*100)
    plt.ylabel('CDF (in percentage)')
    plt.xlabel('Number of Times a VM relaunched')
    savefig('times_relaunched.png',dpi=1000)
    plt.xlim(0,750)
    savefig('times_relaunched_2.png',dpi=1000)
    plt.close()
    
get_relaunch_vm_count()

print "\n\n\n"

def get_relaunch_vm_count_without_1min():
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
                elif int(lst[5]) == 8:
                    pass
                else:
                    if vm_id in vm_dict:
                        lifetime = timestamp - vm_dict[vm_id][0]
                        if lifetime<=60:
                            dummys_1.add((vm_id,vm_dict[vm_id][0]))
                        if lifetime<=300:
                            dummys_5.add((vm_id,vm_dict[vm_id][0]))
                        vm_dict.pop(vm_id)
                        
    
    vm_dict = defaultdict(int)
    delete_dict = defaultdict(int)
    delete_sum = 0
    delete_count = 0
    pass_dict = set()
    with open('/mnt/google_data/scheduler/google_events_data.csv','r+') as f:
        for l in f:
            lst = l.split(",")
            if (float(lst[3])>0 and float(lst[4])>0) or int(lst[5])==8:
                vm_id = lst[1]+'_'+lst[0]
                timestamp = float(lst[2])/(1000000)
                if int(lst[5]) == 1:
                    if (vm_id,timestamp) in dummys_5:
                        pass_dict.add(vm_id)
                        continue
                    vm_dict[vm_id]+=1
                    if delete_dict[vm_id]!=0:
                        delete_sum+=timestamp-delete_dict[vm_id]
                        delete_count+=1
                elif int(lst[5]) == 8:
                    pass
                else:
                    if vm_id in pass_dict:
                        pass_dict.remove(vm_id)
                        continue
                    if vm_id in vm_dict:
                        delete_dict[vm_id]=timestamp
                    pass
    
    relaunch_dict = defaultdict(int)
    for item in vm_dict:
        count = vm_dict[item]
        if count>1:
            relaunch_dict[count]+=1
    relaunch_dict = OrderedDict(sorted(relaunch_dict.iteritems(), key=lambda t: t[0]))
    print delete_sum, delete_count
    print "Average Inter Launch Duration of a VM: ", delete_sum/float(delete_count), "seconds"
    values = relaunch_dict.values()
    cdf = np.cumsum(np.array(values), dtype=np.float64)
    keys = relaunch_dict.keys()
    print "25th Percentile of number of VM Relaunches: ", np.percentile(cdf, 25)
    print "50th Percentile of number of VM Relaunches: ", np.percentile(cdf, 50)
    print "75th Percentile of number of VM Relaunches: ", np.percentile(cdf, 75)
    print "90th Percentile of number of VM Relaunches: ", np.percentile(cdf, 90)
    print "95th Percentile of number of VM Relaunches: ", np.percentile(cdf, 95)
    print "99th Percentile of number of VM Relaunches: ", np.percentile(cdf, 99)
    
    print "Total Number of Relaunched VMs", cdf[-1]
    print "Number of VMs relaunched max number of times:", values[-1]
    print "Max Times a VM Relaunched: ", keys[-1]
    
    plt.figure()
    plt.plot(keys, (cdf/float(cdf[-1]))*100)
    plt.ylabel('CDF (in percentage)')
    plt.xlabel('Number of Times a VM relaunched')
    savefig('times_relaunched_ignoring.png',dpi=1000)
    plt.xlim(0,750)
    savefig('times_relaunched_ignoring_2.png',dpi=1000)
    plt.close()
    
get_relaunch_vm_count_without_1min()
                        
    
    

        
