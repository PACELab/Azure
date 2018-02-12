import pandas as pd

#headers=['vmid','subscriptionid','deploymentid','vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu', 'vmcategory', 'vmcorecount', 'vmmemory']
#data_path = 'vmtable.csv'
#trace_data = pd.read_csv(data_path, header=None, index_col=False,names=headers,delimiter=',')

cpu_data_headers=['number','vmid','minCPU1','maxCPU1','avgCPU1']

cpu_data_path='vm_cpu_readings-file-{}-of-125.csv'

merged_array=[]

number_of_VM_files_to_be_created = 5
number_of_VM_CPU_reading_Files_to_be_used = 10

i=1
while(i<=number_of_VM_CPU_reading_Files_to_be_used):

        cpu_data = pd.read_csv(cpu_data_path.format(i),header=None, index_col=False,names=cpu_data_headers,delimiter=',')

        #merged_data = pd.merge(cpu_data,trace_data,how='inner',left_on='vmid',right_on='vmid')

        merged_array.append(cpu_data)
        i+=1

concatenated_data = pd.concat(merged_array)


#for key in concatenated_data['vmid'].unique():
#       concatenated_data[concatenated_data['vmid']==key].to_csv('zzz{}.csv'.format(key))

count=0
for group,frame in concatenated_data.groupby('vmid'):
        count+=1
        if count==number_of_VM_files_to_be_created:
                frame.to_csv('zz{}.txt'.format(group[:10]).replace('/',''))
                break

print "number of VM's:",count
