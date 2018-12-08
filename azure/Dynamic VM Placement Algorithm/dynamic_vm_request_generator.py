import pandas as pd
import numpy as np

data_path = '../cpu_readings_new/vmtable.csv'
headers=['vmid','subscriptionid','deploymentid','vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu', 'vmcategory', 'vmcorecount', 'vmmemory']
trace_dataframe = pd.read_csv(data_path, header=None, index_col=False,names=headers,delimiter=',')
trace_dataframe = trace_dataframe[(trace_dataframe['vmdeleted']-trace_dataframe['vmcreated'])>86400]

creation_dataframe = trace_dataframe[['vmid','vmcreated','vmcorecount','vmmemory']]
creation_dataframe['activity'] = 'c'

deletion_dataframe = trace_dataframe[['vmid','vmdeleted','vmcorecount','vmmemory']]
deletion_dataframe['activity'] = 'd'

creation_dataframe.columns = ['vmid','timestamp','vmcorecount','vmmemory','activity']
deletion_dataframe.columns = ['vmid','timestamp','vmcorecount','vmmemory','activity']

total_dataframe = pd.concat([creation_dataframe, deletion_dataframe],ignore_index=True)
total_dataframe.columns=['vmid','timestamp','vmcorecount','vmmemory','activity']
total_dataframe = total_dataframe.sort_values(by=['timestamp','activity'],ascending=[1,0])

total_dataframe.to_csv('vm_request_simulation_data.csv',index=False,header=False)

