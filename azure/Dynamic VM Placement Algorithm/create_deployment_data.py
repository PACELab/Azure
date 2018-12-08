import pandas as pd
import numpy as np
import multiprocessing
from threading import Thread
from Queue import Queue
import os
import sys
from collections import defaultdict

##############################

fds = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : None)))

headers=['vmid','subscriptionid','deploymentid','vmcreated', 'vmdeleted', 'maxcpu', 'avgcpu', 'p95maxcpu', 'vmcategory', 'vmcorecount', 'vmmemory']
data_path = '../cpu_readings_new/vmtable.csv'
trace_data = pd.read_csv(data_path, header=None, index_col=False,names=headers,delimiter=',')
#print trace_data.head(5)
trace_data = trace_data[(trace_data['vmdeleted']-trace_data['vmcreated'])>86400]
trace_data['presence'] = 1
trace_data = trace_data.set_index(['vmid'])
trace_data = trace_data[['presence','vmmemory']]
print len(trace_data),"*****"
#print trace_data.head(5)
trace_dict = trace_data.to_dict('index')
#print trace_dict['S1_D1_V1']['presence'],"******"
   #pass
#exit(0)

#############################

def worker_process(range_tuple):
    
    start_num, end_num = range_tuple[0],range_tuple[1]
    
    thread_q = Queue()
    
    for x in range(25):
       worker = NameChangeWorker(thread_q)
       worker.daemon=True
       worker.start()
      
    end_num = min(end_num,35941)
    for i in range(start_num,end_num):
        thread_q.put(total_unique_deployment_ids[i])

    thread_q.join()
    
    
class NameChangeWorker(Thread):
  
   def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue

   def run(self):
       global cpu_data
       while True:
           # Get the work from the queue and expand the tuple
           
           cpu_data_path='../cpu_readings_new/vm_cpu_readings-file-{}-of-125.csv'
           
           deployment_name = self.queue.get()
           
           print "Starting:", deployment_name, "..."
           deployment_lookup_string = str(deployment_name)+"_"
           
           sub_name,dep_name = deployment_name.split('_')
           
           try:
             os.makedirs(sub_name,0777)
           except:
             pass
           g = open(os.path.join(sub_name,deployment_name)+'.csv','a+')
           for i in range(1,126):
               f = open(cpu_data_path.format(i),'r')
               line = f.readline()
               while line:
                 if deployment_lookup_string in line:
                   g.write(line)
                 line = f.readline()
               f.close()
           g.close()
               
           print "Done:", deployment_name, "..."
           self.queue.task_done()
           
def new_func():
    cpu_data_path='../cpu_readings_new/vm_cpu_readings-file-{}-of-125.csv'
    
    for i in range(1,126):
       print "Starting:", i, "..."
       f = open(cpu_data_path.format(i),'r')
       lines = f.readlines()
       for line in lines:
         lst = line.split(',')
         subs,dep,vm = lst[1].split('_')
         try:
             if trace_dict[lst[1]]['presence']!=1:
                 pass
         except Exception as e:
             #print vm,"Skipped.."
             #print e
             pass
         #print line
         #print fds[subs]
         #print type(fds[subs][dep])
         if fds[subs][dep][vm]==None:
             try:
                os.makedirs(subs+'/'+(subs+'_'+dep),0777)
             except Exception as e:
                #print e
                pass
             fds[subs][dep][vm]=open(os.path.join(subs+'/'+(subs+'_'+dep),lst[1])+'.csv','a+')
         fds[subs][dep][vm].write(line)
         #line = f.readline()
       f.close()
       print "Done:", i, "..."
       sys.stdout.flush()
           
def main():
    
    #temp = [100*i for i in range(0,360)]
    #input = [(i,i+100) for i in temp]
    #pool = multiprocessing.Pool(processes = 12)
    #pool.map_async(worker_process,input)
    #worker_process(1,vm_mapper_data)
    #pool.close()
    #pool.join()
    new_func()
    
main()
           
           
           
           
           
           
