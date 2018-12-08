from collections import OrderedDict
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig

def main():
    flag = 0
    #file_path = "/mnt/google_data/google/task_usage/filtered_intervals_usage.csv"
    file_path = "/mnt/google_data/google/task_usage/part-000{:02d}-of-00500.csv"
    effective_cores_dict = OrderedDict()
    index = 10
    for i in range(index):
        print file_path.format(i)
        f = open(file_path.format(i), "r")
        line = f.readline()
        while line:
            line = line.split(',')
            time = int(line[0])
            time/=1000000
            if time%300 !=0:
                time = (time/300)*300
            if time in effective_cores_dict:
                effective_cores_dict[time]+=float(line[5])
            else:
                effective_cores_dict[time]=float(line[5])
            line = f.readline()
    plt.figure()
    plt.plot(effective_cores_dict.keys(), effective_cores_dict.values())
    plt.ylabel('Effective Cores')
    plt.xlabel('time')
    savefig('effective_cores_used{}.png'.format(index),dpi=1000)
    plt.close()
    
main()
    
    
        