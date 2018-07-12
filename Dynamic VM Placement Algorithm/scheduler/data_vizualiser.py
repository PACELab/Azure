import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig
from scheduler.config_loader import *
import os


class DataVisualizer(object):
    def __init__(self, obj=None):
        self.data_obj = obj

    def visualize(self):
        print "max cores ", self.data_obj.max_cores
        print "max ram ", self.data_obj.max_ram
        final_path = 'Graphs/{algo}/{cf}'.format(algo=config["algorithm"], cf=config_file)
        if not os.path.isdir(final_path):
            os.makedirs(final_path)
        plt.figure(1)
        plt.plot(self.data_obj.num_cores_time, self.data_obj.num_cores)
        plt.ylabel('total num cores used')
        savefig('{fp}/cores_used.jpeg'.format(fp=final_path))
        plt.figure(2)
        plt.plot(self.data_obj.amount_ram_time, self.data_obj.amount_ram)
        plt.ylabel('total amount of ram used')
        savefig('{fp}/ram_used.jpeg'.format(fp=final_path))
        plt.figure(3)
        plt.plot(self.data_obj.avg_ram_usage_time, self.data_obj.avg_ram_usage_lst)
        plt.ylabel('avg ram utilization')
        savefig('{fp}/avg_ram_utilization.jpeg'.format(fp=final_path))
        plt.figure(4)
        plt.plot(self.data_obj.avg_cpu_usage_time, self.data_obj.avg_cpu_usage_lst)
        plt.ylabel('avg core utilization')
        savefig('{fp}/avg_core_utilization.jpeg'.format(fp=final_path))
        plt.figure(5)
        plt.plot(self.data_obj.used_servers_time, self.data_obj.used_servers_lst)
        plt.ylabel('Total Servers Used')
        savefig('{fp}/servers_used.jpeg'.format(fp=final_path))
        plt.figure(6)
        plt.plot(self.data_obj.cores_creation_time, self.data_obj.cores_creation_lst)
        plt.ylabel('Cores Requested')
        savefig('{fp}/cores_requested.jpeg'.format(fp=final_path))
        plt.figure(7)
        plt.plot(self.data_obj.ram_creation_time, self.data_obj.ram_creation_lst)
        plt.ylabel('RAM Requested')
        savefig('{fp}/ram_requested.jpeg'.format(fp=final_path))
        plt.figure(8)
        plt.plot(self.data_obj.cores_deletion_time, self.data_obj.cores_deletion_lst)
        plt.ylabel('Cores Deleted')
        savefig('{fp}/cores_deletion.jpeg'.format(fp=final_path))
        plt.figure(9)
        plt.plot(self.data_obj.ram_deletion_time, self.data_obj.ram_deletion_lst)
        plt.ylabel('RAM Deleted')
        savefig('{fp}/ram_deletion.jpeg'.format(fp=final_path))

        del self.data_obj.cores_creation_time[0]
        del self.data_obj.cores_creation_lst[0]
        del self.data_obj.ram_creation_time[0]
        del self.data_obj.ram_creation_lst[0]
        plt.figure(10)
        plt.plot(self.data_obj.cores_creation_time, self.data_obj.cores_creation_lst)
        plt.ylabel('Cores Requested')
        savefig('{fp}/cores_requested_new.jpeg'.format(fp=final_path))
        plt.figure(11)
        plt.plot(self.data_obj.ram_creation_time, self.data_obj.ram_creation_lst)
        plt.ylabel('RAM Requested')
        savefig('{fp}/ram_requested_new.jpeg'.format(fp=final_path))

        del self.data_obj.cores_deletion_time[-1]
        del self.data_obj.cores_deletion_lst[-1]
        del self.data_obj.ram_deletion_time[-1]
        del self.data_obj.ram_deletion_lst[-1]
        plt.figure(12)
        plt.plot(self.data_obj.cores_deletion_time, self.data_obj.cores_deletion_lst)
        plt.ylabel('Cores Deleted')
        savefig('{fp}/cores_deletion_new.jpeg'.format(fp=final_path))
        plt.figure(13)
        plt.plot(self.data_obj.ram_deletion_time, self.data_obj.ram_deletion_lst)
        plt.ylabel('RAM Deleted')
        savefig('{fp}/ram_deletion_new.jpeg'.format(fp=final_path))
        plt.figure(14)
        self.data_obj.cores_histogram = {k: v for k, v in self.data_obj.cores_histogram.iteritems() if v !=0}
        plt.bar(self.data_obj.cores_histogram.keys(), self.data_obj.cores_histogram.values(), width=1.0,color='g')
        plt.ylabel('cores histogram')
        savefig('{fp}/cores_histogram.jpeg'.format(fp=final_path))
        plt.figure(15)
        self.data_obj.ram_histogram = {k: v for k, v in self.data_obj.ram_histogram.iteritems() if v != 0}
        plt.bar(self.data_obj.ram_histogram.keys(), self.data_obj.ram_histogram.values(),width=1.0, color='g')
        plt.ylabel('rams histogram')
        savefig('{fp}/rams_histogram.jpeg'.format(fp=final_path))
        ##HISTOGRRAM FOR CORES AND RAM PENDING

    def check_path(self, final_path):
        if not os.path.isdir(final_path):
            os.makedirs(final_path)

    def common_visualiser(self, f_d):
        base_path = '/mnt/azure_data/AZURE/scheduler/Graphs/common'
        c = 0
        print  "fd is",f_d
        for key, value in f_d.iteritems():
            plt.figure(c)
            plt.ylabel('Total Servers Used')
            p = os.path.join(base_path, key)
            self.check_path(p)
            for v in value:
                algo_name,algo_obj = v
                plt.plot(algo_obj.used_servers_time, algo_obj.used_servers_lst)
            c+=1
            savefig('{fp}/servers_used.jpeg'.format(fp=p))
