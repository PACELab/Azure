import matplotlib.pyplot as plt
from matplotlib.pyplot import savefig


class DataVisualizer(object):
    def __init__(self, obj):
        self.data_obj = obj

    def visualize(self):
        plt.figure(1)
        plt.plot(self.data_obj.num_cores)
        plt.ylabel('num cores used')
        savefig('Graphs/cores_used.jpeg')
        plt.figure(2)
        plt.plot(self.data_obj.amount_ram)
        plt.ylabel('amount of ram used')
        savefig('Graphs/ram_used.jpeg')
        plt.figure(3)
        plt.plot(self.data_obj.avg_ram_usage_lst)
        plt.ylabel('avg cpu utilization')
        savefig('Graphs/avg_cpu_utilization.jpeg')
        plt.figure(4)
        plt.plot(self.data_obj.avg_cpu_usage_lst)
        plt.ylabel('avg ram utilization')
        savefig('Graphs/avg_ram_utilization.jpeg')