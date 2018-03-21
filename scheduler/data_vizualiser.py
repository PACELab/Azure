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
        plt.plot(self.data_obj.avg_servers_allocated)
        plt.ylabel('avg server utilization')
        savefig('Graphs/avg_server_utilization.jpeg')
