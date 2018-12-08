import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.pyplot import savefig

df = pd.read_csv('/mnt/azure_data/time_series/zzzz5i2HiSaz6ZdLR6PXdnD.csv')[:576]

y1 = df["min_cpu_util"]
y2 = df["max_cpu_util"]
y3 = df["avg_cpu_util"]

# plot 1
plt.figure(1)
plt.plot(y1)
plt.ylabel('min cpu utilization')
savefig('min_cpu_utilisation.jpeg')

# plot 2
plt.figure(2)
plt.plot(y2)
plt.ylabel('max cpu utilization')
savefig('max_cpu_utilisation.jpeg')

# plot 3
plt.figure(3)
plt.plot(y3)
plt.ylabel('avg cpu utilization')
savefig('avg_cpu_utilisation.jpeg')
