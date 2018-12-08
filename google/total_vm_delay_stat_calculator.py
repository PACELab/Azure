def get_total_delay_product(path):
    """
    This function tries to calculate the total_vm_delay for a particular scheme, which can be compared against other schemes to decide how drastically algorithm is trying to delay VMs.
    
    It would be great if we actually compare cores*delay because that would present us with a more correct statistic.
    """
    product = 0
    with open(path) as f:
        for line in f:
            num_vms,time = map(int,line.split(","))
            product += num_vms*time
    return product


print get_total_delay_product("/mnt/google_data/scheduler/Graphs/round_robin_min_delay/2threshold_6500_20min/delay_stats.txt")


