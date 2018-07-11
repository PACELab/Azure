from collections import OrderedDict
from scheduler.config_loader import *
power = {
    4: [
        (0, 10, 75.02863247863247),
        (11, 20, 77.83760683760684),
        (21, 30, 80.82905982905983),
        (31, 40, 83.91025641025641),
        (41, 50, 87.42521367521367),
        (51, 60, 90.88034188034187),
        (61, 70, 94.61965811965811),
        (71, 80, 97.07264957264957),
        (81, 90, 100.42307692307693),
        (91, 99, 102.87606837606837),
        (100, 100, 105.0)

    ],
    8: [
        (0, 10, 92.89259259259259),
        (11, 20, 96.37037037037038),
        (21, 30, 100.07407407407408),
        (31, 40, 103.8888888888889),
        (41, 50, 108.24074074074073),
        (51, 60, 112.51851851851852),
        (61, 70, 117.14814814814815),
        (71, 80, 120.18518518518518),
        (81, 90, 124.33333333333334),
        (91, 99, 127.37037037037037),
        (100, 100, 130.0)

    ],
    16: [
        (0, 10, 146.48447293447293),
        (11, 20, 151.968660968661),
        (21, 30, 157.8091168091168),
        (31, 40, 163.82478632478634),
        (41, 50, 170.68732193732194),
        (51, 60, 177.4330484330484),
        (61, 70, 184.73361823361822),
        (71, 80, 189.522792022792),
        (81, 90, 196.06410256410257),
        (91, 99, 200.85327635327633),
        (100, 100, 205.0)

    ]
}

s = 0.7
c=0
from random import randint


class Consolidation(object):
    def __init__(self, allocation_dict, time_stamp):
        global c
        c+=1
        # print "received request {c}".format(c=c)
        self.allocation_dict = allocation_dict
        self.adaptive_utilisation_threshold = 0
        self.vms_to_migrate = []
        self.time_stamp = time_stamp

    def execute(self):
        if self.adap_ut_thr():
            self.create_migrate_map()
            self.migrate_vms()
            self.underload_detection()

    def adap_ut_thr(self):
        f_l = []
        for pool in self.allocation_dict.keys():
            pool_value = self.allocation_dict[pool]
            for server_num in pool_value.keys():
                allocation_obj = pool_value[server_num]
                f_l.append(
                    (allocation_obj.total_num_cores - allocation_obj.num_cores_left) / allocation_obj.total_num_cores)
        if not f_l:
            return False
        median = self.median(f_l)
        deviations = [abs(i - median) for i in f_l]
        mad = self.median(deviations)
        self.adaptive_utilisation_threshold = 1 - s * mad
        return True

    def median(self, lst):
        s_l = sorted(lst)
        m = len(s_l) / 2
        l = len(s_l)
        if l == 1:
            return s_l[0]

        elif l % 2 == 0:
            return s_l[m] + s_l[m - 1] / 2.000

        else:
            return s_l[m]

    def create_migrate_map(self):
        for pool in self.allocation_dict.keys():
            pool_value = self.allocation_dict[pool]
            for server_num in pool_value.keys():
                allocation_obj = pool_value[server_num]
                utilisation = (
                                      allocation_obj.total_num_cores - allocation_obj.num_cores_left) / allocation_obj.total_num_cores
                if utilisation > self.adaptive_utilisation_threshold:
                    l = len(allocation_obj.vms)
                    vm_to_migrate = randint(0, l-1)
                    # TO DO : utilisation is non of cores used by vm
                    #print len(allocation_obj.vms),vm_to_migrate
                    self.allocation_dict[pool][server_num].num_cores_left +=allocation_obj.vms[vm_to_migrate][1]
                    self.allocation_dict[pool][server_num].ram_left += allocation_obj.vms[vm_to_migrate][2]
                    self.vms_to_migrate.append(
                        (pool, server_num, allocation_obj.vms[vm_to_migrate][0], allocation_obj.vms[vm_to_migrate][1],
                         self.get_utilisation(allocation_obj.vms[vm_to_migrate][0]),
                         allocation_obj.vms[vm_to_migrate][2]))
        # print "migrate map 1",self.vms_to_migrate

    def get_utilisation(self, vm_id):
        path = "/mnt/azure_data/new_vm_data"
        subscription, deployment, vm_id = vm_id.split("_")
        path = "{path}/{subs}/{subs}_{dep}/{subs}_{dep}_{vm_id}.csv".format(path=path, subs=subscription,
                                                                            dep=deployment, vm_id=vm_id)
        with open(path, "r")  as f:
            for line in f:
                ts, id, min, max, avg = line.split(",")
                if float(ts) == float(self.time_stamp):
                    return float(avg.strip())

    def migrate_vms(self,caller="overload"):
        # print "caller is {c}".format(c=caller)
        # print "migrate list",self.vms_to_migrate
        # print "allocation in starting"
        # self.print_allocation()
        for from_pool, ser_num, vm_id, num_cores_needed, util, ram_needed in sorted(self.vms_to_migrate,
                                                                                    key=lambda x: x[4]):
            pool_to_use, server_to_use, pow_inc = None, None, float("inf")
            for pool in self.allocation_dict.keys():
                pool_value = self.allocation_dict[pool]
                for server_num in pool_value.keys():
                    allocation_obj = pool_value[server_num]
                    utilisation = (
                                          allocation_obj.total_num_cores - allocation_obj.num_cores_left) / allocation_obj.total_num_cores
                    if ram_needed <= allocation_obj.ram_left and num_cores_needed <= allocation_obj.num_cores_left  and utilisation < self.adaptive_utilisation_threshold:
                        utilisation = (allocation_obj.total_num_cores - allocation_obj.num_cores_left) / allocation_obj.total_num_cores
                        num_cores = allocation_obj.total_num_cores
                        pres_pow = self.power_consumption(utilisation, num_cores=num_cores)
                        # print "pres_pow is ", pres_pow
                        new_util = (
                                           allocation_obj.total_num_cores - allocation_obj.num_cores_left + num_cores_needed) / allocation_obj.total_num_cores
                        new_pow = self.power_consumption(new_util, num_cores=num_cores)
                        pow_inc_tmp = new_pow - pres_pow
                        # print "pow increase:{pi},server:{s}".format(pi=pow_inc_tmp,s=server_num)
                        if pow_inc_tmp < pow_inc:
                            pool_to_use, server_to_use, pow_inc = pool, server_num, pow_inc_tmp
            # print "before migrate***************************"
            # print "from pool :{fp} ,from server:{fs} ,to pool:{tp} ,to server:{ts} ,ram :{r} ,cores:{c}".format(fp=from_pool,fs=ser_num,tp=pool_to_use,ts=server_to_use,r=ram_needed,c=num_cores_needed)
            # self.print_allocation()
            # print "pool,server to use",pool_to_use,server_to_use
            #print "-----",pool_to_use,server_to_use
            # print "tmp is",self.allocation_dict[pool_to_use][server_to_use].vms
            # print "allocation dict",self.allocation_dict
            if pool_to_use is not None and server_to_use is  not  None :
                self.allocation_dict[pool_to_use][server_to_use].num_cores_left -= num_cores_needed
                self.allocation_dict[pool_to_use][server_to_use].ram_left -= ram_needed
                tmp = self.allocation_dict[pool_to_use][server_to_use].vms
                tmp = set(tmp)
                tmp.add((vm_id, num_cores_needed, ram_needed))
                self.allocation_dict[pool_to_use][server_to_use].vms = list(tmp)
                # print "in if "
                # self.print_allocation()
                # print "after migrate****************"
            else:
                return False
            # else:
            #     print "in else"
            #     self.allocation_dict[from_pool][ser_num].num_cores_left -= num_cores_needed
            #     self.allocation_dict[from_pool][ser_num].ram_left -= ram_needed
            #     tmp.add((vm_id, num_cores_needed, ram_needed))
            #     self.allocation_dict[from_pool][ser_num].vms = list(tmp)
            #
            #     print "in false"
            #     self.vms_to_migrate = []
            #     self.print_allocation()
            #     print "after migrate****************"
            #     return False

        self.vms_to_migrate = []
        return True

    def power_consumption(self, utilisation, num_cores):
        global power
        exa_power = power[num_cores]
        ut_per = utilisation * 100
        # print "ut per",ut_per
        if float(ut_per) == float(0):
            return 0
        for l, u, powr in exa_power:
            if l <= ut_per <= u:
                return powr

    def underload_detection(self):
        #select underload  servers
        u_l = []
        for pool in self.allocation_dict:
            pool_value = self.allocation_dict[pool]
            for server_num in pool_value:
                allocation_obj = pool_value[server_num]
                utilisation = (allocation_obj.total_num_cores - allocation_obj.num_cores_left) / allocation_obj.total_num_cores
                if utilisation<self.adaptive_utilisation_threshold and float(utilisation)!=float(0):
                    u_l.append((allocation_obj,utilisation,server_num,pool))
        #check for migration
        for allocation_obj,utilisation,server_num,pool in sorted(u_l,key=lambda x:x[1]):
            for vm in allocation_obj.vms:
                # print "vm is ",vm
                self.vms_to_migrate.append((pool, server_num, vm[0], vm[1], self.get_utilisation(vm[0]), vm[2]))
            b_c,b_r = self.allocation_dict[pool][server_num].num_cores_left,self.allocation_dict[pool][server_num].ram_left
            self.allocation_dict[pool][server_num].num_cores_left = config["servers"]["types"][pool]["max_cores_per_server"]
            self.allocation_dict[pool][server_num].ram_left = config["servers"]["types"][pool]["max_ram_per_server"]
            b_l = self.allocation_dict[pool][server_num].vms
            self.allocation_dict[pool][server_num].vms=[]
            # print "vms migrate map 2",self.vms_to_migrate
            if not self.migrate_vms(caller="underload"):
                self.allocation_dict[pool][server_num].num_cores_left = b_c
                self.allocation_dict[pool][server_num].ram_left = b_r
                self.allocation_dict[pool][server_num].vms = b_l
                break
        # print "Threshold is", self.adaptive_utilisation_threshold
        # self.print_allocation()

    def print_allocation(self):
        for pool in self.allocation_dict:
            pool_value = self.allocation_dict[pool]
            for server_num in pool_value:
                print "pool num : {pn},server_number:{sn},ram_left:{rl},core_left:{cl}".format(pn=pool,
                                                                                                   sn=server_num, rl=
                                                                                                   self.allocation_dict[
                                                                                                       pool][
                                                                                                       server_num].ram_left,
                                                                                                   cl=
                                                                                                   self.allocation_dict[
                                                                                                       pool][
                                                                                                       server_num].num_cores_left)
