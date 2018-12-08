import os
import sys
parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
parent_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
sys.path.append(parent_dir)
import unittest
from  scheduler.executor import *
from scheduler.config_loader import *



class TestRoundRobin(unittest.TestCase):
    def setUp(self):
        self.exe = Executor()

    def tearDown(self):
        pass

    def update_config(self, path):
        with open(path, "r") as f:
            config.update(json.load(f, object_pairs_hook=OrderedDict))

    def test_allocation(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/allocation_test/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    #print "actual:{0},reference:{1}".format(actual,reference)
                    if actual != reference:
                        print "Error actual = {a} : reference = {r}".format(a=actual, r=reference)
                    self.assertEqual(actual, reference)

    def test_full_round(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/full_round_test/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    #print "actual:{0},reference:{1}".format(actual,reference)
                    if actual != reference:
                        print "Error actual = {a} : reference = {r}".format(a=actual, r=reference)
                    self.assertEqual(actual, reference)
                    
    def test_allocation2(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/allocation_test2/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    #print "actual:{0},reference:{1}".format(actual,reference)
                    l1 = actual.split(',')
                    l2 = actual.split(',')
                    lst1 = map(lambda x: x.split('_'),l1[0:10])
                    lst1 = map(lambda x: (float(x[0]),float(x[1])),lst1)
                    lst2 = map(lambda x: x.split('_'),l2[0:10])
                    lst2 = map(lambda x: (float(x[0]),float(x[1])),lst2)
                    
                    for item1, item2 in zip(lst1, lst2):
                        if abs(item1[0]-item2[0])>0.01 or abs(item1[1]-item2[1])>0.01:
                            print "Error actual = {a} : reference = {r}".format(a=item1, r=item2)
                        self.assertTrue(abs(item1[0]-item2[0])<0.01)
                        self.assertTrue(abs(item1[1]-item2[1])<0.01)

                    lst1 = l1[10:]
                    lst2 = l2[10:]
                    lst1 = map(float, lst1)
                    lst2 = map(float, lst2)
                    for item1,item2 in zip(lst1,lst2):
                      if abs(item1-item2)>0.01:
                          print "Error actual = {a} : reference = {r}".format(a=lst1, r=lst2)
                      self.assertTrue(abs(item1-item2)<0.01)

                    
    def test_no_servers(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/no_ofservers_test/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    #print "actual:{0},reference:{1}".format(actual,reference)
                    l1 = actual.split(',')
                    l2 = actual.split(',')
                    lst1 = map(lambda x: x.split('_'),l1[0:10])
                    lst1 = map(lambda x: (float(x[0]),float(x[1])),lst1)
                    lst2 = map(lambda x: x.split('_'),l2[0:10])
                    lst2 = map(lambda x: (float(x[0]),float(x[1])),lst2)
                    
                    for item1, item2 in zip(lst1, lst2):
                        if abs(item1[0]-item2[0])>0.01 or abs(item1[1]-item2[1])>0.01:
                            print "Error actual = {a} : reference = {r}".format(a=item1, r=item2)
                        self.assertTrue(abs(item1[0]-item2[0])<0.01)
                        self.assertTrue(abs(item1[1]-item2[1])<0.01)

                    lst1 = l1[10:]
                    lst2 = l2[10:]
                    lst1 = map(float, lst1)
                    lst2 = map(float, lst2)
                    for item1,item2 in zip(lst1,lst2):
                      if abs(item1-item2)>0.01:
                          print "Error actual = {a} : reference = {r}".format(a=lst1, r=lst2)
                      self.assertTrue(abs(item1-item2)<0.01)


    def test_create_delete(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/create_delete_test/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    # print "actual:{0},reference:{1}".format(actual,reference)
                    l1 = actual.split(',')
                    l2 = reference.split(',')
                    lst1 = map(lambda x: x.split('_'), l1[0:10])
                    lst1 = map(lambda x: (float(x[0]), float(x[1])), lst1)
                    lst2 = map(lambda x: x.split('_'), l2[0:10])
                    lst2 = map(lambda x: (float(x[0]), float(x[1])), lst2)

                    for item1, item2 in zip(lst1, lst2):
                        if abs(item1[0]-item2[0])>0.01 or abs(item1[1]-item2[1])>0.01:
                            print "Error actual = {a} : reference = {r}".format(a=item1, r=item2)
                        self.assertTrue(abs(item1[0]-item2[0])<0.01)
                        self.assertTrue(abs(item1[1]-item2[1])<0.01)

                    lst1 = l1[10:]
                    lst2 = l2[10:]
                    lst1 = map(float, lst1)
                    lst2 = map(float, lst2)
                    for item1,item2 in zip(lst1,lst2):
                      if abs(item1-item2)>0.01:
                          print "Error actual = {a} : reference = {r}".format(a=lst1, r=lst2)
                      self.assertTrue(abs(item1-item2)<0.01)
                      
    def test_merge(self):
        home_path = get_parent_path()
        delta_path = os.path.join(home_path,"scheduler/test/test_case_data/round_robin/merge_testcase/delta_config.json")
        self.update_config(delta_path)
        self.exe.execute()
        actual_output_path = config["actual_output_path"]
        actual_output_path = os.path.join(get_parent_path(),actual_output_path)
        reference_output_path = config["reference_output_path"]
        reference_output_path = os.path.join(get_parent_path(), reference_output_path)
        with open(actual_output_path, "r") as a_o:
            with open(reference_output_path, "r") as r_o:
                for actual, reference in zip(a_o, r_o):
                    # print "actual:{0},reference:{1}".format(actual,reference)
                    l1 = actual.split(',')
                    l2 = reference.split(',')
                    lst1 = map(lambda x: x.split('_'), l1[0:10])
                    lst1 = map(lambda x: (float(x[0]), float(x[1])), lst1)
                    lst2 = map(lambda x: x.split('_'), l2[0:10])
                    lst2 = map(lambda x: (float(x[0]), float(x[1])), lst2)

                    for item1, item2 in zip(lst1, lst2):
                        if abs(item1[0]-item2[0])>0.01 or abs(item1[1]-item2[1])>0.01:
                            print "Error actual = {a} : reference = {r}".format(a=item1, r=item2)
                        self.assertTrue(abs(item1[0]-item2[0])<0.01)
                        self.assertTrue(abs(item1[1]-item2[1])<0.01)

                    lst1 = l1[10:]
                    lst2 = l2[10:]
                    lst1 = map(float, lst1)
                    lst2 = map(float, lst2)
                    for item1,item2 in zip(lst1,lst2):
                      if abs(item1-item2)>0.01:
                          print "Error actual = {a} : reference = {r}".format(a=lst1, r=lst2)
                      self.assertTrue(abs(item1-item2)<0.01)


if __name__ == "__main__":
    unittest.main()
