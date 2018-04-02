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
                    print "actual:{0},reference:{1}".format(actual,reference)
                    if a_o != r_o:
                        print "Error actual = {a} : reference = {r}".format(a=a_o, r=r_o)
                    self.assertEqual(actual, reference)


if __name__ == "__main__":
    unittest.main()
