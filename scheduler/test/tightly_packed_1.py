import unittest


class TestRoundRobin(unittest.TestCase):
    def setUp(self):
        print "in tp1"

    def tearDown(self):
        print "in teardown"

    def test_allocation(self):
        self.assertEqual(True,True)


if __name__ == "__main__":
    unittest.main()
