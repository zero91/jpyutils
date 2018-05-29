from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
from jpyutils import utils

class TestRandom(unittest.TestCase):
    def setUp(self):
        pass

    def test_random_str(self):
        random.seed(2018)
        self.assertEqual(utils.random.random_str(4), 'Iibz')
        self.assertEqual(utils.random.random_str(6), 'OQGthB')

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
