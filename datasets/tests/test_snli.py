from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import logging
import tempfile
import os
import numpy as np
from jpyutils.datasets import SNLIDataset

class TestSNLIDataset(unittest.TestCase):
    def setUp(self):
        pass

    def test_load(self):
        snli = SNLIDataset()
        data = snli.load()

        self.assertEqual(len(data), 3)
        self.assertTrue('train' in data)
        self.assertTrue('dev' in data)
        self.assertTrue('test' in data)
        self.assertEqual(len(data['train'][0]), 3)
        self.assertEqual(len(data['dev'][0]), 3)
        self.assertEqual(len(data['test'][0]), 3)

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
