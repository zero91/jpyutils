from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import logging
import os
import numpy as np
import itertools
import collections
from jpyutils.datasets import NERDataset 
from jpyutils.utils import utilities

class TestNERDataset(unittest.TestCase):
    def setUp(self):
        utilities.get_logger()

    def test_load_MSRA(self):
        ner = NERDataset()
        data = ner.load("MSRA")

        self.assertEqual(len(data), 3)
        self.assertTrue('train' in data)
        self.assertTrue('dev' in data)
        self.assertTrue('test' in data)
        self.assertEqual(len(data['train']), 46364)
        self.assertIsNone(data['dev'])
        self.assertEqual(len(data['test']), 4365)

    def test_load_CoNLL2003(self):
        ner = NERDataset()
        data = ner.load("CoNLL2003")

        self.assertEqual(len(data), 3)
        self.assertTrue('train' in data)
        self.assertTrue('dev' in data)
        self.assertTrue('test' in data)
        self.assertEqual(len(data['train']), 14041)
        self.assertEqual(len(data['dev']), 3250)
        self.assertEqual(len(data['test']), 3453)

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
