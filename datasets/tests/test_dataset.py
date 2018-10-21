from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import unittest

from jpyutils.datasets import Dataset

class DemoDataset(Dataset):
    def __init__(self):
        super(self.__class__, self).__init__()

    def load(self, dataset=None, tokenizer=None, lowercase=True):
        pass


class TestDataset(unittest.TestCase):
    def setUp(self):
        pass

    def test_init(self):
        dataset = DemoDataset()
        self.assertTrue(hasattr(dataset, "_m_module_conf"))

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
