from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
import logging
import tempfile
import os
import shutil
from jpyutils import utils

class TestNetdata(unittest.TestCase):
    def setUp(self):
        self.__random_dir = utils.random.random_str(32)

    def test_download(self):
        utils.netdata.download("http://nlp.stanford.edu/data/glove.42B.300d.zip", "./tmp/ttt")

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
