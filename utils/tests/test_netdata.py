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
        url = "http://www.libfm.org/libfm-1.42.src.tar.gz"
        logger = utils.utilities.get_logger()

        res, headers = utils.netdata.download(url, "%s/src.tar.gz" % (self.__random_dir))
        self.assertTrue(res)

        self.assertEqual(utils.netdata.download(url, "%s/src.tar.gz" % (self.__random_dir)), (True, None))
        with self.assertRaises(IOErrorx):
            utils.netdata.download(url, "%s/src.tar.gz/ttt" % (self.__random_dir))

    def tearDown(self):
        shutil.rmtree(self.__random_dir)

if __name__ == "__main__":
    unittest.main()
