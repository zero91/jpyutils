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
        utils.utilities.get_logger()

    def tearDown(self):
        if os.path.exists(self.__random_dir):
            shutil.rmtree(self.__random_dir)

    def test_download(self):
        url = "http://www.libfm.org/libfm-1.42.src.tar.gz"

        succeed, file_size = utils.netdata.download(url, "%s/src.tar.gz" % (self.__random_dir))
        self.assertTrue(succeed)
        self.assertGreater(file_size, 0)

        with self.assertRaises(IOError):
            utils.netdata.download(url, "%s/src.tar.gz/ttt" % (self.__random_dir))

    def test_upload(self):
        url = "http://httpbin.org/post"

        params = {"name": "Donald", "usage": "test"}
        data = {"date": "2018-06-22"}
        files = {"code": __file__}

        succeed, content = utils.netdata.upload(url, params=params, data=data, files=files)
        self.assertTrue(succeed)
        self.assertGreater(len(content), os.path.getsize(__file__))

    def test_request(self):
        normal_url = "http://www.baidu.com"
        self.assertGreater(len(utils.netdata.request(normal_url)), 1000)

        # For some reasons, some version of python doesn't support SSL connection very well.
        # This is just a demo.
        ssl_url = "https://www.baidu.com"
        self.assertGreater(len(utils.netdata.request(ssl_url, use_ssl=True)), 1000)

if __name__ == "__main__":
    unittest.main()
