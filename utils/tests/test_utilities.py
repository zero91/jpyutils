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

class TestUtilities(unittest.TestCase):
    def setUp(self):
        self.__random_dir = utils.random.random_str(32)

    def test_get_logger(self):
        logger = utils.utilities.get_logger(__name__, save_to_disk=True,
                                            path=self.__random_dir, level=logging.DEBUG)

        logger.debug('this is a logger debug message')
        logger.info('this is a logger info message')
        logger.warning('this is a logger warning message')
        logger.error('this is a logger error message')
        logger.critical('this is a logger critical message')

        self.assertTrue(os.path.exists(self.__random_dir))
        self.assertFalse(os.path.isfile(self.__random_dir))
        shutil.rmtree(self.__random_dir)

    def test_read_zip(self):
        url = "https://github.com/srendle/libfm/archive/master.zip"
        utils.netdata.download(url, self.__random_dir)
        contents = utils.utilities.read_zip(self.__random_dir, filelist=".*.cpp", merge=False)
        for key in contents:
            self.assertEqual(key[-4:], ".cpp")
        self.assertGreater(len(utils.utilities.read_zip(self.__random_dir, merge=False)), 5)
        self.assertGreater(len(utils.utilities.read_zip(self.__random_dir, merge=True)), 10000)
        os.remove(self.__random_dir)

    def test_is_fresh_file(self):
        os.system("touch " + __file__)
        self.assertTrue(utils.utilities.is_fresh_file(__file__))
        self.assertFalse(utils.utilities.is_fresh_file(__file__, 0))

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
