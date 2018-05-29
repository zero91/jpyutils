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
        logger = utils.utilities.get_logger(__name__, save_to_disk=True, path=self.__random_dir, level=logging.DEBUG)

        logger.debug('this is a logger debug message')
        logger.info('this is a logger info message')
        logger.warning('this is a logger warning message')
        logger.error('this is a logger error message')
        logger.critical('this is a logger critical message')

        self.assertTrue(os.path.exists(self.__random_dir))
        self.assertFalse(os.path.isfile(self.__random_dir))
        shutil.rmtree(self.__random_dir)

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
