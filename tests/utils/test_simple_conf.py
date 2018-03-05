#coding: utf-8
import os
import sys
import subprocess
import tempfile
import unittest

sys.path.insert(0, "../../../")
import jpyutils

class TestSimpleConf(unittest.TestCase):
    def setUp(self):
        self.__tmp_fd, self.__tmp_fname = tempfile.mkstemp()
        os.write(self.__tmp_fd, "name: zhangsan\n")
        os.write(self.__tmp_fd, " age : 20   \n")
        os.write(self.__tmp_fd, " # comment: testing \n")
        os.write(self.__tmp_fd, " admins : zhangsan lisi  wangwu \n")
        os.close(self.__tmp_fd)

    def tearDown(self):
        os.remove(self.__tmp_fname)

    def test_conf(self):
        conf = jpyutils.utils.SimpleConf(self.__tmp_fname)
        self.assertEqual(len(conf), 3)
        self.assertEqual(conf['name'], "zhangsan")
        self.assertEqual(conf['age'], "20")
        self.assertEqual(conf['admins'], "zhangsan lisi  wangwu")
        with self.assertRaises(KeyError) as exception:
            conf['non_exist_key']
        del conf['name']
        self.assertEqual(len(conf), 2)

    def test_load_conf(self):
        conf = jpyutils.utils.SimpleConf(self.__tmp_fname)
