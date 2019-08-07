from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import os
import shutil
import hashlib

from jpyutils import utils

class TestDisk(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_move_data(self):
        # case 1. single file, dest not exist
        os.system("man ls > 1.txt")
        utils.disk.move_data(["1.txt"], "case_1")
        self.assertFalse(os.path.exists("1.txt"))
        self.assertTrue(os.path.exists("case_1"))
        os.remove("case_1")

        # case 2. single file, dest exist and is a file
        os.system("man ls > 2.txt")
        os.system("touch case_2")
        utils.disk.move_data(["2.txt"], "case_2")
        self.assertTrue(os.path.exists("2.txt"))
        self.assertTrue(os.path.exists("case_2"))
        os.remove("2.txt")
        os.remove("case_2")

        # case 3. single file, dest exist and is a dir
        os.system("man ls > 3.txt")
        os.makedirs("case_3")
        utils.disk.move_data(["3.txt"], "case_3")
        self.assertFalse(os.path.exists("3.txt"))
        self.assertTrue(os.path.exists("case_3/3.txt"))
        shutil.rmtree("case_3")

        # case 4. multiple files, dest not exist
        os.system("man ls > 4.1.txt")
        os.system("man cp > 4.2.txt")
        utils.disk.move_data(["4.1.txt", "4.2.txt"], "case_4")
        self.assertFalse(os.path.exists("4.1.txt"))
        self.assertFalse(os.path.exists("4.2.txt"))
        self.assertTrue(os.path.exists("case_4/4.1.txt"))
        self.assertTrue(os.path.exists("case_4/4.2.txt"))
        shutil.rmtree("case_4")

        # case 5. multiple files, dest exist and is a file
        os.system("man ls > 5.1.txt")
        os.system("man cp > 5.2.txt")
        os.system("touch case_5")
        with self.assertRaises(IOError):
            utils.disk.move_data(["5.1.txt", "5.2.txt"], "case_5")
        os.remove("5.1.txt")
        os.remove("5.2.txt")
        os.remove("case_5")

        # case 6. multiple files, dest exist and is a dir
        os.system("man ls > 6.1.txt")
        os.system("man cp > 6.2.txt")
        os.makedirs("case_6")
        utils.disk.move_data(["6.1.txt", "6.2.txt"], "case_6")
        self.assertFalse(os.path.exists("6.1.txt"))
        self.assertFalse(os.path.exists("6.2.txt"))
        self.assertTrue(os.path.exists("case_6/6.1.txt"))
        self.assertTrue(os.path.exists("case_6/6.2.txt"))
        shutil.rmtree("case_6")

        os.system("man ls > 6.1.txt")
        os.system("man cp > 6.2.txt")
        os.makedirs("case_6")
        os.system("man ls > case_6/6.1.txt")
        utils.disk.move_data(["6.1.txt", "6.2.txt"], "case_6")
        self.assertFalse(os.path.exists("6.1.txt"))
        self.assertFalse(os.path.exists("6.2.txt"))
        self.assertTrue(os.path.exists("case_6/6.1.txt"))
        self.assertTrue(os.path.exists("case_6/6.2.txt"))
        shutil.rmtree("case_6")

    def test_md5(self):
        with open(__file__, 'rb') as fin:
            self.assertEqual(hashlib.md5(fin.read()).hexdigest(), utils.disk.md5(__file__))

    def test_is_fresh(self):
        os.system("touch " + __file__)
        self.assertTrue(utils.disk.is_fresh(__file__))
        self.assertFalse(utils.disk.is_fresh(__file__, 0))


if __name__ == "__main__":
    unittest.main()

