from lanfang.utils import disk

import tempfile
import unittest
import os
import shutil
import hashlib


class TestDisk(unittest.TestCase):
  def setUp(self):
    self._m_data_dir = tempfile.mkdtemp()

  def tearDown(self):
    if os.path.exists(self._m_data_dir):
      shutil.rmtree(self._m_data_dir)

  def test_move_data(self):
    # case 1. single file, dest not exist
    case_1_file = self.__write_content(self._m_data_dir, "1.txt", "1.txt file")
    new_1_file = disk.move_data(
        case_1_file,
        os.path.join(self._m_data_dir, "new_1.txt"))

    self.assertFalse(os.path.exists(case_1_file))
    self.assertTrue(os.path.exists(new_1_file))

    # case 2. single file, dest exist and is a file
    case_2_file = self.__write_content(self._m_data_dir, "2.txt", "2.txt file")
    dup_2_file = self.__write_content(
        self._m_data_dir, "dup_2.txt", "dup_2.txt file")

    with self.assertRaises(IOError):
      disk.move_data(case_2_file, dup_2_file)

    self.assertTrue(os.path.exists(case_2_file))
    self.assertTrue(os.path.exists(dup_2_file))

    # case 3. single file, dest exist and is a dir
    case_3_file = self.__write_content(self._m_data_dir, "3.txt", "3.txt file")
    case_3_dir = os.path.join(self._m_data_dir, "case_3")
    os.makedirs(case_3_dir, exist_ok=True)

    new_3_file = disk.move_data(case_3_file, case_3_dir)
    self.assertFalse(os.path.exists(case_3_file))
    self.assertTrue(os.path.exists(new_3_file))
    self.assertEqual(new_3_file, os.path.join(case_3_dir, "3.txt"))

    # case 4. multiple files, dest not exist
    case_4_1_file = self.__write_content(
        self._m_data_dir, "4.1.txt", "4.1.txt file")
    case_4_2_file = self.__write_content(
        self._m_data_dir, "4.2.txt", "4.2.txt file")
    case_4_dir = os.path.join(self._m_data_dir, "case_4")
    new_4_files = disk.move_data([case_4_1_file, case_4_2_file], case_4_dir)

    self.assertIsInstance(new_4_files, list)
    self.assertEqual(len(new_4_files), 2)

    self.assertFalse(os.path.exists(case_4_1_file))
    self.assertFalse(os.path.exists(case_4_2_file))
    for new_file in new_4_files:
      self.assertTrue(os.path.exists(new_file))

    # case 5. multiple files, dest exist and is a file
    case_5_1_file = self.__write_content(
        self._m_data_dir, "5.1.txt", "5.1.txt file")

    case_5_2_file = self.__write_content(
        self._m_data_dir, "5.2.txt", "5.2.txt file")

    case_5_dup_file = self.__write_content(
        self._m_data_dir, "dup_5.txt", "dup_5.txt file")

    with self.assertRaises(IOError):
      disk.move_data([case_5_1_file, case_5_2_file], case_5_dup_file)

    # case 6. multiple files, dest exist and is a dir
    case_6_1_file = self.__write_content(
        self._m_data_dir, "6.1.txt", "6.1.txt file")

    case_6_2_file = self.__write_content(
        self._m_data_dir, "6.2.txt", "6.2.txt file")

    case_6_dir = os.path.join(self._m_data_dir, "case_6")

    new_6_files = disk.move_data([case_6_1_file, case_6_2_file], case_6_dir)

    self.assertFalse(os.path.exists(case_6_1_file))
    self.assertFalse(os.path.exists(case_6_2_file))
    for new_file in new_6_files:
      self.assertTrue(os.path.exists(new_file))

  def test_md5(self):
    with open(__file__, 'rb') as fin:
      self.assertEqual(
          hashlib.md5(fin.read()).hexdigest(), disk.md5(__file__))

  def test_is_fresh(self):
    test_file = self.__write_content(
        self._m_data_dir, "fresh.txt", "fresh.txt file")

    self.assertTrue(disk.is_fresh(test_file, days=1))
    self.assertFalse(disk.is_fresh(test_file, days=0))

  def __write_content(self, save_dir, fname, content):
    save_file = os.path.join(save_dir, fname)
    with open(save_file, 'w') as fout:
      fout.write(content)
    return save_file
