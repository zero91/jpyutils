import lanfang

import unittest
import os
import shutil
import hashlib
import pathlib


class TestDisk(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def _write_content(self, parent_dir, fname, content):
    with (pathlib.Path(parent_dir) / fname).open('w') as fout:
      fout.write(content)

  def test_move_data(self):
    random_path = pathlib.Path(lanfang.utils.random.random_str(10))

    # case 1. single file, dest not exist
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "1.txt", lanfang.utils.random.random_str(1000))

      lanfang.utils.disk.move_data([random_path/"1.txt"], random_path/"case_1")
      self.assertFalse((random_path/"1.txt").exists())
      self.assertTrue((random_path/"case_1").exists())
    finally:
      shutil.rmtree(random_path)

    # case 2. single file, dest exist and is a file
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "2.txt", lanfang.utils.random.random_str(1000))

      self._write_content(
        random_path, "case_2", lanfang.utils.random.random_str(1000))

      with self.assertRaises(IOError):
        lanfang.utils.disk.move_data(
          [random_path/"2.txt"], random_path/"case_2")

      self.assertTrue((random_path/"2.txt").exists())
      self.assertTrue((random_path/"case_2").exists())
    finally:
      shutil.rmtree(random_path)

    # case 3. single file, dest exist and is a dir
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "3.txt", lanfang.utils.random.random_str(1000))

      (random_path/"case_3").mkdir()
      lanfang.utils.disk.move_data([random_path/"3.txt"], random_path/"case_3")
      self.assertFalse((random_path/"3.txt").exists())
      self.assertTrue((random_path/"case_3/3.txt").exists())
    finally:
      shutil.rmtree(random_path)

    # case 4. multiple files, dest not exist
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "4.1.txt", lanfang.utils.random.random_str(1000))

      self._write_content(
        random_path, "4.2.txt", lanfang.utils.random.random_str(1000))

      lanfang.utils.disk.move_data(
        [random_path/"4.1.txt", random_path/"4.2.txt"], random_path/"case_4")

      self.assertFalse((random_path/"4.1.txt").exists())
      self.assertFalse((random_path/"4.2.txt").exists())
      self.assertTrue((random_path/"case_4/4.1.txt").exists())
      self.assertTrue((random_path/"case_4/4.2.txt").exists())
    finally:
      shutil.rmtree(random_path)

    # case 5. multiple files, dest exist and is a file
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "5.1.txt", lanfang.utils.random.random_str(1000))

      self._write_content(
        random_path, "5.2.txt", lanfang.utils.random.random_str(1000))

      self._write_content(
        random_path, "case_5", lanfang.utils.random.random_str(1000))

      with self.assertRaises(IOError):
        lanfang.utils.disk.move_data(
          [random_path/"5.1.txt", random_path/"5.2.txt"], random_path/"case_5")

    finally:
      shutil.rmtree(random_path)

    # case 6. multiple files, dest exist and is a dir
    try:
      random_path.mkdir()
      self._write_content(
        random_path, "6.1.txt", lanfang.utils.random.random_str(1000))

      self._write_content(
        random_path, "6.2.txt", lanfang.utils.random.random_str(1000))

      (random_path/"case_6").mkdir()

      lanfang.utils.disk.move_data(
        [random_path/"6.1.txt", random_path/"6.2.txt"],
        random_path/"case_6")

      self.assertFalse((random_path/"6.1.txt").exists())
      self.assertFalse((random_path/"6.2.txt").exists())
      self.assertTrue((random_path/"case_6/6.1.txt").exists())
      self.assertTrue((random_path/"case_6/6.2.txt").exists())
    finally:
      shutil.rmtree(random_path)

  def test_md5(self):
    with open(__file__, 'rb') as fin:
      self.assertEqual(
        hashlib.md5(fin.read()).hexdigest(),
        lanfang.utils.disk.md5(__file__))

  def test_is_fresh(self):
    os.system("touch " + __file__)
    self.assertTrue(lanfang.utils.disk.is_fresh(__file__))
    self.assertFalse(lanfang.utils.disk.is_fresh(__file__, 0))

