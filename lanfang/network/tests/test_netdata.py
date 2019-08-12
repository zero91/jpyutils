import unittest
import os
import shutil
import logging

from lanfang.network import netdata

class TestNetdata(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_download(self):
    url = "pip+https://pypi.org/simple?name=requests&version=2.17.0"
    save_dir = "download"
    package_name = "requests-2.17.0-py2.py3-none-any.whl"

    os.makedirs(save_dir, exist_ok=True)
    res = netdata.download(url, save_dir, overwrite=True)
    self.assertEqual(len(res), 1)
    self.assertEqual(
      res[0], os.path.join(save_dir, package_name))
    self.assertTrue(
      os.path.isfile(os.path.join(save_dir, package_name)))
    shutil.rmtree(save_dir)

  def test_pip_download(self):
    index_url = "https://mirrors.aliyun.com/pypi/simple/"
    index_url = "https://pypi.org/simple"
    save_dir = "pip_download"

    res = netdata.pip_download(
      index_url, save_dir,
      {"name": "requests", 'version': '2.19.0'})
    self.assertEqual(len(res), 1)
    self.assertEqual(res[0], save_dir)
    self.assertTrue(os.path.isfile(save_dir))
    os.remove(save_dir)

    os.makedirs(save_dir)
    res = netdata.pip_download(
      index_url, save_dir,
      {"name": "requests", 'version': '2.20.0'})
    package_name = "requests-2.20.0-py2.py3-none-any.whl"
    self.assertEqual(len(res), 1)
    self.assertEqual(res[0], os.path.join(save_dir, package_name))
    self.assertTrue(os.path.isdir(save_dir))
    self.assertTrue(
      os.path.isfile(os.path.join(save_dir, package_name)))
    shutil.rmtree(save_dir)

  def test_http_download(self):
    logging.basicConfig(level=logging.INFO)
    data_url = "https://ai.stanford.edu/~amaas/data/"\
               "sentiment/aclImdb_v1.tar.gz"

    fname = netdata.http_download(data_url, "./http_download")
    self.assertTrue(os.path.isfile(fname))
    os.remove(fname)


if __name__ == "__main__":
  unittest.main()
