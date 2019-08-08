import lanfang
import unittest
import random
import logging
import time
import sys
import os
import subprocess


class TestTaskRunner(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_join(self):
    task = lanfang.runner.TaskRunner(["curl", "https://www.baidu.com"])
    task.start()
    task.join()
    self.assertEqual(task.exitcode, 0)

  def test_share_data(self):
    import multiprocessing
    m = multiprocessing.Manager()
    share_dict = m.dict({
      "proc": {
        "input": {
          "name": "donald",
          "age": 19,
          "money": 88,
        },
        #"output": {}
      }
    })
    return
    #TODO
    task = lanfang.runner.TaskRunner(
      #target=["python", "tmp.py", "-o", "./abc", "--name", "apple"],
      #target=" ".join(["python", "tmp.py", "-o", "./abc", "--name", "apple"]),
      target=" ".join(["python", "tmp.py", "--name", "apple"]),
      share_dict=share_dict,
      name="proc",
      shell=True,
      pre_hook=tmp.pre_hook)
    task.start()
    task.join()
    self.assertEqual(task.exitcode, 0)
    print(share_dict)

if __name__ == "__main__":
  unittest.main()
