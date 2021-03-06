import lanfang
import unittest
import random
import logging
import time
import random
import subprocess
import sys

def test_func(name, age, money=100):
  print("-" * 100)
  print("Begin test_func")
  print("name = '%s'" % (name))
  print("age = '%s'" % (age))
  print("money = '%s'" % (money))

  print("End test_func")
  print("-" * 100)
  #return {"name": "Hello " + name, "age": age + 100, "money": money * 10000}


class TestProcRunner(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_proc(self):
    def _proc_with_exception(a, b, name="test"):
      raise ValueError("Test Exception")

    # Case 1: Missing parameters
    proc_1 = lanfang.runner.ProcRunner(
      target=_proc_with_exception,
      name="proc_1",
      retry=3,
    )
    proc_1.start()
    proc_1.join()
    self.assertEqual(proc_1.exitcode, 1)

    # Case 2: Riase an exception
    proc_2 = lanfang.runner.ProcRunner(
      target=_proc_with_exception,
      name="proc_2",
      args=(1, 2),
      retry=3,
    )
    proc_2.start()
    proc_2.join()
    self.assertEqual(proc_2.exitcode, 1)

    # Case 3: Normal
    proc_3 = lanfang.runner.ProcRunner(
      target=sum,
      name="proc_3",
      args=(range(10),),
      retry=3,
    )
    proc_3.start()
    proc_3.join()
    self.assertEqual(proc_3.exitcode, 0)
    self.assertTrue("elapsed_time" in proc_3.info)
    self.assertTrue("start_time" in proc_3.info)
    self.assertTrue("exitcode" in proc_3.info)
    self.assertTrue("try" in proc_3.info)

  def test_proc_return_value(self):
    def func(a, b):
      return "%d + %d = %d" % (a, b, a + b)

    proc = lanfang.runner.ProcRunner(
      target=func,
      name="proc",
      args=(1, 3),
      retry=3,
    )
    proc.start()
    proc.join()
    self.assertEqual(proc.info['return'], '1 + 3 = 4')

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

    proc = lanfang.runner.ProcRunner(
      target = test_func,
      name = "proc",
      args=("zero91", 28),
      #kwargs={"money": 50},
      share_dict=share_dict
    )
    proc.start()
    proc.join()
    print("=" * 100)
    print("share_dict = %s" % (share_dict))
    print("DONE")
    print("=" * 100)

if __name__ == "__main__":
  unittest.main()
