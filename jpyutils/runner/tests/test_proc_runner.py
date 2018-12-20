from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
import logging
import time
import random
import subprocess
import sys
from jpyutils import runner

class TestProcRunner(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_proc(self):
        def _proc_with_exception(a, b, name="test"):
            raise ValueError("Test Exception")

        # Case 1: Missing parameters
        proc_1 = runner.ProcRunner(
            target=_proc_with_exception,
            name="proc_1",
            retry=3,
        )
        proc_1.start()
        proc_1.join()
        self.assertEqual(proc_1.exitcode, 1)

        # Case 2: Riase an exception
        proc_2 = runner.ProcRunner(
            target=_proc_with_exception,
            name="proc_2",
            args=(1, 2),
            retry=3,
        )
        proc_2.start()
        proc_2.join()
        self.assertEqual(proc_2.exitcode, 1)

        # Case 3: Normal
        proc_3 = runner.ProcRunner(
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

        proc = runner.ProcRunner(
            target=func,
            name="proc",
            args=(1, 3),
            retry=3,
        )
        proc.start()
        proc.join()
        self.assertEqual(proc.info['return'], b'1 + 3 = 4')

if __name__ == "__main__":
    unittest.main()
