from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
import logging
import time
import subprocess
from jpyutils import runner

class TestTaskRunner(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_join(self):
        task = runner.TaskRunner(
            cmd=["curl", "https://www.baidu.com"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        task.start()
        task.join()
        self.assertEqual(task.exitcode, 0)

if __name__ == "__main__":
    unittest.main()
