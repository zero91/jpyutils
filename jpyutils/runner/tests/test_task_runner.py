from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
import logging
import time
import sys
import subprocess
from jpyutils import runner

class TestTaskRunner(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_join(self):
        task = runner.TaskRunner(
            target=["curl", "https://www.baidu.com"],
        )
        task.start()
        task.join()
        self.assertEqual(task.exitcode, 0)

if __name__ == "__main__":
    unittest.main()
