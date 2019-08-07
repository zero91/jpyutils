from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import logging
import os

from jpyutils import utils

class TestMonitor(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_tint(self):
        print(utils.terminal.tint("TestColor", bg_color="yellow"))
        print(utils.terminal.tint("TestColor", bg_color=None, highlight=True))
        print(utils.terminal.tint("TestColor", bg_color=None, highlight=False))

if __name__ == "__main__":
    unittest.main()
