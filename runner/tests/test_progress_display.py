import sys
import unittest
import subprocess

from jpyutils import runner

class TestTableProgressDisplay(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_add_job(self):
        display = runner.progress_display.TableProgressDisplay()


if __name__ == '__main__':
    unittest.main()
