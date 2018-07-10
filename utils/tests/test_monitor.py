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

    def test_mail(self):
        receivers = "jianzhang9102@gmail.com"
        subject = "Test Email"
        content = 'This is a testing email. <br/><br/>' \
                  '<font size="5" color="red">Just ignore this email.</font>'
        self.assertEqual(utils.monitor.mail(receivers, subject, content), 0)

if __name__ == "__main__":
    unittest.main()
