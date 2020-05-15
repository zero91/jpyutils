from lanfang.utils import terminal

import unittest


class TestMonitor(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_tint(self):
    self.assertEqual(
        terminal.tint("BG_Yellow", bg_color="yellow"),
        '\033[31;43mBG_Yellow\033[0m')

    self.assertEqual(
        terminal.tint("Highlight", bg_color=None, highlight=True),
        '\033[1;31mHighlight\033[0m')

    self.assertEqual(
        terminal.tint("RED", bg_color=None, highlight=False),
        '\033[31mRED\033[0m')
