import lanfang
import unittest


class TestMonitor(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_tint(self):
    print(lanfang.utils.terminal.tint(
      "BG_Yellow", bg_color="yellow"))

    print(lanfang.utils.terminal.tint(
      "Highlight", bg_color=None, highlight=True))

    print(lanfang.utils.terminal.tint(
      "RED", bg_color=None, highlight=False))
