import lanfang
import unittest
import random


class TestRandom(unittest.TestCase):
  def setUp(self):
    pass

  def test_random_str(self):
    random.seed(2018)
    self.assertEqual(lanfang.utils.random.random_str(4), 'Iibz')
    self.assertEqual(lanfang.utils.random.random_str(6), 'OQGthB')

  def tearDown(self):
    pass
