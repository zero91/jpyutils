import unittest
from lanfang.ai.utils import tags


class TestTags(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_is_valid_iob(self):
    test_cases = [
        (True, "IOB1", []),
        (True, "IOB1", ['O']),
        (True, "IOB1", ['I-PER', 'I-PER']),
        (True, "IOB1", ['I-PER', 'I-PER', 'B-PER']),
        (False, "IOB1", ['B-PER', 'I-PER', 'O']),
        (False, "IOB1", ['I-PER', 'B-ORG']),
        (False, "IOB1", ['O', 'B-PER']),

        (True, "IOB2", []),
        (True, "IOB2", ['B-PER', 'I-PER', 'O']),
        (True, "IOB2", ['O', 'B-PER']),
        (True, "IOB2", ['O', 'B-PER', 'I-PER', 'B-PER', 'I-PER']),
        (True, "IOB2", ['O', 'B-PER', 'I-PER', 'B-ORG']),
        (True, "IOB2", ['O', 'B-PER', 'I-PER', 'B-ORG', 'I-ORG']),
        (False, "IOB2", ['I-PER', 'B-ORG']),
        (False, "IOB2", ['B-PER', 'I-ORG']),
        (False, "IOB2", ['I-PER', 'B-PER'])
    ]
    for res, version, case in test_cases:
      self.assertEqual(res, tags.is_valid_iob(case, version=version))

  def test_is_valid_iobes(self):
    test_cases = [
        (True, []),
        (True, ['O']),
        (True, ['S-ORG']),
        (True, ['B-PER', 'E-PER']),
        (True, ['B-PER', 'I-PER', 'E-PER']),
        (True, ['B-PER', 'I-PER', 'I-PER', 'E-PER']),
        (True, ['B-PER', 'E-PER', 'S-ORG']),
        (True, ['O', 'B-PER', 'I-PER', 'I-PER', 'E-PER']),
        (True, ['S-ORG', 'O', 'B-PER', 'I-PER', 'I-PER', 'E-PER']),
        (True, ['O', 'B-PER', 'I-PER', 'I-PER', 'E-PER', 'S-ORG']),

        (False, ['B-PER']),
        (False, ['I-PER']),
        (False, ['E-PER']),
        (False, ['B-PER', 'I-PER']),
        (False, ['I-PER', 'B-PER']),
        (False, ['I-PER', 'S-PER']),
        (False, ['I-PER', 'E-PER']),
        (False, ['E-PER', 'E-PER']),
        (False, ['E-PER', 'I-PER']),
    ]
    for res, case in test_cases:
      self.assertEqual(res, tags.is_valid_iobes(case))

  def test_iob1_to_iob2(self):
    # case 1
    self.assertListEqual(tags.iob1_to_iob2(
        ["O", "O", "O", "O"]),
        ["O", "O", "O", "O"])

    # case 2
    with self.assertRaises(ValueError):
      tags.iob1_to_iob2(["B-PER", "I-PER", "I-PER", "O"])

    # case 3
    self.assertListEqual(tags.iob1_to_iob2(
        ["I-PER", "I-PER", "I-PER", "O"]),
        ["B-PER", "I-PER", "I-PER", "O"])

    # case 4
    self.assertListEqual(tags.iob1_to_iob2(
        ["I-PER", "I-PER", "I-PER", "O", "I-ORG", "B-ORG", "B-ORG"]),
        ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"])

    # case 5
    self.assertListEqual(tags.iob1_to_iob2(
        ["I-PER", "B-PER", "I-PER", "O"]),
        ["B-PER", "B-PER", "I-PER", "O"])

    # case 6
    with self.assertRaises(ValueError):
      tags.iob1_to_iob2(["I-PER", "B-PER", "I-PER", "O", "ORG"])

    # case 7
    self.assertListEqual(tags.iob1_to_iob2(
        ["I-PER", "B-PER", "I-ORG", "O", "I-ORG"]),
        ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"])

  def test_iob_to_iobes(self):
    # case 1
    self.assertListEqual(tags.iob_to_iobes(
        ["B-PER", "I-PER", "I-PER", "O"]),
        ["B-PER", "I-PER", "E-PER", "O"])

    # case 2
    self.assertListEqual(tags.iob_to_iobes(
        ["I-PER", "I-PER", "I-PER", "O"], version="IOB1"),
        ["B-PER", "I-PER", "E-PER", "O"])

    with self.assertRaises(ValueError):
      tags.iob_to_iobes(["I-PER", "I-PER", "I-PER", "O"])

    # case 3
    self.assertListEqual(tags.iob_to_iobes(
        ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"]),
        ["B-PER", "I-PER", "E-PER", "O", "S-ORG", "S-ORG", "S-ORG"])

    # case 4
    self.assertListEqual(tags.iob_to_iobes(
        ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"]),
        ["S-PER", "S-PER", "S-ORG", "O", "S-ORG"])

    # case 5
    with self.assertRaises(ValueError):
      tags.iob_to_iobes(["I-PER", "B-PER", "I-ORG", "O", "ORG"])

    # case 6
    self.assertListEqual(tags.iob_to_iobes(
        ["B-PER", "I-PER", "O", "B-ORG"]),
        ["B-PER", "E-PER", "O", "S-ORG"])

  def test_iobes_to_iob(self):
    # case 1
    self.assertListEqual(tags.iobes_to_iob(
        ["B-PER", "I-PER", "E-PER", "O"]),
        ["B-PER", "I-PER", "I-PER", "O"])

    # case 2
    self.assertListEqual(tags.iobes_to_iob(
        ["B-PER", "I-PER", "E-PER", "O"]),
        ["B-PER", "I-PER", "I-PER", "O"])

    # case 3
    self.assertListEqual(tags.iobes_to_iob(
        ["B-PER", "I-PER", "E-PER", "O", "S-ORG", "S-ORG", "S-ORG"]),
        ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"])

    # case 4
    self.assertListEqual(tags.iobes_to_iob(
        ["S-PER", "S-PER", "S-ORG", "O", "S-ORG"]),
        ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"])

    # case 5
    with self.assertRaises(ValueError):
      tags.iobes_to_iob(["I-PER", "B-PER", "I-ORG", "O", "B-ORG"])

    # case 6
    self.assertListEqual(tags.iobes_to_iob(
        ["B-PER", "E-PER", "O", "S-ORG"]),
        ["B-PER", "I-PER", "O", "B-ORG"])

    # case 7
    with self.assertRaises(ValueError):
      tags.iobes_to_iob(["B-PER", "B-PER", "E-PER", "O"])

    # case 8
    with self.assertRaises(ValueError):
      tags.iobes_to_iob(["B-PER", "I-PER", "I-PER", "O"])

    # case 9
    with self.assertRaises(ValueError):
      tags.iobes_to_iob(["I-PER", "I-PER", "E-PER", "O"])

  def test_is_start_chunk(self):
    self.assertTrue(tags.is_start_chunk("O", "B-PER"))
    self.assertTrue(tags.is_start_chunk("O", "I-PER"))
    self.assertTrue(tags.is_start_chunk("O", "E-PER"))
    self.assertTrue(tags.is_start_chunk("O", "S-PER"))
    self.assertFalse(tags.is_start_chunk("O", "O"))

    self.assertTrue(tags.is_start_chunk("B-PER", "B-PER"))
    self.assertFalse(tags.is_start_chunk("B-PER", "I-PER"))
    self.assertFalse(tags.is_start_chunk("B-PER", "E-PER"))
    self.assertTrue(tags.is_start_chunk("B-PER", "S-PER"))
    self.assertFalse(tags.is_start_chunk("B-PER", "O"))

    self.assertTrue(tags.is_start_chunk("I-PER", "B-PER"))
    self.assertFalse(tags.is_start_chunk("I-PER", "I-PER"))
    self.assertFalse(tags.is_start_chunk("I-PER", "E-PER"))
    self.assertTrue(tags.is_start_chunk("I-PER", "S-PER"))
    self.assertFalse(tags.is_start_chunk("I-PER", "O"))

    self.assertTrue(tags.is_start_chunk("E-PER", "B-PER"))
    self.assertTrue(tags.is_start_chunk("E-PER", "I-PER"))
    self.assertTrue(tags.is_start_chunk("E-PER", "E-PER"))
    self.assertTrue(tags.is_start_chunk("E-PER", "S-PER"))
    self.assertFalse(tags.is_start_chunk("E-PER", "O"))

    self.assertTrue(tags.is_start_chunk("S-PER", "B-PER"))
    self.assertTrue(tags.is_start_chunk("S-PER", "I-PER"))
    self.assertTrue(tags.is_start_chunk("S-PER", "E-PER"))
    self.assertTrue(tags.is_start_chunk("S-PER", "S-PER"))
    self.assertFalse(tags.is_start_chunk("S-PER", "O"))

  def test_is_end_chunk(self):
    self.assertFalse(tags.is_end_chunk("O", "B-PER"))
    self.assertFalse(tags.is_end_chunk("O", "I-PER"))
    self.assertFalse(tags.is_end_chunk("O", "E-PER"))
    self.assertFalse(tags.is_end_chunk("O", "S-PER"))
    self.assertFalse(tags.is_end_chunk("O", "O"))

    self.assertTrue(tags.is_end_chunk("B-PER", "B-PER"))
    self.assertFalse(tags.is_end_chunk("B-PER", "I-PER"))
    self.assertFalse(tags.is_end_chunk("B-PER", "E-PER"))
    self.assertTrue(tags.is_end_chunk("B-PER", "S-PER"))
    self.assertTrue(tags.is_end_chunk("B-PER", "O"))

    self.assertTrue(tags.is_end_chunk("I-PER", "B-PER"))
    self.assertFalse(tags.is_end_chunk("I-PER", "I-PER"))
    self.assertFalse(tags.is_end_chunk("I-PER", "E-PER"))
    self.assertTrue(tags.is_end_chunk("I-PER", "S-PER"))
    self.assertTrue(tags.is_end_chunk("I-PER", "O"))

    self.assertTrue(tags.is_end_chunk("E-PER", "B-PER"))
    self.assertTrue(tags.is_end_chunk("E-PER", "I-PER"))
    self.assertTrue(tags.is_end_chunk("E-PER", "E-PER"))
    self.assertTrue(tags.is_end_chunk("E-PER", "S-PER"))
    self.assertTrue(tags.is_end_chunk("E-PER", "O"))

    self.assertTrue(tags.is_end_chunk("S-PER", "B-PER"))
    self.assertTrue(tags.is_end_chunk("S-PER", "I-PER"))
    self.assertTrue(tags.is_end_chunk("S-PER", "E-PER"))
    self.assertTrue(tags.is_end_chunk("S-PER", "S-PER"))
    self.assertTrue(tags.is_end_chunk("S-PER", "O"))

  def test_get_entities(self):
    entities = tags.get_entities(['O', 'O', 'O', 'B-MISC', 'I-MISC', 'I-MISC',
                                  'O', 'B-PER', 'I-PER', 'S-PER'])
    self.assertEqual(len(entities), 3)
    self.assertTupleEqual(entities[0], ('MISC', 3, 5))
    self.assertTupleEqual(entities[1], ('PER', 7, 8))
    self.assertTupleEqual(entities[2], ('PER', 9, 9))

    entities = tags.get_entities(['O', 'O', 'I-MISC', 'E-MISC', 'B-MISC', 'O',
                                  'B-PER', 'B-PER', 'E-PER'])
    self.assertEqual(len(entities), 4)
    self.assertTupleEqual(entities[0], ('MISC', 2, 3))
    self.assertTupleEqual(entities[1], ('MISC', 4, 4))
    self.assertTupleEqual(entities[2], ('PER', 6, 6))
    self.assertTupleEqual(entities[3], ('PER', 7, 8))
