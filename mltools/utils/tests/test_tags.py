from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import unittest
import random
from jpyutils.mltools import utils

class TestTags(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_valid_iob(self):
        # Test IOB1
        self.assertTrue(utils.tags.is_valid_iob([], version="IOB1"))
        self.assertTrue(utils.tags.is_valid_iob(['O'], version="IOB1"))
        self.assertTrue(utils.tags.is_valid_iob(['I-PER', 'I-PER'], version="IOB1"))
        self.assertTrue(utils.tags.is_valid_iob(['I-PER', 'I-PER', 'B-PER'], version="IOB1"))

        self.assertFalse(utils.tags.is_valid_iob(['B-PER', 'I-PER', 'O'], version="IOB1"))
        self.assertFalse(utils.tags.is_valid_iob(['I-PER', 'B-ORG'], version="IOB1"))
        self.assertFalse(utils.tags.is_valid_iob(['O', 'B-PER'], version="IOB1"))

        # Test IOB2
        self.assertTrue(utils.tags.is_valid_iob([]))
        self.assertTrue(utils.tags.is_valid_iob(['B-PER', 'I-PER', 'O']))
        self.assertTrue(utils.tags.is_valid_iob(['O', 'B-PER']))
        self.assertTrue(utils.tags.is_valid_iob(['O', 'B-PER', 'I-PER', 'B-PER', 'I-PER']))
        self.assertTrue(utils.tags.is_valid_iob(['O', 'B-PER', 'I-PER', 'B-ORG']))
        self.assertTrue(utils.tags.is_valid_iob(['O', 'B-PER', 'I-PER', 'B-ORG', 'I-ORG']))

        self.assertFalse(utils.tags.is_valid_iob(['I-PER', 'B-ORG']))
        self.assertFalse(utils.tags.is_valid_iob(['B-PER', 'I-ORG']))
        self.assertFalse(utils.tags.is_valid_iob(['I-PER', 'B-PER']))

    def test_is_valid_iobes(self):
        # True
        self.assertTrue(utils.tags.is_valid_iobes([]))
        self.assertTrue(utils.tags.is_valid_iobes(['O']))
        self.assertTrue(utils.tags.is_valid_iobes(['S-ORG']))
        self.assertTrue(utils.tags.is_valid_iobes(['B-PER', 'E-PER']))
        self.assertTrue(utils.tags.is_valid_iobes(['B-PER', 'I-PER', 'E-PER']))
        self.assertTrue(utils.tags.is_valid_iobes(['B-PER', 'I-PER', 'I-PER', 'E-PER']))
        self.assertTrue(utils.tags.is_valid_iobes(['B-PER', 'E-PER', 'S-ORG']))
        self.assertTrue(utils.tags.is_valid_iobes(['O', 'B-PER', 'I-PER', 'I-PER', 'E-PER']))
        self.assertTrue(utils.tags.is_valid_iobes(['S-ORG', 'O', 'B-PER', 'I-PER', 'I-PER', 'E-PER']))
        self.assertTrue(utils.tags.is_valid_iobes(['O', 'B-PER', 'I-PER', 'I-PER', 'E-PER', 'S-ORG']))

        # False
        self.assertFalse(utils.tags.is_valid_iobes(['B-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['I-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['E-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['B-PER', 'I-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['I-PER', 'B-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['I-PER', 'S-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['I-PER', 'E-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['E-PER', 'E-PER']))
        self.assertFalse(utils.tags.is_valid_iobes(['E-PER', 'I-PER']))

    def test_iob1_to_iob2(self):
        # case 1
        tags = ["O", "O", "O", "O"]
        self.assertListEqual(utils.tags.iob1_to_iob2(tags), ["O", "O", "O", "O"])

        # case 2
        with self.assertRaises(ValueError):
            utils.tags.iob1_to_iob2(["B-PER", "I-PER", "I-PER", "O"])

        # case 3
        tags = ["I-PER", "I-PER", "I-PER", "O"]
        self.assertListEqual(utils.tags.iob1_to_iob2(tags), ["B-PER", "I-PER", "I-PER", "O"])

        # case 4
        tags = ["I-PER", "I-PER", "I-PER", "O", "I-ORG", "B-ORG", "B-ORG"]
        self.assertListEqual(
            utils.tags.iob1_to_iob2(tags),
            ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"]
        )

        # case 5
        tags = ["I-PER", "B-PER", "I-PER", "O"]
        self.assertListEqual(utils.tags.iob1_to_iob2(tags), ["B-PER", "B-PER", "I-PER", "O"])

        # case 6
        with self.assertRaises(ValueError):
            utils.tags.iob1_to_iob2(["I-PER", "B-PER", "I-PER", "O", "ORG"])

        # case 7
        tags = ["I-PER", "B-PER", "I-ORG", "O", "I-ORG"]
        self.assertListEqual(
            utils.tags.iob1_to_iob2(tags),
            ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"]
        )

    def test_iob_to_iobes(self):
        # case 1
        self.assertListEqual(
            utils.tags.iob_to_iobes(["B-PER", "I-PER", "I-PER", "O"]),
            ["B-PER", "I-PER", "E-PER", "O"]
        )

        # case 2
        self.assertListEqual(
            utils.tags.iob_to_iobes(["I-PER", "I-PER", "I-PER", "O"], version="IOB1"),
            ["B-PER", "I-PER", "E-PER", "O"]
        )

        with self.assertRaises(ValueError):
            utils.tags.iob_to_iobes(["I-PER", "I-PER", "I-PER", "O"])

        # case 3
        self.assertListEqual(
            utils.tags.iob_to_iobes(["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"]),
            ["B-PER", "I-PER", "E-PER", "O", "S-ORG", "S-ORG", "S-ORG"]
        )

        # case 4
        self.assertListEqual(
            utils.tags.iob_to_iobes(["B-PER", "B-PER", "B-ORG", "O", "B-ORG"]),
            ["S-PER", "S-PER", "S-ORG", "O", "S-ORG"]
        )

        # case 5
        with self.assertRaises(ValueError):
            utils.tags.iob_to_iobes(["I-PER", "B-PER", "I-ORG", "O", "ORG"])

        # case 6
        self.assertListEqual(
            utils.tags.iob_to_iobes(["B-PER", "I-PER", "O", "B-ORG"]),
            ["B-PER", "E-PER", "O", "S-ORG"]
        )

    def test_iobes_to_iob(self):
        # case 1
        self.assertListEqual(
            utils.tags.iobes_to_iob(["B-PER", "I-PER", "E-PER", "O"]),
            ["B-PER", "I-PER", "I-PER", "O"]
        )

        # case 2
        self.assertListEqual(
            utils.tags.iobes_to_iob(["B-PER", "I-PER", "E-PER", "O"]),
            ["B-PER", "I-PER", "I-PER", "O"]
        )

        # case 3
        self.assertListEqual(
            utils.tags.iobes_to_iob(["B-PER", "I-PER", "E-PER", "O", "S-ORG", "S-ORG", "S-ORG"]),
            ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"]
        )

        # case 4
        self.assertListEqual(
            utils.tags.iobes_to_iob(["S-PER", "S-PER", "S-ORG", "O", "S-ORG"]),
            ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"]
        )

        # case 5
        with self.assertRaises(Exception):
            print(utils.tags.iobes_to_iob(["I-PER", "B-PER", "I-ORG", "O", "B-ORG"]))

        # case 6
        self.assertListEqual(
            utils.tags.iobes_to_iob(["B-PER", "E-PER", "O", "S-ORG"]),
            ["B-PER", "I-PER", "O", "B-ORG"]
        )

        # case 7
        with self.assertRaises(ValueError):
            utils.tags.iobes_to_iob(["B-PER", "B-PER", "E-PER", "O"])

        # case 8
        with self.assertRaises(ValueError):
            utils.tags.iobes_to_iob(["B-PER", "I-PER", "I-PER", "O"])

        # case 9
        with self.assertRaises(ValueError):
            utils.tags.iobes_to_iob(["I-PER", "I-PER", "E-PER", "O"])

    def test_is_start_chunk(self):
        self.assertTrue(utils.tags.is_start_chunk("O", "B-PER"))
        self.assertTrue(utils.tags.is_start_chunk("O", "I-PER"))
        self.assertTrue(utils.tags.is_start_chunk("O", "E-PER"))
        self.assertTrue(utils.tags.is_start_chunk("O", "S-PER"))
        self.assertFalse(utils.tags.is_start_chunk("O", "O"))

        self.assertTrue(utils.tags.is_start_chunk("B-PER", "B-PER"))
        self.assertFalse(utils.tags.is_start_chunk("B-PER", "I-PER"))
        self.assertFalse(utils.tags.is_start_chunk("B-PER", "E-PER"))
        self.assertTrue(utils.tags.is_start_chunk("B-PER", "S-PER"))
        self.assertFalse(utils.tags.is_start_chunk("B-PER", "O"))

        self.assertTrue(utils.tags.is_start_chunk("I-PER", "B-PER"))
        self.assertFalse(utils.tags.is_start_chunk("I-PER", "I-PER"))
        self.assertFalse(utils.tags.is_start_chunk("I-PER", "E-PER"))
        self.assertTrue(utils.tags.is_start_chunk("I-PER", "S-PER"))
        self.assertFalse(utils.tags.is_start_chunk("I-PER", "O"))

        self.assertTrue(utils.tags.is_start_chunk("E-PER", "B-PER"))
        self.assertTrue(utils.tags.is_start_chunk("E-PER", "I-PER"))
        self.assertTrue(utils.tags.is_start_chunk("E-PER", "E-PER"))
        self.assertTrue(utils.tags.is_start_chunk("E-PER", "S-PER"))
        self.assertFalse(utils.tags.is_start_chunk("E-PER", "O"))

        self.assertTrue(utils.tags.is_start_chunk("S-PER", "B-PER"))
        self.assertTrue(utils.tags.is_start_chunk("S-PER", "I-PER"))
        self.assertTrue(utils.tags.is_start_chunk("S-PER", "E-PER"))
        self.assertTrue(utils.tags.is_start_chunk("S-PER", "S-PER"))
        self.assertFalse(utils.tags.is_start_chunk("S-PER", "O"))

    def test_is_end_chunk(self):
        self.assertFalse(utils.tags.is_end_chunk("O", "B-PER"))
        self.assertFalse(utils.tags.is_end_chunk("O", "I-PER"))
        self.assertFalse(utils.tags.is_end_chunk("O", "E-PER"))
        self.assertFalse(utils.tags.is_end_chunk("O", "S-PER"))
        self.assertFalse(utils.tags.is_end_chunk("O", "O"))

        self.assertTrue(utils.tags.is_end_chunk("B-PER", "B-PER"))
        self.assertFalse(utils.tags.is_end_chunk("B-PER", "I-PER"))
        self.assertFalse(utils.tags.is_end_chunk("B-PER", "E-PER"))
        self.assertTrue(utils.tags.is_end_chunk("B-PER", "S-PER"))
        self.assertTrue(utils.tags.is_end_chunk("B-PER", "O"))

        self.assertTrue(utils.tags.is_end_chunk("I-PER", "B-PER"))
        self.assertFalse(utils.tags.is_end_chunk("I-PER", "I-PER"))
        self.assertFalse(utils.tags.is_end_chunk("I-PER", "E-PER"))
        self.assertTrue(utils.tags.is_end_chunk("I-PER", "S-PER"))
        self.assertTrue(utils.tags.is_end_chunk("I-PER", "O"))

        self.assertTrue(utils.tags.is_end_chunk("E-PER", "B-PER"))
        self.assertTrue(utils.tags.is_end_chunk("E-PER", "I-PER"))
        self.assertTrue(utils.tags.is_end_chunk("E-PER", "E-PER"))
        self.assertTrue(utils.tags.is_end_chunk("E-PER", "S-PER"))
        self.assertTrue(utils.tags.is_end_chunk("E-PER", "O"))

        self.assertTrue(utils.tags.is_end_chunk("S-PER", "B-PER"))
        self.assertTrue(utils.tags.is_end_chunk("S-PER", "I-PER"))
        self.assertTrue(utils.tags.is_end_chunk("S-PER", "E-PER"))
        self.assertTrue(utils.tags.is_end_chunk("S-PER", "S-PER"))
        self.assertTrue(utils.tags.is_end_chunk("S-PER", "O"))

    def test_get_entities(self):
        tags = ['O', 'O', 'O', 'B-MISC', 'I-MISC', 'I-MISC', 'O', 'B-PER', 'I-PER', 'S-PER']
        entities = utils.tags.get_entities(tags)
        self.assertEqual(len(entities), 3)
        self.assertTupleEqual(entities[0], ('MISC', 3, 5))
        self.assertTupleEqual(entities[1], ('PER', 7, 8))
        self.assertTupleEqual(entities[2], ('PER', 9, 9))

        tags = ['O', 'O', 'I-MISC', 'E-MISC', 'B-MISC', 'O', 'B-PER', 'B-PER', 'E-PER']
        entities = utils.tags.get_entities(tags)
        self.assertEqual(len(entities), 4)
        self.assertTupleEqual(entities[0], ('MISC', 2, 3))
        self.assertTupleEqual(entities[1], ('MISC', 4, 4))
        self.assertTupleEqual(entities[2], ('PER', 6, 6))
        self.assertTupleEqual(entities[3], ('PER', 7, 8))

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
