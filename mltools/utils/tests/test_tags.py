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
        self.assertListEqual(utils.tags.iob1_to_iob2(tags),
                             ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"])

        # case 5
        tags = ["I-PER", "B-PER", "I-PER", "O"]
        self.assertListEqual(utils.tags.iob1_to_iob2(tags), ["B-PER", "B-PER", "I-PER", "O"])

        # case 6
        with self.assertRaises(ValueError):
            utils.tags.iob1_to_iob2(["I-PER", "B-PER", "I-PER", "O", "ORG"])

        # case 7
        tags = ["I-PER", "B-PER", "I-ORG", "O", "I-ORG"]
        self.assertListEqual(utils.tags.iob1_to_iob2(tags),
                             ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"])

    def test_iob_to_iobes(self):
        # case 1
        self.assertListEqual(utils.tags.iob_to_iobes(["B-PER", "I-PER", "I-PER", "O"]),
                                                     ["B-PER", "I-PER", "E-PER", "O"])

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
        self.assertListEqual(utils.tags.iobes_to_iob(["B-PER", "I-PER", "E-PER", "O"]),
                                                     ["B-PER", "I-PER", "I-PER", "O"])

        # case 2
        self.assertListEqual(utils.tags.iobes_to_iob(["B-PER", "I-PER", "E-PER", "O"]),
                                                     ["B-PER", "I-PER", "I-PER", "O"])

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

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
