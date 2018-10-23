from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import random
from jpyutils.mltools import utils

class TestTags(unittest.TestCase):
    def setUp(self):
        pass

    def test_iob1_to_iob2(self):
        # case 1
        tags = ["O", "O", "O", "O"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["O", "O", "O", "O"])

        # case 2
        tags = ["B-PER", "I-PER", "I-PER", "O"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["B-PER", "I-PER", "I-PER", "O"])

        # case 3
        tags = ["I-PER", "I-PER", "I-PER", "O"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["B-PER", "I-PER", "I-PER", "O"])

        # case 4
        tags = ["I-PER", "I-PER", "I-PER", "O", "I-ORG", "B-ORG", "B-ORG"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["B-PER", "I-PER", "I-PER", "O", "B-ORG", "B-ORG", "B-ORG"])

        # case 5
        tags = ["I-PER", "B-PER", "I-PER", "O"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["B-PER", "B-PER", "I-PER", "O"])

        # case 6
        tags = ["I-PER", "B-PER", "I-PER", "O", "ORG"]
        self.assertFalse(utils.tags.iob1_to_iob2(tags))

        # case 7
        tags = ["I-PER", "B-PER", "I-ORG", "O", "I-ORG"]
        self.assertTrue(utils.tags.iob1_to_iob2(tags))
        self.assertListEqual(tags, ["B-PER", "B-PER", "B-ORG", "O", "B-ORG"])

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
