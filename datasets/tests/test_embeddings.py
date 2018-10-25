from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import logging
import tempfile
import os
import numpy as np
from jpyutils import datasets
from jpyutils import utils

class TestEmbeddings(unittest.TestCase):
    def setUp(self):
        utils.utilities.get_logger()
        self.__p_embeddings = datasets.Embeddings()

    def test_load(self):
        with self.assertRaises(KeyError):
            self.__p_embeddings.load("test", 300)

        word2id, id2word, word_embeddings_1 = self.__p_embeddings.load("glove.6B", 50)
        self.assertEqual(word_embeddings_1.shape[0], len(word2id))
        self.assertEqual(word_embeddings_1.shape[1], 50)

        word2id, id2word, word_embeddings_2 = self.__p_embeddings.load(
                "glove.6B", dim=50, extra_dict={"<BEG>": 0, "<END>": 1, "<UNK>": 2})
        self.assertEqual(word_embeddings_2.shape[0], len(word2id))
        self.assertEqual(word_embeddings_2.shape[1], 50)
        self.assertEqual(word_embeddings_1.shape[0] + 3, word_embeddings_2.shape[0])

    def test_generate(self):
        random_embeddings = self.__p_embeddings.generate((3, 50))
        self.assertEqual(random_embeddings.shape, (3, 50))
        norm = np.linalg.norm(random_embeddings, axis=1)
        self.assertEqual(norm.shape, (3,))
        for elem in norm:
            self.assertAlmostEqual(elem, 1.0)

    def test_add_data(self):
        self.assertNotIn("test", self.__p_embeddings._m_dataset_conf)
        self.__p_embeddings.add_data("test", "http://jpyutils.demo.com/xx", {300: ".*"})
        self.assertIn("test", self.__p_embeddings._m_dataset_conf)

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
