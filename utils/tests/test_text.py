from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import tensorflow as tf
from jpyutils import utils

class TestRandom(unittest.TestCase):
    def setUp(self):
        pass

    def test_text2array(self):
        sentences = [["hello", "world"], ["It", "is", "raining", "outside"]]
        word2id = { "hello": 4, "world": 8, "It": 7, "is": 5, "outside": 6}

        # default usage
        array_1, sizes_1 = utils.text.text2array(sentences, word2id)
        self.assertEqual(array_1.shape, (2, 6))
        self.assertListEqual(array_1[0].tolist(), [0, 4, 8, 1, 3, 3])
        self.assertListEqual(array_1[1].tolist(), [0, 7, 5, 2, 6, 1])
        self.assertEqual(sizes_1.shape, (2,))
        self.assertListEqual(sizes_1.tolist(), [4, 6])

        # some sentences' length is longer than 'maxlen'
        array_2, sizes_2 = utils.text.text2array(sentences, word2id, maxlen=3 + 2)
        self.assertEqual(array_2.shape, (2, 5))
        self.assertListEqual(array_2[0].tolist(), [0, 4, 8, 1, 3])
        self.assertListEqual(array_2[1].tolist(), [0, 7, 5, 2, 1])
        self.assertEqual(sizes_2.shape, (2,))
        self.assertListEqual(sizes_2.tolist(), [4, 5])

        # some sentences' length is much longer than 'maxlen'
        array_3, sizes_3 = utils.text.text2array(sentences, word2id, maxlen=1 + 2)
        self.assertEqual(array_3.shape, (2, 3))
        self.assertListEqual(array_3[0].tolist(), [0, 4, 1])
        self.assertListEqual(array_3[1].tolist(), [0, 7, 1])
        self.assertEqual(sizes_3.shape, (2,))
        self.assertListEqual(sizes_3.tolist(), [3, 3])

        # all sentences' length is is shorter than 'maxlen'
        array_4, sizes_4 = utils.text.text2array(sentences, word2id, maxlen=8 + 2)
        self.assertEqual(array_4.shape, (2, 10))
        self.assertListEqual(array_4[0].tolist(), [0, 4, 8, 1, 3, 3, 3, 3, 3, 3])
        self.assertListEqual(array_4[1].tolist(), [0, 7, 5, 2, 6, 1, 3, 3, 3, 3])
        self.assertEqual(sizes_4.shape, (2,))
        self.assertListEqual(sizes_4.tolist(), [4, 6])

        # do not use 'beg' sentry
        array_5, sizes_5 = utils.text.text2array(sentences, word2id, maxlen=5 + 2, beg=None)
        self.assertEqual(array_5.shape, (2, 7))
        self.assertListEqual(array_5[0].tolist(), [4, 8, 1, 3, 3, 3, 3])
        self.assertListEqual(array_5[1].tolist(), [7, 5, 2, 6, 1, 3, 3])
        self.assertEqual(sizes_5.shape, (2,))
        self.assertListEqual(sizes_5.tolist(), [3, 5])

        # skip unknown tokens.
        array_6, sizes_6 = utils.text.text2array(sentences, word2id, maxlen=5 + 2, unknown=None)
        self.assertEqual(array_6.shape, (2, 7))
        self.assertListEqual(array_6[0].tolist(), [0, 4, 8, 1, 3, 3, 3])
        self.assertListEqual(array_6[1].tolist(), [0, 7, 5, 6, 1, 3, 3])
        self.assertEqual(sizes_6.shape, (2,))
        self.assertListEqual(sizes_6.tolist(), [4, 5])

    def test_clip_sentence(self):
        tf.InteractiveSession()
        sentences = tf.constant([[0, 3, 2, 1, 1], [0, 4, 5, 2, 1]])

        sizes_1 = tf.constant([1, 1])
        clipped_sents1 = utils.text.clip_sentence(sentences, sizes_1)
        self.assertListEqual(clipped_sents1.eval().tolist(), [[0], [0]])

        sizes_2 = tf.constant([1, 2])
        clipped_sents2 = utils.text.clip_sentence(sentences, sizes_2)
        self.assertListEqual(clipped_sents2.eval().tolist(), [[0, 3], [0, 4]])

        sizes_3 = tf.constant([4, 2])
        clipped_sents3 = utils.text.clip_sentence(sentences, sizes_3)
        self.assertListEqual(clipped_sents3.eval().tolist(), [[0, 3, 2, 1], [0, 4, 5, 2]])

    def test_mask3d(self):
        tf.InteractiveSession()
        sentences = tf.constant([[[0, 3, 2], [0, 4, 5]], [[2, 1, 5], [3, 4, 5]]], dtype=tf.float32)
        sentence_sizes = tf.constant([1, 2])

        masked1 = utils.text.mask3d(sentences, sentence_sizes, 99).eval()
        self.assertListEqual(tf.shape(sentences).eval().tolist(), tf.shape(masked1).eval().tolist())
        self.assertEqual(masked1.tolist(), [[[0, 99, 99], [0, 99, 99]], [[2, 1, 99],[3, 4, 99]]])


    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
