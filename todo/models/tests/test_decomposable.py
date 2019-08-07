from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import unittest
import logging
import tempfile
import os
import yaml
import numpy as np
import tensorflow as tf
from jpyutils import utils
from jpyutils.models import decomposable
from jpyutils.models import Embeddings

class TestDecomposableNLIModel(unittest.TestCase, decomposable.DecomposableNLIModel):
    def setUp(self):
        embeddings_fname = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                        "embeddings.txt")
        self.__word2id, self.__word_embeddings = Embeddings().load(
                                None, 50, resource_info={"local": embeddings_fname}, id_shift=4)
        self.__word2id.update({"beg": 0, "end": 1, "unknown": 2, "padding": 3})

        self.__text_array_1, self.__text_sizes_1 = utils.text.text2array(
                    [["hello", "world"], ["nice", "to", "meet", "you"]], self.__word2id)
        self.__text_array_2, self.__text_sizes_2 = utils.text.text2array(
                    [["hello", "nice", "world"], ["love", "to", "you"]], self.__word2id)

    def test_build_model(self):
        utils.utilities.get_logger()
        sess = tf.InteractiveSession()
        vocab_size = len(self.__word2id)
        print(vocab_size)
        embedding_size = self.__word_embeddings.shape[1]
        hidden_size = 5

        #model = decomposable.DecomposableNLIModel(2, vocab_size, embedding_size, hidden_size)
        with open('config.yaml') as f:
            conf = yaml.safe_load(f)
            model = decomposable.DecomposableNLIModel(conf)

        model.initialize(sess, self.__word_embeddings)

        feed_dict = {
                model._sentence1: self.__text_array_1,
                model._sentence1_size: self.__text_sizes_1,
                model._sentence2: self.__text_array_2,
                model._sentence2_size: self.__text_sizes_2,
                model._ph_embeddings: self.__word_embeddings,
                model._dropout_rate: 1.0,
                model._label: [0, 1],
                model._l2_constant: 2.0,
        }

        projected1 = sess.run(model._projected1,
                              feed_dict={model._sentence1: self.__text_array_1,
                                         model._sentence1_size: self.__text_sizes_1,
                                         model._ph_embeddings: self.__word_embeddings})

        alpha, beta, att1, att2 = sess.run(
                            [model._alpha, model._beta, model._att_sent1, model._att_sent2],
                            feed_dict=feed_dict)

        v1, v2 = sess.run([model._v1, model._v2], feed_dict=feed_dict)

        logits = sess.run(model._logits, feed_dict=feed_dict)
        answer = sess.run(model._answer, feed_dict=feed_dict)

        accuracy = sess.run(model._accuracy, feed_dict=feed_dict)
        cross_entropy = sess.run(model._cross_entropy, feed_dict=feed_dict)
        loss = sess.run(model._loss, feed_dict=feed_dict)
        print(loss)
        print(loss.shape)

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()
