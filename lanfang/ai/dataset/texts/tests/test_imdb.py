from lanfang.ai.dataset.texts import imdb
from lanfang.ai import names

import unittest
import os
import tensorflow as tf
import logging
logging.basicConfig(level=logging.INFO)


class TestIMDB(unittest.TestCase):
  def setUp(self):
    self._m_imdb = imdb.IMDB()

  def tearDown(self):
    pass

  def test_parameters(self):
    self.assertEqual(len(self._m_imdb.parameters()), 6)

  def test_artifacts(self):
    artifacts = self._m_imdb.artifacts()
    self.assertEqual(len(self._m_imdb.artifacts()), 1)
    self.assertTrue(names.Dictionary.VOCAB_FILE in artifacts)

  def test_preparse(self):
    save_dir = self._m_imdb.prepare()
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "train.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "dev.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "test.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "vocab.txt")))

  def test_paddings(self):
    self.assertIsNotNone(self._m_imdb.paddings())

  def test_read_parse(self):
    train_data = self._m_imdb.read(
        names.DataSplit.TRAIN, tf.estimator.ModeKeys.TRAIN)

    for i, (sent, label) in enumerate(train_data):
      if i >= 10:
        break
      self.assertEqual(len(sent.shape), 1)
      self.assertEqual(label.shape, ())
      self.assertEqual(sent.dtype, tf.int64)
      self.assertEqual(label.dtype, tf.int32)
      features, labels = self._m_imdb.parse(
          tf.estimator.ModeKeys.TRAIN, sent, label)
      parse_sent = features[names.Text.SENTENCE]
      parse_label = labels[names.Classification.LABEL]
      self.assertEqual(parse_sent.shape, sent.shape)
      self.assertEqual(parse_label.shape, ())
      self.assertEqual(parse_sent.dtype, tf.int32)
      self.assertEqual(parse_label.dtype, tf.int32)

  def test_data_fn(self):
    train_fn = self._m_imdb.data_fn(names.DataSplit.TRAIN, batch_size=32)
    train_data = train_fn(tf.estimator.ModeKeys.EVAL, config=None)

    if tf.executing_eagerly():
      for features, labels in train_data.take(10):
        self.assertListEqual(
            features[names.Text.SENTENCE].shape.as_list(), [32, 256])
        self.assertListEqual(
            labels[names.Classification.LABEL].shape.as_list(), [32])
