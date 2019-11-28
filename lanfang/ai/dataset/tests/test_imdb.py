from lanfang.ai.dataset import imdb
from lanfang.ai.engine import names

import unittest
import os
import tensorflow as tf


class TestIMDB(unittest.TestCase):
  def setUp(self):
    self._m_imdb = imdb.IMDB()

  def tearDown(self):
    pass

  def test_get_params(self):
    self.assertEqual(len(self._m_imdb.get_params()), 5);

  def test_artifacts(self):
    artifacts = self._m_imdb.artifacts()
    self.assertEqual(len(self._m_imdb.artifacts()), 1)
    self.assertTrue(names.Dictionary.VOCAB_FILE in artifacts)

  def test_preparse(self):
    save_dir = self._m_imdb.prepare()
    self.assertTrue(os.path.isdir(os.path.join(save_dir, "train")))
    self.assertTrue(os.path.isdir(os.path.join(save_dir, "dev")))
    self.assertTrue(os.path.isdir(os.path.join(save_dir, "test")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "vocab.txt")))

  def test_get_padding(self):
    self.assertIsNotNone(self._m_imdb.get_padding())

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
