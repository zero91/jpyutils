from lanfang.ai.dataset.texts import snli
from lanfang.ai import names

import unittest
import os
import tensorflow as tf
import logging
logging.basicConfig(level=logging.INFO)


class TestSNLI(unittest.TestCase):
  def setUp(self):
    self._m_snli = snli.SNLI()

  def tearDown(self):
    pass

  def test_parameters(self):
    self.assertEqual(len(self._m_snli.parameters()), 5)

  def test_artifacts(self):
    artifacts = self._m_snli.artifacts()
    self.assertEqual(len(self._m_snli.artifacts()), 2)
    self.assertTrue(names.Dictionary.VOCAB_FILE in artifacts)
    self.assertTrue(names.Dictionary.LABEL_FILE in artifacts)

  def test_prepare(self):
    save_dir = self._m_snli.prepare()
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "vocab.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "label.vocab.txt")))

    for split in ["train", "dev", "test"]:
      self.assertTrue(os.path.isdir(os.path.join(save_dir, split)))
      self.assertTrue(os.path.isfile(os.path.join(save_dir, split, "sent1.txt")))
      self.assertTrue(os.path.isfile(os.path.join(save_dir, split, "sent2.txt")))
      self.assertTrue(os.path.isfile(os.path.join(save_dir, split, "label.txt")))

  def test_paddings(self):
    self.assertIsNotNone(self._m_snli.paddings())

  def test_read_parse(self):
    train_data = self._m_snli.read(
        names.DataSplit.TRAIN, tf.estimator.ModeKeys.TRAIN)

    for i, (sent1, sent2, label) in enumerate(train_data):
      if i >= 10:
        break
      self.assertEqual(len(sent1.shape), 1)
      self.assertEqual(len(sent2.shape), 1)
      self.assertEqual(label.shape, ())
      self.assertEqual(sent1.dtype, tf.int64)
      self.assertEqual(sent2.dtype, tf.int64)
      self.assertEqual(label.dtype, tf.int64)
      features, labels = self._m_snli.parse(
          tf.estimator.ModeKeys.TRAIN, sent1, sent2, label)
      parse_sent_1 = features[names.Text.SENTENCE_A]
      parse_sent_2 = features[names.Text.SENTENCE_B]
      parse_label = labels[names.Classification.LABEL]
      self.assertEqual(parse_sent_1.shape, sent1.shape)
      self.assertEqual(parse_sent_2.shape, sent2.shape)
      self.assertEqual(parse_label.shape, ())
      self.assertEqual(parse_sent_1.dtype, tf.int32)
      self.assertEqual(parse_sent_2.dtype, tf.int32)
      self.assertEqual(parse_label.dtype, tf.int32)

  def test_data_fn(self):
    train_fn = self._m_snli.data_fn(names.DataSplit.TRAIN, batch_size=32)
    train_data = train_fn(tf.estimator.ModeKeys.EVAL, config=None)

    if tf.executing_eagerly():
      for features, labels in train_data.take(10):
        self.assertListEqual(
            features[names.Text.SENTENCE_A].shape.as_list()[:-1], [32])
        self.assertListEqual(
            features[names.Text.SENTENCE_B].shape.as_list()[:-1], [32])
        self.assertListEqual(
            labels[names.Classification.LABEL].shape.as_list(), [32])
