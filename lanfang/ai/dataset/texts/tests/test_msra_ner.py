import unittest
import os
import logging
logging.basicConfig(level=logging.INFO)

from lanfang.ai.dataset.texts.msra_ner import MSRA_NER
from lanfang.ai import names
import tensorflow as tf


class TestMSRA_NER(unittest.TestCase):
  def setUp(self):
    self._m_msra_ner = MSRA_NER()
    self._m_msra_ner.prepare()

  def tearDown(self):
    pass

  def test_parameters(self):
    self.assertEqual(len(self._m_msra_ner.parameters()), 6)
    self.assertEqual(len(self._m_msra_ner.default_parameters()), 6)

  def test_meta(self):
    self.assertEqual(len(self._m_msra_ner.meta()), 2)

  def test_artifacts(self):
    artifacts = self._m_msra_ner.artifacts()
    self.assertEqual(len(self._m_msra_ner.artifacts()), 3)
    self.assertTrue(names.Dictionary.VOCAB_FILE in artifacts)
    self.assertTrue(names.Dictionary.LABEL_FILE in artifacts)
    self.assertTrue(names.Dictionary.WORD_SEG_VOCAB_FILE in artifacts)

  def test_prepare(self):
    save_dir = self._m_msra_ner.prepare()
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "vocab.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "entity.vocab.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "seg.vocab.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "train.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "dev.txt")))
    self.assertTrue(os.path.isfile(os.path.join(save_dir, "test.txt")))

  def test_paddings(self):
    self.assertIsNotNone(self._m_msra_ner.paddings())

  def test_read_parse(self):
    train_data = self._m_msra_ner.read(
        names.DataSplit.TRAIN, tf.estimator.ModeKeys.TRAIN)

    for i, (tokens, entity_tags,
            seg_tags, sentence_length) in enumerate(train_data):
      if i >= 10:
        break
      self.assertEqual(len(tokens.shape), 1)
      self.assertEqual(len(entity_tags.shape), 1)
      self.assertEqual(len(seg_tags.shape), 1)
      self.assertEqual(len(sentence_length.shape), 0)

      self.assertEqual(tokens.dtype, tf.int64)
      self.assertEqual(entity_tags.dtype, tf.int64)
      self.assertEqual(seg_tags.dtype, tf.int64)
      self.assertEqual(sentence_length.dtype, tf.int32)

      features, labels = self._m_msra_ner.parse(
          tf.estimator.ModeKeys.TRAIN,
          tokens, entity_tags, seg_tags, sentence_length)

      sent = features[names.Text.SENTENCE]
      word_seg_tags = features[names.Text.WORD_SEG_TAGS]
      sentence_length = features[names.Text.SENTENCE_LENGTH]
      label = labels[names.Classification.LABEL]

      self.assertListEqual(sent.shape.as_list(), word_seg_tags.shape.as_list())
      self.assertListEqual(sent.shape.as_list(), label.shape.as_list())
      self.assertEqual(sent.shape[0], sentence_length.numpy())

      self.assertEqual(sent.dtype, tf.int32)
      self.assertEqual(word_seg_tags.dtype, tf.int32)
      self.assertEqual(sentence_length.dtype, tf.int32)
      self.assertEqual(label.dtype, tf.int32)

  def test_data_fn(self):
    train_fn = self._m_msra_ner.data_fn(names.DataSplit.TRAIN, batch_size=32)
    train_data = train_fn(tf.estimator.ModeKeys.EVAL, config=None)

    if tf.executing_eagerly():
      for features, labels in train_data.take(10):
        length = max(features[names.Text.SENTENCE_LENGTH])
        self.assertListEqual(
            features[names.Text.SENTENCE].shape.as_list(), [32, length])
        self.assertListEqual(
            features[names.Text.WORD_SEG_TAGS].shape.as_list(), [32, length])
        self.assertListEqual(
            labels[names.Classification.LABEL].shape.as_list(), [32, length])
