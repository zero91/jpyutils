from lanfang.ai.dataset.images import cifar
from lanfang.ai.engine import names

import unittest
import os
import tensorflow as tf
import logging
logging.basicConfig(level=logging.INFO)


class TestCifar10(unittest.TestCase):
  def setUp(self):
    self._m_cifar10 = cifar.Cifar10()

  def tearDown(self):
    pass

  def test_parameters(self):
    self.assertEqual(len(self._m_cifar10.parameters()), 0)
    self.assertEqual(len(self._m_cifar10.meta()), 4)

  def test_artifacts(self):
    self.assertDictEqual(self._m_cifar10.artifacts(), {})

  def test_preparse(self):
    save_dir = self._m_cifar10.prepare()

    self.assertTrue(os.path.isfile(
        os.path.join(save_dir, "cifar-10-python.tar.gz")))

    self.assertTrue(os.path.isdir(
        os.path.join(save_dir, "cifar-10-batches-py")))

  def test_paddings(self):
    self.assertIsNone(self._m_cifar10.paddings())

  def test_read_parse(self):
    train_data = self._m_cifar10.read(
        names.DataSplit.TRAIN, tf.estimator.ModeKeys.TRAIN)
    for i, (image, label) in enumerate(train_data):
      if i >= 10:
        break
      self.assertEqual(image.shape, (32, 32, 3))
      self.assertEqual(label.shape, ())
      self.assertEqual(image.dtype, tf.uint8)
      self.assertEqual(label.dtype, tf.uint8)
      features, labels = self._m_cifar10.parse(
          tf.estimator.ModeKeys.TRAIN, image, label)
      parse_image = features[names.Image.IMAGE]
      parse_label = labels[names.Classification.LABEL]
      self.assertEqual(parse_image.shape, (32, 32, 3))
      self.assertEqual(parse_label.shape, ())
      self.assertEqual(parse_image.dtype, tf.float32)
      self.assertEqual(parse_label.dtype, tf.int32)


class TestCifar100(unittest.TestCase):
  def setUp(self):
    self._m_cifar100 = cifar.Cifar100()

  def tearDown(self):
    pass

  def test_parameters(self):
    self.assertEqual(len(self._m_cifar100.parameters()), 0)
    self.assertEqual(len(self._m_cifar100.meta()), 4)
    self.assertEqual(len(self._m_cifar100.default_parameters()), 0)

  def test_artifacts(self):
    self.assertDictEqual(self._m_cifar100.artifacts(), {})

  def test_preparse(self):
    save_dir = self._m_cifar100.prepare()

    self.assertTrue(os.path.isfile(
        os.path.join(save_dir, "cifar-100-python.tar.gz")))

    self.assertTrue(os.path.isdir(
        os.path.join(save_dir, "cifar-100-python")))

  def test_paddings(self):
    self.assertIsNone(self._m_cifar100.paddings())

  def test_read_parse(self):
    train_data = self._m_cifar100.read(
        names.DataSplit.TRAIN, tf.estimator.ModeKeys.TRAIN)
    for i, (image, label) in enumerate(train_data):
      if i >= 10:
        break
      self.assertEqual(image.shape, (32, 32, 3))
      self.assertEqual(label.shape, ())
      self.assertEqual(image.dtype, tf.uint8)
      self.assertEqual(label.dtype, tf.uint8)
      features, labels = self._m_cifar100.parse(
          tf.estimator.ModeKeys.TRAIN, image, label)
      parse_image = features[names.Image.IMAGE]
      parse_label = labels[names.Classification.LABEL]
      self.assertEqual(parse_image.shape, (32, 32, 3))
      self.assertEqual(parse_label.shape, ())
      self.assertEqual(parse_image.dtype, tf.float32)
      self.assertEqual(parse_label.dtype, tf.int32)
