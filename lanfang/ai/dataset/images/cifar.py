from lanfang.ai.engine.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.utils import disk

import urllib
import os
import pickle
import tarfile
import logging
import numpy as np
import tensorflow as tf


class Cifar10(Dataset):
  """The CIFAR-10 dataset consists of 60000 32x32 color images in 10 classes,
  with 6000 images per class.  There are 50000 training images and 10000 test
  images.

  The dataset is divided into five training batches and one test batch, each
  with 10000 images. The test batch contains exactly 1000 randomly-selected
  images from each class. The training batches contain the remaining images
  in random order, but some training batches may contain more images from
  one class than another. Between them, the training batches contain exactly
  5000 images from each class.

  Official Link: https://www.cs.toronto.edu/~kriz/cifar.html
  """

  __data_url__ = "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz"
  __data_md5__ = "c58f30108f718f92721af3b95e74349a"

  def __init__(self, local_dir="~/.lanfang/ai/dataset/images/cifar10",
                     **kwargs):
    """Initialize this class."""
    self._m_local_dir = os.path.expanduser(local_dir)
    self._m_dev_size = 5000
    self._m_seed = 9507

  @staticmethod
  def name():
    return "cifar10"

  @staticmethod
  def default_parameters():
    return {}

  def parameters(self):
    return {}

  def meta(self):
    return {
      names.Image.HEIGHT: 32,
      names.Image.WIDTH: 32,
      names.Image.CHANNEL: 3,
      names.Classification.NUM_CLASSES: 10
    }

  def artifacts(self):
    """Return artifacts of this dataset."""
    return {}

  def prepare(self):
    """This function will be called once to prepare the dataset."""
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    data_fname = os.path.basename(self.__class__.__data_url__)
    local_fname = os.path.join(self._m_local_dir, data_fname)
    if os.path.exists(local_fname) and \
        disk.md5(local_fname) == self.__class__.__data_md5__:
      return self._m_local_dir

    logging.info("Downloading...")
    urllib.request.urlretrieve(self.__class__.__data_url__, local_fname)

    logging.info("Extracting files...")
    tar = tarfile.open(local_fname)
    tar.extractall(self._m_local_dir)
    tar.close()
    return self._m_local_dir

  def read(self, split, mode):
    """Create an instance of the dataset object."""
    if split in [names.DataSplit.TRAIN, names.DataSplit.DEV]:
      batches = ["data_batch_%d" % (i + 1) for i in range(5)]
    elif split == names.DataSplit.TEST:
      batches = ["test_batch"]
    else:
      raise ValueError("Invalid split value '%s'" % (split))

    meta = self.meta()

    all_images = []
    all_labels = []
    for batch_fname in batches:
      data_fname = os.path.join(
          self._m_local_dir, "cifar-10-batches-py", batch_fname)

      with open(data_fname, 'rb') as fin:
        d = pickle.load(fin, encoding='bytes')
        images = np.array(d[b"data"], dtype=np.uint8)
        labels = np.array(d[b"labels"], dtype=np.uint8)

        image_height = meta[names.Image.HEIGHT]
        image_width = meta[names.Image.WIDTH]
        channels = meta[names.Image.CHANNEL]
        images = np.reshape(
            images, [images.shape[0], channels, image_height, image_width])
        images = np.transpose(images, [0, 2, 3, 1])
        logging.info("Loaded %d examples.", images.shape[0])

        all_images.append(images)
        all_labels.append(labels)

    all_images = np.concatenate(all_images)
    all_labels = np.concatenate(all_labels)
    if split in [names.DataSplit.TRAIN, names.DataSplit.DEV]:
      np.random.seed(self._m_seed)
      sample_idx = np.arange(0, all_labels.shape[0])
      np.random.shuffle(sample_idx)
      if split == names.DataSplit.TRAIN:
        all_images = all_images[sample_idx[self._m_dev_size:]]
        all_labels = all_labels[sample_idx[self._m_dev_size:]]
      else:
        all_images = all_images[sample_idx[:self._m_dev_size]]
        all_labels = all_labels[sample_idx[:self._m_dev_size]]

    return tf.data.Dataset.from_tensor_slices((all_images, all_labels))

  def parse(self, mode, image, label):
    """Parse input record to features and labels."""
    image = tf.cast(image, tf.float32)
    label = tf.cast(label, tf.int32)

    if mode == tf.estimator.ModeKeys.TRAIN:
      meta = self.meta()
      height = meta[names.Image.HEIGHT]
      width = meta[names.Image.WIDTH]
      channels = meta[names.Image.CHANNEL]
      image = tf.image.resize_with_crop_or_pad(image, height + 4, width + 4)
      image = tf.image.random_crop(image, [height, width, channels])
      image = tf.image.random_flip_left_right(image)

    image = tf.image.per_image_standardization(image)
    return {names.Image.IMAGE: image}, {names.Classification.LABEL: label}

  def paddings(self):
    return None


class Cifar100(Cifar10):
  """This dataset is just like the CIFAR-10, except it has 100 classes
  containing 600 images each. There are 500 training images and 100 testing
  images per class. The 100 classes in the CIFAR-100 are grouped into 20
  superclasses. Each image comes with a "fine" label (the class to which
  it belongs) and a "coarse" label (the superclass to which it belongs).

  Official Link: https://www.cs.toronto.edu/~kriz/cifar.html
  """

  __data_url__ = "https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz"
  __data_md5__ = "eb9058c3a382ffc7106e4002c42a8d85"

  def __init__(self, local_dir="~/.lanfang/ai/dataset/images/cifar100",
                     **kwargs):
    """Initialize this class."""
    super(self.__class__, self).__init__(local_dir, **kwargs)

  @staticmethod
  def name():
    return "cifar100"

  def meta(self):
    return {
      names.Image.HEIGHT: 32,
      names.Image.WIDTH: 32,
      names.Image.CHANNEL: 3,
      names.Classification.NUM_CLASSES: 100
    }

  def read(self, split, mode):
    """Create an instance of the dataset object."""
    if split in [names.DataSplit.TRAIN, names.DataSplit.DEV]:
      batches = ["train"]
    elif split == names.DataSplit.TEST:
      batches = ["test"]
    else:
      raise ValueError("Invalid split value '%s'" % (split))

    meta = self.meta()

    all_images = []
    all_labels = []
    for batch_fname in batches:
      data_fname = os.path.join(
          self._m_local_dir, "cifar-100-python", batch_fname)

      with open(data_fname, 'rb') as fin:
        d = pickle.load(fin, encoding='bytes')
        images = np.array(d[b"data"], dtype=np.uint8)
        labels = np.array(d[b"fine_labels"], dtype=np.uint8)

        image_height = meta[names.Image.HEIGHT]
        image_width = meta[names.Image.WIDTH]
        channels = meta[names.Image.CHANNEL]
        images = np.reshape(
            images, [images.shape[0], channels, image_height, image_width])
        images = np.transpose(images, [0, 2, 3, 1])
        logging.info("Loaded %d examples.", images.shape[0])

        all_images.append(images)
        all_labels.append(labels)

    all_images = np.concatenate(all_images)
    all_labels = np.concatenate(all_labels)
    if split in [names.DataSplit.TRAIN, names.DataSplit.DEV]:
      np.random.seed(self._m_seed)
      sample_idx = np.arange(0, all_labels.shape[0])
      np.random.shuffle(sample_idx)
      if split == names.DataSplit.TRAIN:
        all_images = all_images[sample_idx[self._m_dev_size:]]
        all_labels = all_labels[sample_idx[self._m_dev_size:]]
      else:
        all_images = all_images[sample_idx[:self._m_dev_size]]
        all_labels = all_labels[sample_idx[:self._m_dev_size]]

    return tf.data.Dataset.from_tensor_slices((all_images, all_labels))
