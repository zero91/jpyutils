from lanfang.ai.engine.base.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.utils import disk

import gzip
import os
import urllib.request
import struct
import logging
import numpy as np
import tensorflow as tf


class Mnist(Dataset):
  def __init__(self, local_dir="~/.lanfang/ai/dataset/mnist", **params):
    super(self.__class__, self).__init__()

    self._m_remote_url = "http://yann.lecun.com/exdb/mnist/"
    self._m_local_dir = os.path.expanduser(local_dir)

    self._m_train_image_fname = "train-images-idx3-ubyte.gz"
    self._m_train_label_fname = "train-labels-idx1-ubyte.gz"
    self._m_test_image_fname = "t10k-images-idx3-ubyte.gz"
    self._m_test_label_fname = "t10k-labels-idx1-ubyte.gz"
    self._m_train_part_num = 50000 # split train data into train/dev

    self._m_files_md5 = {
      "t10k-images-idx3-ubyte.gz": "9fb629c4189551a2d022fa330f9573f3",
      "t10k-labels-idx1-ubyte.gz": "ec29112dd5afa0611ce80d1b7f02629c",
      "train-images-idx3-ubyte.gz": "f68b3c2dcbeaaa9fbdd348bbdeb94873",
      "train-labels-idx1-ubyte.gz": "d53e105ee54ea40749a09fcbcd1e9432",
    }

  def name():
    return "mnist"

  def get_params(self):
    return {
      names.Image.HEIGHT: 28,
      names.Image.WIDTH: 28,
      names.Image.CHANNEL: 1,
      names.Classification.NUM_CLASSES: 10
    }

  def artifacts(self):
    """Return artifacts of this dataset."""
    return super(self.__class__, self).artifacts()

  def prepare(self):
    """This function will be called once to prepare the dataset."""
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    for fname, md5_value in self._m_files_md5.items():
      local_fname = os.path.join(self._m_local_dir, fname)
      remote_fname = os.path.join(self._m_remote_url, fname)
      if os.path.exists(local_fname):
        if disk.md5(local_fname) == md5_value:
          continue
      logging.info("Download file from %s.", remote_fname)
      urllib.request.urlretrieve(remote_fname, local_fname)
    return self._m_local_dir

  def get_padding(self):
    """Padded shapes and padding values for batch data"""
    return super(self.__class__, self).get_padding()

  def read(self, split, mode):
    """Create an instance of the dataset object."""
    if split in [names.DataSplit.TRAIN, names.DataSplit.DEV]:
      image_fname = self._m_train_image_fname
      label_fname = self._m_train_label_fname
    elif split == names.DataSplit.TEST:
      image_fname = self._m_test_image_fname
      label_fname = self._m_test_label_fname
    else:
      raise ValueError("Invalid split value '%s'" % (split))

    image_fname = os.path.join(self._m_local_dir, image_fname)
    label_fname = os.path.join(self._m_local_dir, label_fname)

    with gzip.open(image_fname, "rb") as f:
      _, num, rows, cols = struct.unpack(">IIII", f.read(16))
      images = np.frombuffer(f.read(num * rows * cols), dtype=np.uint8)
      images = np.reshape(images, [num, rows, cols, 1])
      logging.info("Loaded %d images of size [%d, %d].", num, rows, cols)

    with gzip.open(label_fname, "rb") as f:
      _, num = struct.unpack(">II", f.read(8))
      labels = np.frombuffer(f.read(num), dtype=np.uint8)
      logging.info("Loaded %d labels.", num)

    if split == names.DataSplit.TRAIN:
      images = images[:self._m_train_part_num]
      labels = labels[:self._m_train_part_num]
    elif split == names.DataSplit.DEV:
      images = images[self._m_train_part_num:]
      labels = labels[self._m_train_part_num:]

    return tf.data.Dataset.from_tensor_slices((images, labels))

  def parse(self, mode, image, label):
    """Parse input into features and labels."""
    image = tf.cast(image, tf.float32)
    label = tf.cast(label, tf.int32)
    return {names.Image.IMAGE: image}, {names.Classification.LABEL: label}
