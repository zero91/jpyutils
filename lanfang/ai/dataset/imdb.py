from lanfang.ai.engine.base.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.utils import disk

import os
import urllib.request
import json
import random
import collections
import glob
import operator
import tarfile
import logging
import tensorflow as tf


class IMDB(Dataset):
  """Dataset of IMDB.

  IMDB: Large Movie Review Dataset
  Links: http://ai.stanford.edu/~amaas/data/sentiment/

  """
  __data_url__ = "http://ai.stanford.edu/~amaas/data/sentiment/" \
                 "aclImdb_v1.tar.gz"
  __data_md5__ = "7c2ac02c03563afcf9b574c7e56c153a"

  def __init__(self, vocab_size=10000, lowercase=True, oov_size=1, maxlen=256,
                     local_dir="~/.lanfang/ai/dataset/imdb", **params):
    self._m_local_dir = os.path.expanduser(local_dir)
    self._m_fname = os.path.basename(self.__class__.__data_url__)
    self._m_seed = 9507
    self._m_dev_size = 5000

    self._m_vocab_size = vocab_size
    self._m_lowercase = lowercase
    self._m_oov_size = oov_size
    self._m_maxlen = maxlen

    self._m_data_dir = "vocab_size=%s_lowercase=%s_oov_size=%s_maxlen=%s" % (
        self._m_vocab_size, self._m_lowercase,
        self._m_oov_size, self._m_maxlen)

  def name():
    return "imdb"

  def get_params(self):
    return {
      names.Dictionary.VOCAB_SIZE: self._m_vocab_size,
      names.Dictionary.LOWERCASE: self._m_lowercase,
      names.Dictionary.OOV_SIZE: self._m_oov_size,
      names.Text.MAXLEN: self._m_maxlen,
      names.Classification.NUM_CLASSES: 2
    }

  def artifacts(self):
    return {
      names.Dictionary.VOCAB_FILE: os.path.join(
          self._m_local_dir, self._m_data_dir)
    }

  def prepare(self):
    """This function will be called once to prepare the dataset."""
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    tar_fname = os.path.join(self._m_local_dir, self._m_fname)
    if not os.path.exists(tar_fname) or disk.md5(
            tar_fname) != self.__class__.__data_md5__:
      logging.info("Download %s", self.__class__.__data_url__)
      urllib.request.urlretrieve(self.__class__.__data_url__, tar_fname)

      tar = tarfile.open(tar_fname)
      tar.extractall(self._m_local_dir)
      tar.close()
    return self._preprocess()

  def get_padding(self):
    num_classes = self.get_params()[names.Classification.NUM_CLASSES]
    padded_shapes = (
      {names.Text.SENTENCE: tf.TensorShape([None])},
      {names.Classification.LABEL: tf.TensorShape([])},
    )

    padding_values = (
      {"sent": tf.constant(0, dtype=tf.int32)},
      {"label": tf.constant(0, dtype=tf.int32)},
    )
    return padded_shapes, padding_values

  def read(self, split, mode):
    if split not in names.DataSplit:
      raise ValueError("Invalid split value '%s'" % (split))

    params = self.get_params()
    data_path = os.path.join(self._m_local_dir, self._m_data_dir)

    valid_vocab_size = params[
        names.Dictionary.VOCAB_SIZE] - params[names.Dictionary.OOV_SIZE]
    word2id = tf.lookup.StaticVocabularyTable(
        tf.lookup.TextFileInitializer(
            filename=os.path.join(data_path, "vocab.txt"),
            key_dtype=tf.string,
            key_index=0,
            value_dtype=tf.int64,
            value_index=tf.lookup.TextFileIndex.LINE_NUMBER,
            delimiter='\t',
            vocab_size=valid_vocab_size),
        num_oov_buckets=params[names.Dictionary.OOV_SIZE])

    split_path = os.path.join(self._m_local_dir, self._m_data_dir, split)
    sents = tf.data.TextLineDataset(os.path.join(split_path, "reviews.txt"))
    sents = sents.map(lambda s: word2id.lookup(tf.strings.split([s]).values))

    labels = tf.data.TextLineDataset(os.path.join(split_path, "polarities.txt"))
    labels = labels.map(lambda l: tf.strings.to_number(l, out_type=tf.int32))
    return tf.data.Dataset.zip((sents, labels))

  def parse(self, mode, sent, label):
    sent = tf.cast(sent, tf.int32)
    return {"sent": sent}, {"label": label}

  def _preprocess(self):
    full_data_dir = os.path.join(self._m_local_dir, self._m_data_dir)
    if os.path.exists(os.path.join(full_data_dir, "DONE")):
      return full_data_dir

    os.makedirs(full_data_dir, exist_ok=True)
    random.seed(self._m_seed)

    # Load data
    data_splits = collections.defaultdict(list)
    for split in ["train", "test"]:
      for polarity, label in [("pos", 1), ("neg", 0)]:
        logging.info("Processing %s/%s data", split, polarity)
        subdir = os.path.join(self._m_local_dir, "aclImdb", split, polarity)
        for fname in sorted(glob.glob(os.path.join(subdir, "*.txt"))):
          with open(fname, 'r') as fin:
            review = fin.read()
            if self._m_lowercase is True:
              review = review.lower()
            if self._m_maxlen is not None and self._m_maxlen > 0:
              review = " ".join(review.split()[:self._m_maxlen])
            data_splits[split].append((review, label))
      random.shuffle(data_splits[split])

    # Split train into train/dev dataset
    data_splits["dev"] = data_splits["train"][:self._m_dev_size]
    data_splits["train"] = data_splits["train"][self._m_dev_size:]

    # Save split data
    for split, samples in data_splits.items():
      os.makedirs(os.path.join(full_data_dir, split), exist_ok=True)
      ftext = open(os.path.join(full_data_dir, split, "reviews.txt"), 'w')
      flabel = open(os.path.join(full_data_dir, split, "polarities.txt"), 'w')
      for text, label in samples:
        ftext.write("%s\n" % (text))
        flabel.write("%s\n" % (label))
      ftext.close()
      flabel.close()

    # Build dict
    vocab = collections.defaultdict(int)
    with open(os.path.join(full_data_dir, "train", "reviews.txt"), 'r') as fin:
      for line in fin:
        for token in line.split():
          vocab[token] += 1
      vocab["<PAD>"] = max(vocab.values()) + 1
    with open(os.path.join(full_data_dir, "vocab.txt"), 'w') as fout:
      for word, freq in sorted(
          vocab.items(), key=operator.itemgetter(1), reverse=True):
        fout.write("%s\t%d\n" % (word, freq))

    # Create empty flag done file.
    with open(os.path.join(full_data_dir, "DONE"), 'w'):
      pass
    return full_data_dir
