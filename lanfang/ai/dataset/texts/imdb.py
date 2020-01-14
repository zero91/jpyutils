from lanfang.ai.engine.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.ai.utils.tokenizer import Tokenizer
from lanfang.ai.utils.dictionary import Dictionary
from lanfang.utils import disk

import os
import urllib.request
import random
import tarfile
import logging
import itertools
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
                     tokenizer="simple_tokenizer", vocab_file=None,
                     local_dir="~/.lanfang/ai/dataset/texts/imdb", **kwargs):
    self._m_vocab_size = vocab_size
    self._m_lowercase = lowercase
    self._m_oov_size = oov_size
    self._m_maxlen = maxlen
    self._m_tokenizer = Tokenizer.create(tokenizer)

    self._m_local_dir = os.path.expanduser(local_dir)
    self._m_data_path = "{}/{}.lowercase={}".format(
        self._m_local_dir, tokenizer, self._m_lowercase)
    self._m_vocab_file = vocab_file

    self._m_seed = 9507
    self._m_dev_size = 5000

  @staticmethod
  def name():
    return "imdb"

  @staticmethod
  def version():
    return "v1.0"

  @staticmethod
  def sota():
    return

  @staticmethod
  def default_parameters():
    return {
      names.Dictionary.VOCAB_FILE: None,
      names.Dictionary.VOCAB_SIZE: 10000,
      names.Dictionary.LOWERCASE: True,
      names.Dictionary.OOV_SIZE: 1,
      names.Text.MAXLEN: 256,
      names.Text.TOKENIZER: "simple_tokenizer",
    }

  def parameters(self):
    return {
      names.Dictionary.VOCAB_FILE: self._m_vocab_file,
      names.Dictionary.VOCAB_SIZE: self._m_vocab_size,
      names.Dictionary.LOWERCASE: self._m_lowercase,
      names.Dictionary.OOV_SIZE: self._m_oov_size,
      names.Text.MAXLEN: self._m_maxlen,
      names.Text.TOKENIZER: self._m_tokenizer.name()
    }

  def meta(self):
    return {
      names.Classification.NUM_CLASSES: 2
    }

  def artifacts(self):
    return {
      names.Dictionary.VOCAB_FILE: os.path.join(self._m_data_path, "vocab.txt")
    }

  def prepare(self):
    """This function will be called once to prepare the dataset."""
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    tar_fname = os.path.join(
      self._m_local_dir, os.path.basename(self.__class__.__data_url__))
    if not os.path.exists(tar_fname) or disk.md5(
            tar_fname) != self.__class__.__data_md5__:
      logging.info("Download %s", self.__class__.__data_url__)
      urllib.request.urlretrieve(self.__class__.__data_url__, tar_fname)

      tar = tarfile.open(tar_fname)
      tar.extractall(self._m_local_dir)
      tar.close()

    # Split data
    random.seed(self._m_seed)
    data_info = {"train": [], "dev": [], "test": []}
    for d, polarity in itertools.product(["train", "test"], ["pos", "neg"]):
      data_path = "{}/aclImdb/{}/{}".format(self._m_local_dir, d, polarity)

      label = 1 if polarity == 'pos' else 0
      data_files = []
      for fname in sorted(os.listdir(data_path)):
        data_files.append({
            "file": "aclImdb/%s/%s/%s" % (d, polarity, fname),
            "label": label
        })
      if d == "train":
        random.shuffle(data_files)
        data_info["train"].extend(data_files[: -self._m_dev_size // 2])
        data_info["dev"].extend(data_files[-self._m_dev_size // 2: ])
      else:
        data_info["test"].extend(data_files)
    random.shuffle(data_info["train"])

    os.makedirs(self._m_data_path, exist_ok=True)
    # Build dictionary
    if self._m_vocab_file is not None:
      vocab_file = self._m_vocab_file
    else:
      vocab_file = os.path.join(self._m_data_path, "vocab.txt")
    if not os.path.exists(vocab_file):
      train_files = map(
          lambda f: self._m_local_dir + "/" + f['file'], data_info["train"])
      self._m_tokenizer.build_dict(
          files=train_files,
          save_file=vocab_file,
          lowercase=self._m_lowercase)

    # Tokenize and save data
    for split, files in data_info.items():
      split_file = os.path.join(self._m_data_path, "{}.txt".format(split))
      if os.path.exists(split_file):
        continue
      with open(split_file, 'w') as fout:
        for doc in files:
          with open(os.path.join(self._m_local_dir, doc['file']), 'r') as fin:
            tokens = self._m_tokenizer.tokenize(fin.read())
          token_str = "\1".join(tokens)
          fout.write("%s\2%s\2%s\n" % (doc['file'], doc['label'], token_str))
    return self._m_data_path

  def paddings(self):
    num_classes = self.meta()[names.Classification.NUM_CLASSES]
    padded_shapes = (
      {names.Text.SENTENCE: tf.TensorShape([None])},
      {names.Classification.LABEL: tf.TensorShape([])},
    )

    padding_values = (
      {names.Text.SENTENCE: tf.constant(0, dtype=tf.int32)},
      {names.Classification.LABEL: tf.constant(0, dtype=tf.int32)},
    )
    return padded_shapes, padding_values

  def read(self, split, mode):
    if split not in [names.DataSplit.TRAIN,
                     names.DataSplit.DEV,
                     names.DataSplit.TEST]:
      raise ValueError("Invalid split value '%s'" % (split))

    if self._m_vocab_file is not None:
      vocab_file = self._m_vocab_file
    else:
      vocab_file = os.path.join(self._m_data_path, "vocab.txt")

    dictionary = Dictionary(
        vocab_file=vocab_file,
        vocab_size=self._m_vocab_size)

    dataset = tf.data.TextLineDataset(
        "{}/{}.txt".format(self._m_data_path, split))

    def decode_line(line):
      fields = tf.strings.split(line, sep='\2', maxsplit=2)
      label = tf.strings.to_number(fields[1], out_type=tf.int32)
      tokens = tf.strings.split(fields[2], sep='\1')[:self._m_maxlen]
      if self._m_lowercase is True:
        tokens = tf.strings.lower(tokens)
      return dictionary.lookup(tokens), label

    dataset = dataset.map(decode_line)
    return dataset

  def parse(self, mode, sent, label):
    sent = tf.cast(sent, tf.int32)
    return {names.Text.SENTENCE: sent}, {names.Classification.LABEL: label}
