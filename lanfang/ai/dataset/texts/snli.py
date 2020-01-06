from lanfang.ai.engine.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.ai.utils.tokenizer import Tokenizer
from lanfang.ai.utils.dictionary import Dictionary
from lanfang.utils import disk

import os
import urllib.request
import json
import logging

import nltk
import tensorflow as tf


class SNLI(Dataset):
  """Dataset of SNLI.

  SNLI: The Stanford Natural Language Inference (SNLI) Corpus
  Links: https://nlp.stanford.edu/projects/snli/
  """

  __data_url__ = "https://nlp.stanford.edu/projects/snli/snli_1.0.zip"
  __data_md5__ = ""

  def __init__(self, vocab_size=10000, lowercase=True, oov_size=1, maxlen=256,
                     vocab_file=None,
                     local_dir="~/.lanfang/ai/dataset/texts/snli", **kwargs):
    self._m_vocab_size = vocab_size
    self._m_lowercase = lowercase
    self._m_oov_size = oov_size
    self._m_maxlen = maxlen
    self._m_vocab_file = vocab_file

    self._m_local_dir = os.path.expanduser(local_dir)
    self._m_data_path = "{}/lowercase={}".format(
        self._m_local_dir, self._m_lowercase)

  def name():
    return "snli"

  def default_parameters():
    return {
      names.Dictionary.VOCAB_FILE: None,
      names.Dictionary.VOCAB_SIZE: 10000,
      names.Dictionary.LOWERCASE: True,
      names.Dictionary.OOV_SIZE: 1,
      names.Text.MAXLEN: 256
    }

  def parameters(self):
    return {
      names.Dictionary.VOCAB_FILE: self._m_vocab_file,
      names.Dictionary.VOCAB_SIZE: self._m_vocab_size,
      names.Dictionary.LOWERCASE: self._m_lowercase,
      names.Dictionary.OOV_SIZE: self._m_oov_size,
      names.Text.MAXLEN: self._m_maxlen
    }

  def meta(self):
    return {
      names.Classification.NUM_CLASSES: 3
    }

  def artifacts(self):
    return {
      names.Dictionary.VOCAB_FILE: os.path.join(
          self._m_data_path, "vocab.txt"),

      names.Dictionary.LABEL_FILE: os.path.join(
          self._m_data_path, "label.vocab.txt")
    }

  def prepare(self):
    """This function will be called once to prepare the dataset."""
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    zip_fname = os.path.join(
        self._m_local_dir, os.path.basename(self.__class__.__data_url__))
    if not os.path.exists(zip_fname):
      logging.info("Download %s", self.__class__.__data_url__)
      urllib.request.urlretrieve(self.__class__.__data_url__, zip_fname)

    self._extract_data(zip_fname, self._m_data_path)
    return self._m_data_path

  def paddings(self):
    num_classes = self.meta()[names.Classification.NUM_CLASSES]
    padded_shapes = (
      {
        names.Text.SENTENCE_A: tf.TensorShape([None]),
        names.Text.SENTENCE_B: tf.TensorShape([None])
      },
      {names.Classification.LABEL: tf.TensorShape([])}
    )

    padding_values = (
      {
        names.Text.SENTENCE_A: tf.constant(0, dtype=tf.int32),
        names.Text.SENTENCE_B: tf.constant(0, dtype=tf.int32),
      },
      {names.Classification.LABEL: tf.constant(0, dtype=tf.int32)}
    )
    return padded_shapes, padding_values

  def read(self, split):
    if split not in ["train", "dev", "test"]:
      raise ValueError("Invalid split value '%s'" % (split))

    if self._m_vocab_file is not None:
      vocab_file = self._m_vocab_file
    else:
      vocab_file = os.path.join(self._m_data_path, "vocab.txt")

    vocab_dict = Dictionary(
        vocab_file=vocab_file,
        vocab_size=self._m_vocab_size)

    def decode_sent(sent):
      tokens = tf.strings.split(sent, sep='\1')
      if self._m_lowercase is True:
        tokens = tf.strings.lower(tokens)
      return vocab_dict.lookup(tokens)

    sent1 = tf.data.TextLineDataset(
        os.path.join(self._m_data_path, split, "sent1.txt"))
    sent1 = sent1.map(decode_sent)

    sent2 = tf.data.TextLineDataset(
        os.path.join(self._m_data_path, split, "sent2.txt"))
    sent2 = sent2.map(decode_sent)

    num_classes = self.meta()[names.Classification.NUM_CLASSES]
    oov_size = 1 # The api required positive integer.
    label_dict = Dictionary(
        vocab_file=self.artifacts()[names.Dictionary.LABEL_FILE],
        vocab_size=num_classes + oov_size,
        oov_size=oov_size)
    label_fn = lambda lbl: label_dict.lookup(tf.strings.strip(lbl))
    label = tf.data.TextLineDataset(
        os.path.join(self._m_data_path, split, "label.txt"))
    label = label.map(label_fn)
    return tf.data.Dataset.zip((sent1, sent2, label))

  def parse(self, mode, sent1, sent2, label):
    sent1 = tf.cast(sent1, dtype=tf.int32)
    sent2 = tf.cast(sent2, dtype=tf.int32)
    label = tf.cast(label, dtype=tf.int32)
    return (
        {names.Text.SENTENCE_A: sent1, names.Text.SENTENCE_B: sent2},
        {names.Classification.LABEL: label}
    )

  def _extract_data(self, zip_fname, output_path):
    train_tokens = []
    train_labels = []
    for split in ["train", "dev", "test"]:
      if os.path.exists(os.path.join(output_path, split, 'sent1.txt')) \
          and os.path.exists(os.path.join(output_path, split, 'sent2.txt')) \
          and os.path.exists(os.path.join(output_path, split, 'label.txt')):
        continue

      logging.info("Processing %s data", split)
      os.makedirs(os.path.join(output_path, split), exist_ok=True)
      fsent1 = open(os.path.join(output_path, split, 'sent1.txt'), 'w')
      fsent2 = open(os.path.join(output_path, split, 'sent2.txt'), 'w')
      flabel = open(os.path.join(output_path, split, 'label.txt'), 'w')
      for f, data in disk.read_zip(zip_fname, ".*%s\.jsonl" % split).items():
        for idx, line in enumerate(data.split('\n'), 1):
          if idx % 50000 == 0:
            logging.info("Processing %d lines", idx)
          if line == "":
            continue

          if self._m_lowercase is True:
            line = line.lower()
          line_data = json.loads(line)
          label = line_data['gold_label']
          if label == '-':
            continue
          sent1 = nltk.Tree.fromstring(line_data['sentence1_parse']).leaves()
          sent2 = nltk.Tree.fromstring(line_data['sentence2_parse']).leaves()

          if split == "train":
            train_tokens.extend([sent1, sent2])
            train_labels.append([label])

          fsent1.write("{}\n".format("\1".join(sent1)))
          fsent2.write("{}\n".format("\1".join(sent2)))
          flabel.write("{}\n".format(label))
      fsent1.close()
      fsent2.close()
      flabel.close()

      if split != "train":
        continue

      Tokenizer.create("identity_tokenizer").build_dict(
          text=train_tokens,
          save_file=os.path.join(output_path, "vocab.txt"))
      Tokenizer.create("identity_tokenizer").build_dict(
          text=train_labels,
          save_file=os.path.join(output_path, "label.vocab.txt"))
