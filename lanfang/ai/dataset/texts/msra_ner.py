from lanfang.ai.engine.dataset import Dataset
from lanfang.ai.engine import names
from lanfang.ai import utils
from lanfang.ai.utils.tokenizer import Tokenizer
from lanfang.ai.utils.dictionary import Dictionary
from lanfang.utils import cjk

import os
import urllib.request
import logging
import itertools
import random
import operator

import tensorflow as tf


class MSRA_NER(Dataset):
  """Chinese Dataset of Named Entity Recognition from MSRA.

  Links: https://github.com/InsaneLife/ChineseNLPCorpus/tree/master/NER/MSRA
  Backup Data Links: https://github.com/zero91/data/blob/master/NER/MSRA.zip
  
  Paper: [The Third International Chinese Language Processing Bakeoff: 
          Word Segmentation and Named Entity Recognition](
          https://faculty.washington.edu/levow/papers/sighan06.pdf)
  """

  __data_url__ = "https://raw.githubusercontent.com/"\
                 "InsaneLife/ChineseNLPCorpus/master/NER/MSRA"
  __data_md5__ = None

  def __init__(self, vocab_size=4711, lowercase=False, oov_size=1, maxlen=100,
                     vocab_file=None, tokenizer="jieba",
                     local_dir="~/.lanfang/ai/dataset/texts/msra_ner",
                     **kwargs):
    self._m_vocab_size = vocab_size
    self._m_lowercase = lowercase
    self._m_oov_size = oov_size
    self._m_maxlen = maxlen
    self._m_vocab_file = vocab_file
    self._m_tokenizer = Tokenizer.create(tokenizer)

    self._m_local_dir = os.path.expanduser(local_dir)
    self._m_data_path = "{}/tokenizer={}.maxlen={}.lowercase={}".format(
        self._m_local_dir, tokenizer, maxlen, lowercase)
    self._m_pos2entity = {"nr": "PER", "ns": "LOC", "nt": "ORG"}
    self._m_seed = 9507
    self._m_dev_size = 5000

  @staticmethod
  def name():
    return "msra_ner"

  @staticmethod
  def version():
    return "sighan06"

  @staticmethod
  def sota():
    return

  @staticmethod
  def default_parameters():
    return {
      names.Dictionary.VOCAB_SIZE: 4711,
      names.Text.LOWERCASE: False,
      names.Dictionary.OOV_SIZE: 1,
      names.Text.MAXLEN: 100,
      names.Dictionary.VOCAB_FILE: None,
      names.Text.TOKENIZER: "jieba"
    }

  def parameters(self):
    return {
      names.Dictionary.VOCAB_SIZE: self._m_vocab_size,
      names.Text.LOWERCASE: self._m_lowercase,
      names.Dictionary.OOV_SIZE: self._m_oov_size,
      names.Text.MAXLEN: self._m_maxlen,
      names.Dictionary.VOCAB_FILE: self._m_vocab_file,
      names.Text.TOKENIZER: self._m_tokenizer.name()
    }

  def meta(self):
    return {
      names.Classification.NUM_CLASSES: 7,
      names.Dictionary.WORD_SEG_VOCAB_SIZE: 5
    }

  def artifacts(self):
    return {
      names.Dictionary.VOCAB_FILE: os.path.join(
          self._m_data_path, "vocab.txt"),

      names.Dictionary.LABEL_FILE: os.path.join(
          self._m_data_path, "entity.vocab.txt"),

      names.Dictionary.WORD_SEG_VOCAB_FILE: os.path.join(
          self._m_data_path, "seg.vocab.txt"),
    }

  def prepare(self):
    if not os.path.exists(self._m_local_dir):
      os.makedirs(self._m_local_dir)

    for fname in ["test1.txt", "testright1.txt", "train1.txt"]:
      url = "{}/{}".format(self.__class__.__data_url__, fname)
      local_file = os.path.join(self._m_local_dir, fname)
      if os.path.exists(local_file):
        continue
      logging.info("Download %s", url)
      urllib.request.urlretrieve(url, local_file)

    if not os.path.exists(self._m_data_path):
      os.makedirs(self._m_data_path)

    train_fname = os.path.join(self._m_data_path, "train.txt")
    dev_fname = os.path.join(self._m_data_path, "dev.txt")
    test_fname = os.path.join(self._m_data_path, "test.txt")

    if not os.path.exists(test_fname):
      logging.info("Preprocess %s/testright1.txt", self._m_local_dir)
      sentences = self._preprocess(os.path.join(
          self._m_local_dir, "testright1.txt"))
      self._save_sentences(sentences, test_fname)

    if not os.path.exists(train_fname) or not os.path.exists(dev_fname):
      logging.info("Preprocess %s/train1.txt", self._m_local_dir)
      sentences = self._preprocess(os.path.join(
          self._m_local_dir, "train1.txt"))
      random.seed(self._m_seed)
      random.shuffle(sentences)
      self._save_sentences(sentences[: self._m_dev_size], dev_fname)
      self._save_sentences(sentences[self._m_dev_size: ], train_fname)

    self._build_dict()
    return self._m_data_path

  def read(self, split, mode):
    if split not in [names.DataSplit.TRAIN,
                     names.DataSplit.DEV,
                     names.DataSplit.TEST]:
      raise ValueError("Invalid split value '%s'" % (split))

    if self._m_vocab_file is not None:
      vocab_file = self._m_vocab_file
    else:
      vocab_file = os.path.join(self._m_data_path, "vocab.txt")

    vocab_dict = Dictionary(
        vocab_file=vocab_file,
        vocab_size=self._m_vocab_size,
        oov_size=self._m_oov_size)

    entity_vocab_dict = Dictionary(
        vocab_file=os.path.join(self._m_data_path, "entity.vocab.txt"),
        vocab_size=self.meta()[names.Classification.NUM_CLASSES])

    seg_vocab_dict = Dictionary(
        vocab_file=os.path.join(self._m_data_path, "seg.vocab.txt"),
        vocab_size=self.meta()[names.Dictionary.WORD_SEG_VOCAB_SIZE])

    def read_data():
      sentence = []
      with open(os.path.join(self._m_data_path, split + '.txt'), 'r') as fin:
        for line in fin:
          if line == '\n':
            if len(sentence) > 0:
              yield tuple(itertools.chain(zip(*sentence), [len(sentence)]))
              sentence = []
            continue
          sentence.append(line.split())
      if len(sentence) > 0:
        yield tuple(itertools.chain(zip(*sentence), [len(sentence)]))

    sentences = tf.data.Dataset.from_generator(
        read_data,
        output_types=(tf.string, tf.string, tf.string, tf.int32))

    def lookup(tokens, entity_tags, seg_tags, sentence_length):
      tokens = vocab_dict.lookup(tokens)
      entity_tags = entity_vocab_dict.lookup(entity_tags)
      seg_tags = seg_vocab_dict.lookup(seg_tags)
      return tokens, entity_tags, seg_tags, sentence_length

    sentences = sentences.map(lookup)
    return sentences

  def paddings(self):
    num_classes = self.meta()[names.Classification.NUM_CLASSES]
    padded_shapes = (
      {
        names.Text.SENTENCE: tf.TensorShape([None]),
        names.Text.WORD_SEG_TAGS: tf.TensorShape([None]),
        names.Text.SENTENCE_LENGTH: tf.TensorShape([]),
      },
      {
        names.Classification.LABEL: tf.TensorShape([None])
      }
    )

    padding_values = (
      {
        names.Text.SENTENCE: tf.constant(0, dtype=tf.int32),
        names.Text.WORD_SEG_TAGS: tf.constant(0, dtype=tf.int32),
        names.Text.SENTENCE_LENGTH: tf.constant(0, dtype=tf.int32),
      },
      {
        names.Classification.LABEL: tf.constant(0, dtype=tf.int32)
      }
    )
    return padded_shapes, padding_values

  def parse(self, mode, tokens, entity_tags, seg_tags, sentence_length):
    tokens = tf.cast(tokens, dtype=tf.int32)
    entity_tags = tf.cast(entity_tags, dtype=tf.int32)
    seg_tags = tf.cast(seg_tags, dtype=tf.int32)
    return (
      {
        names.Text.SENTENCE: tokens,
        names.Text.WORD_SEG_TAGS: seg_tags,
        names.Text.SENTENCE_LENGTH: sentence_length
      },
      {
        names.Classification.LABEL: entity_tags
      }
    )

  def _preprocess(self, data_file):
    # O, B-PER, I-PER, B-LOC, I-LOC, B-ORG, I-ORG
    sentences = []
    with open(data_file, 'r') as fin:
      for line in fin:
        words = []
        entity_tags = []
        for word_pos in line.split():
          word, pos = word_pos.split('/')
          for i, char in enumerate(word):
            if pos in self._m_pos2entity:
              if i == 0:
                entity_tags.append("B-" + self._m_pos2entity[pos])
              else:
                entity_tags.append("I-" + self._m_pos2entity[pos])
            else:
              entity_tags.append("O")

          if self._m_lowercase is True:
            word = word.lower()
          words.append(cjk.full2half(word))

        punc_pos = {';': None, ',': None}
        sentence = []
        for char, entity_tag in zip(itertools.chain(*words), entity_tags):
          if len(sentence) >= self._m_maxlen:
            if punc_pos[';'] is not None:
              sentences.append(sentence[:punc_pos[';']])
              sentence = sentence[punc_pos[';']:]
            elif punc_pos[','] is not None:
              sentences.append(sentence[:punc_pos[',']])
              sentence = sentence[punc_pos[',']:]
            else:
              sentences.append(sentence)
              sentence = []
            punc_pos = {punc: None for punc in punc_pos}

          sentence.append([char, entity_tag])
          if char in punc_pos:
            punc_pos[char] = len(sentence)
        sentences.append(sentence)

    for sentence in sentences:
      words = "".join(map(operator.itemgetter(0), sentence))
      seg_tags = utils.tags.tag_words(self._m_tokenizer.tokenize(words))[0]
      for i, tag in enumerate(itertools.chain(*seg_tags)):
        sentence[i].append(tag)
    return sentences

  def _save_sentences(self, sentences, output_fname):
    with open(output_fname, 'w') as fout:
      for sentence in sentences:
        for char, entity_tag, seg_tag in sentence:
          fout.write("{} {} {}\n".format(char, entity_tag, seg_tag))
        fout.write("\n")

  def _build_dict(self):
    vocab_file = os.path.join(self._m_data_path, 'vocab.txt')
    entity_vocab_file = os.path.join(self._m_data_path, 'entity.vocab.txt')
    seg_vocab_file = os.path.join(self._m_data_path, 'seg.vocab.txt')
    if os.path.exists(vocab_file) \
        and os.path.exists(entity_vocab_file) \
        and os.path.exists(seg_vocab_file):
      return

    with open(os.path.join(self._m_data_path, 'train.txt'), 'r') as fin:
      tokens = []
      for line in fin:
        if line == "\n":
          continue
        tokens.append(line.split())

    for c, fname in enumerate([vocab_file, entity_vocab_file, seg_vocab_file]):
      if fname in [vocab_file, seg_vocab_file]:
        extra_tokens = [names.SpecialToken.PAD]
      else:
        extra_tokens = None
      Tokenizer.create("identity_tokenizer").build_dict(
          text=[map(operator.itemgetter(c), tokens)],
          save_file=fname,
          extra_tokens=extra_tokens)
