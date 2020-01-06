import tensorflow as tf


class Dictionary(object):
  def __init__(self, vocab_file,
                     vocab_size,
                     oov_size=1,
                     key_index=0,
                     value_index=tf.lookup.TextFileIndex.LINE_NUMBER,
                     value_dtype=tf.int64,
                     delimiter='\t'):
    valid_vocab_size = vocab_size - oov_size
    self._m_word2id = tf.lookup.StaticVocabularyTable(
        tf.lookup.TextFileInitializer(
            filename=vocab_file,
            key_dtype=tf.string,
            key_index=key_index,
            value_dtype=value_dtype,
            value_index=value_index,
            delimiter=delimiter,
            vocab_size=valid_vocab_size),
        num_oov_buckets=oov_size)

  def lookup(self, s):
    return self._m_word2id.lookup(s)
