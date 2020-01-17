import enum


class DataSplit(str, enum.Enum):
  TRAIN = "train"
  DEV = "dev"
  TEST = "test"


class Image(str, enum.Enum):
  IMAGE = "image"
  WIDTH = "image_width"
  HEIGHT = "image_height"
  CHANNEL = "image_channel"


class Text(str, enum.Enum):
  MAXLEN = "maxlen"
  SENTENCE = "sent"
  SENTENCE_LENGTH = "sentence_length"
  WORD_SEG_TAGS = "word_seg_tags"
  SENTENCE_A = "sent_a"
  SENTENCE_B = "sent_b"
  SENTENCE_C = "sent_c"
  TOKENIZER = "tokenizer"
  LOWERCASE = "lowercase"


class Dictionary(str, enum.Enum):
  OOV_SIZE = "oov_size"
  VOCAB_FILE = "vocab_file"
  VOCAB_SIZE = "vocab_size"
  LABEL_FILE = "label_file"
  WORD_SEG_VOCAB_FILE = "word_seg_vocab_file"
  WORD_SEG_VOCAB_SIZE = "word_seg_vocab_size"


class SpecialToken(str, enum.Enum):
  PAD = "<PAD>"
  UNKNOWN = "<UNKNOWN>"


class CNN(str, enum.Enum):
  FILTERS = "cnn_filters"
  KERNEL_SIZE = "cnn_kernel_size"
  KERNEL_STRIDES = "cnn_kernel_strides"
  POOL_SIZE = "cnn_pool_size"
  POOL_STRIDES = "cnn_pool_strides"


class Dense(str, enum.Enum):
  UNITS = "dense_units"


class Embedding(str, enum.Enum):
  WORD_EMBEDDING_DIM = "word_embedding_dim"
  CHAR_EMBEDDING_DIM = "char_embedding_dim"


class Regularization(str, enum.Enum):
  DROP_RATE = "drop_rate"
  L1_WEIGHT = "l1_loss_weight"
  L2_WEIGHT = "l2_loss_weight"


class Classification(str, enum.Enum):
  NUM_CLASSES = "num_classes"
  LABEL = "label"
