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


class Dictionary(str, enum.Enum):
  VOCAB_FILE = "vocab_file"
  VOCAB_SIZE = "vocab_size"
  LOWERCASE = "lowercase"
  OOV_SIZE = "oov_size"


class Text(str, enum.Enum):
  MAXLEN = "maxlen"
  SENTENCE = "sent"
  SENTENCE_A = "sent_a"
  SENTENCE_B = "sent_b"


class Classification(str, enum.Enum):
  NUM_CLASSES = "num_classes"
  LABEL = "label"


class CNN(str, enum.Enum):
  FILTERS = "filters"
  KERNEL_SIZE = "kernel_size"
  KERNEL_STRIDES = "kernel_strides"
  POOL_SIZE = "pool_size"
  POOL_STRIDES = "pool_strides"


class Dense(str, enum.Enum):
  UNITS = "units"


class Regularization(str, enum.Enum):
  DROP_RATE = "drop_rate"
