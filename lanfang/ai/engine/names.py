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


class Classification(str, enum.Enum):
  NUM_CLASSES = "num_classes"
  LABEL = "label"

