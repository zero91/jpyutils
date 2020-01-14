import abc
import functools
import tensorflow as tf


class Dataset(abc.ABC):
  """Base class for a dataset.
  """

  __datasets__ = {}

  @staticmethod
  def register(dataset_class):
    """Register a dataset."""
    name = dataset_class.name().lower()
    if name not in Dataset.__datasets__:
      Dataset.__datasets__[name] = {}

    version = dataset_class.version().lower()
    if version in Dataset.__datasets__[name]:
      raise KeyError("Version %s of dataset %s is already been registered" % (
          dataset_class.version(), dataset_class.name()))

    Dataset.__datasets__[name][version] = dataset_class

  @staticmethod
  def create(name, **kwargs):
    """Create dataset."""
    name = name.lower()
    if name not in Dataset.__datasets__:
      raise KeyError("Can't find dataset: %s" % (name))

    if "version" in kwargs:
      version = kwargs.pop("version")
    elif len(Dataset.__datasets__[name]) == 1:
      version = list(Dataset.__datasets__[name].keys())[0]
    else:
      versions = Dataset.__datasets__[name].keys()
      raise KeyError("Dataset %s have multiple versions: %s, "
          "please specify one." % (name, ",".join(versions)))

    return Dataset.__datasets__[name][version](**kwargs)

  @staticmethod
  @abc.abstractmethod
  def name():
    raise NotImplementedError(
        "Abstract static method 'name' must be implemented.")

  @staticmethod
  @abc.abstractmethod
  def version():
    """Return version of this dataset."""
    raise NotImplementedError(
        "Abstract static method 'version' must be implemented.")

  @staticmethod
  @abc.abstractmethod
  def sota():
    """Return SOTA research result of using dataset."""
    raise NotImplementedError(
        "Abstract static method 'sota' must be implemented.")

  @staticmethod
  @abc.abstractmethod
  def default_parameters():
    raise NotImplementedError(
        "Abstract static method 'default_parameters' must be implemented.")

  @abc.abstractmethod
  def parameters(self):
    """Return parameters of this dataset."""
    pass

  @abc.abstractmethod
  def meta(self):
    """Return meta info of this dataset.
    """
    pass

  @abc.abstractmethod
  def artifacts(self):
    """Return artifacts of this dataset."""
    pass

  @abc.abstractmethod
  def prepare(self):
    """Prepare the dataset."""
    pass

  @abc.abstractmethod
  def read(self, split, mode):
    """Create an instance of the tf.data.Dataset object."""
    pass

  @abc.abstractmethod
  def parse(self, mode, *args):
    """Parse an example into features and labels."""
    pass

  @abc.abstractmethod
  def paddings(self):
    """Padded shapes and padding values for batch data."""
    pass

  def cache(self, dataset):
    """Return a cached dataset. Return the original dataset
    if you don't want to cache the data.
    """
    return dataset.cache()

  def data_fn(self, splits, *, batch_size=32,
                               num_epochs=None,
                               shuffle_batches=100,
                               prefetch_buffer_size=1):
    def input_fn(splits, mode, config):
      if isinstance(splits, dict):
        splits, weights = list(zip(*splits.items()))
      else:
        weights = None

      if not isinstance(splits, (list, tuple)):
        splits = [splits]

      ds = [self.read(split, mode) for split in splits]
      d = tf.data.experimental.sample_from_datasets(ds, weights=weights)
      d = d.map(functools.partial(self.parse, mode),
                num_parallel_calls=tf.data.experimental.AUTOTUNE)
      d = self.cache(d)

      if mode == tf.estimator.ModeKeys.TRAIN:
        d = d.shuffle(batch_size * shuffle_batches)
        d = d.repeat(num_epochs)

      paddings = self.paddings()
      if paddings is not None:
        padded_shapes, padding_values = paddings
        d = d.padded_batch(
            batch_size,
            padded_shapes=padded_shapes,
            padding_values=padding_values)
      else:
        d = d.batch(batch_size)

      d = d.prefetch(prefetch_buffer_size)
      return d
    return functools.partial(input_fn, splits)
