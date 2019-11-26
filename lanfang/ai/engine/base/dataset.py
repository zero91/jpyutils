import abc
import functools
import tensorflow as tf


class Dataset(abc.ABC):
  """Base class for a dataset.
  """

  # The following are factory methods.
  __datasets__ = {}

  @staticmethod
  def register(name, dataset_class):
    """Register a dataset."""
    Dataset.__datasets__[name] = dataset_class

  @staticmethod
  def create(name, **params):
    """Create dataset."""
    return Dataset.__datasets__[name](**params)

  # The following are abstract methods.
  @abc.abstractstaticmethod
  def name():
    raise NotImplementedError(
        "You should implement abstract static method 'name' first.")

  @abc.abstractmethod
  def get_params(self):
    """Return parameters of this dataset."""
    return {}

  @abc.abstractmethod
  def artifacts(self):
    """Return artifacts of this dataset."""
    return {}

  @abc.abstractmethod
  def prepare(self):
    """This function will be called once to prepare the dataset."""
    pass

  @abc.abstractmethod
  def get_padding(self):
    """Padded shapes and padding values for batch data"""
    return None

  @abc.abstractmethod
  def read(self, split, mode):
    """Create an instance of the tf.data.Dataset object."""
    pass

  @abc.abstractmethod
  def parse(self, mode, *args):
    """Parse input into features and labels."""
    pass

  def get_input_fn(self, splits, batch_size=32,
                                 num_epochs=None,
                                 shuffle_batches=100,
                                 prefetch_buffer_size=1,
                                 cache=False):
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
      if cache is True:
        d = d.cache()

      if mode == tf.estimator.ModeKeys.TRAIN:
        d = d.shuffle(batch_size * shuffle_batches)
        d = d.repeat(num_epochs)

      batch_shape = self.get_padding()
      if batch_shape is not None:
        padded_shapes, padding_values = batch_shape
        d = d.padded_batch(
          batch_size,
          padded_shapes=padded_shapes,
          padding_values=padding_values)
      else:
        d = d.batch(batch_size)

      d = d.prefetch(prefetch_buffer_size)
      return d
    return functools.partial(input_fn, splits)
