import abc
import tensorflow as tf


class Model(abc.ABC):
  """Base class for a model.
  """

  __models__ = {}

  @staticmethod
  def register(model_class):
    """Register a model."""
    name = model_class.name().lower()
    if name in Model.__models__:
      raise KeyError("Model '%s' is already exists." % (name))
    Model.__models__[name] = model_class

  @staticmethod
  def create(name, **kwargs):
    """Create model."""
    return Model.__models__[name.lower()](**kwargs)

  @staticmethod
  @abc.abstractmethod
  def name():
    raise NotImplementedError(
        "Abstract static method 'name' must be implemented.")

  @staticmethod
  @abc.abstractmethod
  def default_parameters():
    raise NotImplementedError(
        "Abstract static method 'default_parameters' must be implemented.")

  @abc.abstractmethod
  def parameters(self):
    pass

  @abc.abstractmethod
  def prepare(self):
    """This function will be called once to prepare the model."""
    pass

  @abc.abstractmethod
  def model_fn(self):
    pass

  @abc.abstractmethod
  def losses_fn(self):
    pass

  @abc.abstractmethod
  def metrics_fn(self):
    pass


class KerasModel(Model):
  pass
