from lanfang import utils
import abc
import copy
import tensorflow as tf


class Model(abc.ABC):
  """Base class for a model.
  """

  # The following are factory methods.
  __models__ = {}

  @staticmethod
  def register(name, model_class):
    """Register model."""
    Model.__models__[name] = model_class

  @staticmethod
  def create(name, **params):
    """Create model."""
    model_class = Model.__models__[name]
    kwargs = utils.func.extract_kwargs(model_class, params)
    return model_class(**kwargs)

  # The following are abstract methods.
  @abc.abstractstaticmethod
  def name():
    raise NotImplementedError(
        "You should implement abstract static method 'name' first.")

  @abc.abstractstaticmethod
  def get_default_params():
    """Return model default parameters."""
    return {}

  @abc.abstractmethod
  def prepare(self):
    """This function will be called once to prepare the model."""
    pass

  @abc.abstractmethod
  def create_model(self):
    """Create a model. """
    pass

  @abc.abstractmethod
  def get_losses_fn(self):
    """Get model losses function."""
    pass

  @abc.abstractmethod
  def get_metrics(self):
    """Get model evaluation metrics."""
    pass

  # The following are class instance methods.
  def __init__(self, **params):
    self.__set_params(params)
    self._m_model = self.create_model()

  def get_model(self):
    features = self.get_features()
    outputs = self.get_predict(features)
    if isinstance(outputs, tf.keras.Model):
      return outputs
    return tf.keras.Model(inputs=features, outputs=outputs)

  def get_params(self):
    """Return model runtime parameters."""
    return copy.deepcopy(self.__m_params)

  def __set_params(self, params):
    if hasattr(self, "__m_params"): # Runtime parameters has already been set.
      return

    self.__m_params = copy.deepcopy(self.__class__.get_default_params())
    for param_name, param_value in params.items():
      if param_name not in self.__m_params:
        continue
      self.__m_params[param_name] = param_value
