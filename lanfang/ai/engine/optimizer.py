import abc
import copy

import tensorflow as tf


class Optimizer(abc.ABC):
  __optimizers__ = {}

  @staticmethod
  @abc.abstractmethod
  def register(name, optimizer_name):
    pass

  @staticmethod
  @abc.abstractmethod
  def create(name, learning_rate, **kwargs):
    """Create an optimizer.

    Parameters
    ----------
    name: str
      The algorithm of the optimizer.

    learning_rate: float, dict
      The learning rate or learning rate parameters of the optimize algorithm.

    Returns
    -------
    optimizer: An optimizer instance.
    """
    pass


class KerasOptimizer(Optimizer):
  __optimizers__ = {
    "adadelta": tf.keras.optimizers.Adadelta,
    "adagrad": tf.keras.optimizers.Adagrad,
    "adam": tf.keras.optimizers.Adam,
    "adamax": tf.keras.optimizers.Adamax,
    "ftrl": tf.keras.optimizers.Ftrl,
    "nadam": tf.keras.optimizers.Nadam,
    "rmsprop": tf.keras.optimizers.RMSprop,
    "sgd": tf.keras.optimizers.SGD,
  }

  @staticmethod
  def register(name, optimizer_class):
    """Register an optimizer."""
    if name.lower() in KerasOptimizer.__optimizers__:
      raise ValueError(
          "Optimizer '%s' is already exists in KerasOptimizer" % (name))

    if not isinstance(optimizer_class, tf.keras.optimizers.Optimizer):
      raise TypeError(
          "Parameter 'optimizer_class' of method KerasOptimizer.register "
          "must be an instance of tf.keras.optimizers.Optimizer.")

    KerasOptimizer.__optimizers__[name.lower()] = optimizer_class

  @staticmethod
  def create(name, learning_rate, **kwargs):
    """Create an optimizer.

    Parameters
    ----------
    name: str
      The algorithm of the optimizer.

    learning_rate: float, dict
      The learning rate or learning rate parameters of the optimize algorithm.

    Returns
    -------
    optimizer: tf.keras.optimizers.Optimizer
      An instance of tf.keras.optimizers.Optimizer.

    Returns
    -------
    optimizer: tf.keras.optimizers.Optimizer
      An instance of tf.keras.optimizers.Optimizer.
    """

    if isinstance(learning_rate, dict):
      learning_rate_params = copy.deepcopy(learning_rate)
      schedule = learning_rate_params.pop("schedule")
      learning_rate = KerasLearningRateSchedule.create(
          schedule, **learning_rate_params)

    return KerasOptimizer.__optimizers__[name.lower()](
        learning_rate=learning_rate, **kwargs)


class LearningRateSchedule(abc.ABC):
  __schedules__ = {}

  @staticmethod
  @abc.abstractmethod
  def register(name, schedule_class):
    pass

  @staticmethod
  @abc.abstractmethod
  def create(name, **kwargs):
    pass


class KerasLearningRateSchedule(LearningRateSchedule):

  __schedules__ = {
    "exponential_decay": tf.keras.optimizers.schedules.ExponentialDecay,
    "inverse_time_decay": tf.keras.optimizers.schedules.InverseTimeDecay,
    "polynomial_decay": tf.keras.optimizers.schedules.PolynomialDecay,
    "piecewise_constant_decay":
        tf.keras.optimizers.schedules.PiecewiseConstantDecay,
  }

  @staticmethod
  def register(name, schedule_class):
    """Register a learning rate schedule."""
    if name in KerasLearningRateSchedule.__schedules__:
      raise ValueError("Learning rate schedule '%s' is already exists "
          "in KerasLearningRateSchedule" % (name))

    if not isinstance(
        schedule_class,
        tf.keras.optimizers.schedules.LearningRateSchedule):
      raise TypeError("The parameter 'schedule_class' of method "
          "KerasLearningRateSchedule.register must be an instance of "
          "tf.keras.optimizers.schedules.LearningRateSchedule.")

    KerasLearningRateSchedule.__schedules__[name] = schedule_class

  @staticmethod
  def create(name, **kwargs):
    """Create an instance of tf.keras.optimizers.schedules.LearningRateSchedule
    which can manage the learning rate during training.

    Parameters
    ----------
    name: str
      The name of the learning rate schedule.

    kwargs: dict
      The parameters to initialize the LearningRateSchedule object.

    Returns
    -------
    schedule: tf.keras.optimizers.schedules.LearningRateSchedule
      An instance of LearningRateScheduler.
    """

    return KerasLearningRateSchedule.__schedules__[name](**kwargs)
