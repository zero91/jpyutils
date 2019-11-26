from lanfang import utils
import abc
import copy
import tensorflow as tf


class Optimizer(abc.ABC):

  # The following are factory methods.
  __algorithms__ = {
    "Adadelta": tf.keras.optimizers.Adadelta,
    "Adagrad": tf.keras.optimizers.Adagrad,
    "Adam": tf.keras.optimizers.Adam,
    "Adamax": tf.keras.optimizers.Adamax,
    "Ftrl": tf.keras.optimizers.Ftrl,
    "Nadam": tf.keras.optimizers.Nadam,
    "RMSprop": tf.keras.optimizers.RMSprop,
    "SGD": tf.keras.optimizers.SGD,
  }

  __learning_rate_schedules__ = {
    "exponential_decay": tf.keras.optimizers.schedules.ExponentialDecay,
    "inverse_time_decay": tf.keras.optimizers.schedules.InverseTimeDecay,
    "polynomial_decay": tf.keras.optimizers.schedules.PolynomialDecay,
    "piecewise_constant_decay":
        tf.keras.optimizers.schedules.PiecewiseConstantDecay,
  }

  @staticmethod
  def register(name, new_class, class_type="algorithm"):
    """Register an algorithm or learning rate schedule."""
    if class_type == "algorithm":
      Optimizer.__algorithms__[name] = new_class
    elif class_type == "learning_rate_schedule":
      Optimizer.__learning_rate_schedules__[name] = new_class
    else:
      raise ValueError(
          "'class_type' must be 'algorithm' or 'learning_rate_schedule'.")

  @staticmethod
  def create(algorithm, learning_rate, **params):
    """Create an optimizer.

    Parameters
    ----------
    algorithm: str
      The algorithm of the optimizer.

    learning_rate: float, dict
      The learning rate or learning rate parameters of the optimize algorithm.

    Returns
    -------
    optimizer: tf.keras.optimizers.Optimizer
      An instance of tf.keras.optimizers.Optimizer.
    """

    if isinstance(learning_rate, dict):
      learning_rate_params = copy.deepcopy(learning_rate)
      schedule = learning_rate_params.pop("schedule")
      initial_learning_rate = learning_rate_params.pop("initial_value")
      learning_rate = Optimizer.create_learning_rate_schedule(
          schedule, initial_learning_rate, learning_rate_params)

    algorithm_cls = Optimizer.__algorithms__[algorithm]
    params = copy.deepcopy(params)
    params["learning_rate"] = learning_rate
    parameters = utils.func.extract_kwargs(algorithm_cls, params, raises=True)
    if issubclass(algorithm_cls, tf.keras.optimizers.Optimizer):
      for param_name in ["clipnorm", "clipvalue", "lr", "decay"]:
        if param_name not in params:
          continue
        parameters[param_name] = params[param_name]
    return algorithm_cls(**parameters)

  @staticmethod
  def create_learning_rate_schedule(schedule_name,
                                    initial_learning_rate,
                                    params):
    """Create an instance of LearningRateScheduler object
    which can manage the learning rate during training.

    Parameters
    ----------
    schedule_name: str
      The name of the learning rate scheduler.

    initial_learning_rate: float
      The initial value of learning rate.

    params: dict
      The paramters to initialize the LearningRateScheduler object.

    Returns
    -------
    scheduler: LearningRateScheduler
      An instance of LearningRateScheduler.
    """

    params = copy.deepcopy(params)
    params["initial_learning_rate"] = initial_learning_rate

    schedule = Optimizer.__learning_rate_schedules__[schedule_name]
    parameters = utils.func.extract_kwargs(schedule, params, raises=True)
    return schedule(**parameters)
