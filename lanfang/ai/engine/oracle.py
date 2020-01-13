from lanfang.ai.engine.dataset import Dataset
from lanfang.ai.engine.model import Model
from lanfang.ai.engine.model import KerasModel
from lanfang.ai.engine.optimizer import KerasOptimizer
from lanfang.utils import func

import abc
import copy
import os
import json
import logging
import shutil

import tensorflow as tf


class OracleConfig(object):
  def __init__(self, *, tag=None,
                        output_dir=None,
                        dataset_params=None,
                        model_params=None,
                        optimizer_params=None,
                        model_selector_params=None,
                        tensorboard_params=None):
    self._m_tag = tag
    self._m_output_dir = output_dir
    self._setup_params(
        dataset_params=dataset_params,
        model_params=model_params,
        optimizer_params=optimizer_params,
        model_selector_params=model_selector_params,
        tensorboard_params=tensorboard_params)

  @property
  def tag(self):
    return self._m_tag

  @property
  def output_dir(self):
    return self._m_output_dir

  @property
  def dataset_params(self):
    return copy.deepcopy(self._m_dataset_params)

  @property
  def model_params(self):
    return copy.deepcopy(self._m_model_params)

  @property
  def optimizer_params(self):
    return copy.deepcopy(self._m_optimizer_params)

  @property
  def model_selector(self):
    return copy.deepcopy(self._m_model_selector_params)

  @property
  def tensorboard(self):
    return copy.deepcopy(self._m_tensorboard_params)

  def _setup_params(self, *, dataset_params,
                             model_params,
                             optimizer_params,
                             model_selector_params,
                             tensorboard_params):
    root_path = os.path.dirname(os.path.realpath(__file__))
    config_fname = "{}/config/default_oracle_config.json".format(root_path)
    with open(config_fname, 'r') as fin:
      config = json.load(fin)

    if self._m_output_dir is None:
      self._m_output_dir = config["output_dir"]

    if self._m_tag is None:
      self._m_tag = config["tag"]

    config_items = [
        "dataset",
        "model",
        "optimizer",
        "model_selector",
        "tensorboard"
    ]

    for item in config_items:
      setattr(self, "_m_" + item + "_params", config[item])
      item_value = locals()[item + "_params"]
      if item_value is not None:
        getattr(self, "_m_" + item + "_params").update(item_value)


class BaseOracle(abc.ABC):
  def __init__(self, *, tag=None,
                        output_dir=None,
                        dataset_params=None,
                        model_params=None,
                        optimizer_params=None,
                        model_selector_params=None,
                        tensorboard_params=None):
    self._m_config = OracleConfig(
        tag=tag,
        output_dir=output_dir,
        dataset_params=dataset_params,
        model_params=model_params,
        optimizer_params=optimizer_params)

    self._m_model_dir = "{0}/{1}/{2}/{3}".format(
        self._m_config.output_dir,
        self._m_config.dataset_params["name"],
        self._m_config.model_params["name"],
        self._m_config.tag)

  @abc.abstractmethod
  def initialize(self):
    pass

  @abc.abstractmethod
  def train(self, overwrite=False):
    pass

  @abc.abstractmethod
  def evaluate(self, splits):
    pass

  @abc.abstractmethod
  def evaluate_data(self, x, y):
    pass

  @abc.abstractmethod
  def predict(self, splits):
    pass

  @abc.abstractmethod
  def predict_data(self, x):
    pass

  def export(self):
    pass


class KerasOracle(BaseOracle):
  def __init__(self, **kwargs):
    super(self.__class__, self).__init__(**kwargs)

    self._m_dataset = self._create_dataset()
    self._m_model = self._create_model()
    self._m_optimizer = self._create_optimizer()
    self._m_keras_model = None

  def initialize(self):
    self._m_dataset.prepare()
    self._m_model.prepare()

    self._m_keras_model = self._m_model.model_fn()
    self._m_keras_model.compile(
        optimizer=self._m_optimizer,
        loss=self._m_model.losses_fn(),
        metrics=self._m_model.metrics_fn())
    self._m_keras_model.summary()

    if "checkpoint" not in self._m_model.parameters():
      checkpoint = self._m_config.model_params["checkpoint"]
      if checkpoint is not None:
        self._m_keras_model.load_weights(checkpoint)

  def train(self, overwrite=True):
    """Train a model.

    Parameters
    ----------
    overwrite_params: bool
      Whether to overwrite existing params or not.

    Returns
    -------
    history: tf.keras.callbacks.History
      A `History` object. Its `History.history` attribute is
      a record of training loss values and metrics values
      at successive epochs, as well as validation loss values
      and validation metrics values (if applicable).

    Raises
    ------
    RuntimeError: If the parameters doesn't match the existing parameters.
    """

    if self._m_keras_model is None:
      raise RuntimeError("KerasOracle must be initialized first.")

    #params_fname = os.path.join(self._m_model_dir, "params.json")
    #if os.path.exists(params_fname) and not overwrite_params:
    #  with open(params_fname, "r") as fp:
    #    if json.load(fp) != self._m_hparams:
    #      raise RuntimeError("Mismatching parameters found.")
    #else:
    #  with open(params_fname, "w") as fp:
    #    out = json.dumps(self._m_hparams, indent=2, sort_keys=True)
    #    fp.write(out)

    callbacks = []

    # callback for saving checkpoint 
    selector = self._m_config.model_selector
    model_path = os.path.join(
        self._m_model_dir,
        "model.{epoch:02d}-{%s:.6f}" % (selector["eval_metrics"]))

    callbacks.append(tf.keras.callbacks.ModelCheckpoint(
        model_path,
        monitor=selector["eval_metrics"],
        save_best_only=selector["save_best_only"],
        save_weights_only=selector["save_weights_only"],
        mode=selector["mode"],
        save_freq=selector["save_freq"]))

    # callback for early stopping
    callbacks.append(tf.keras.callbacks.EarlyStopping(
        monitor=selector["eval_metrics"],
        patience=selector["early_stopping_patient_epochs"],
        mode=selector["mode"]))

    # callback for TensorBoard
    tb_kwargs = func.extract_kwargs(
        tf.keras.callbacks.TensorBoard,
        self._m_config.tensorboard)
    if "log_dir" not in tb_kwargs:
      tb_kwargs["log_dir"] = "{}/tensorboard".format(self._m_model_dir)
    if os.path.exists(tb_kwargs["log_dir"]):
      shutil.rmtree(tb_kwargs["log_dir"])

    callbacks.append(tf.keras.callbacks.TensorBoard(**tb_kwargs))

    # train data
    dataset_params = self._m_config.dataset_params
    train_input_fn = self._m_dataset.data_fn(
        dataset_params["train_splits"],
        batch_size=dataset_params["batch_size"],
        num_epochs=1,
        shuffle_batches=dataset_params["shuffle_batches"],
        prefetch_buffer_size=dataset_params["prefetch_buffer_size"])
    train_data = train_input_fn(tf.estimator.ModeKeys.TRAIN, config=None)

    # dev data
    dev_input_fn = self._m_dataset.data_fn(
        dataset_params["dev_splits"],
        batch_size=dataset_params["batch_size"],
        num_epochs=1,
        prefetch_buffer_size=dataset_params["prefetch_buffer_size"])
    dev_data = dev_input_fn(tf.estimator.ModeKeys.EVAL, config=None)

    # It seems like a bug in tensorflow==2.0.0 when we can't determine
    # the steps of the validation data for the lazy loading mechanism
    # in tensorflow. So we use this unefficient ways to determine the
    # steps of the validation data.
    validation_steps = tf.data.experimental.cardinality(dev_data)
    if validation_steps == tf.data.experimental.UNKNOWN_CARDINALITY:
      validation_steps = len(list(dev_data))
    elif validation_steps == tf.data.experimental.INFINITE_CARDINALITY:
      validation_steps = None

    history = self._m_keras_model.fit(
        train_data,
        epochs=dataset_params["num_train_epochs"],
        shuffle=False,
        validation_data=dev_data,
        #validation_steps=validation_steps,
        callbacks=callbacks)
    return history

  def evaluate(self, splits):
    if not isinstance(splits, (list, tuple)):
      splits = [splits]

    res = {}
    dataset_params = self._m_config.dataset_params
    for split in splits:
      eval_input_fn = self._m_dataset.data_fn(
          split,
          batch_size=dataset_params["batch_size"],
          num_epochs=1,
          prefetch_buffer_size=dataset_params["prefetch_buffer_size"])
      eval_data = eval_input_fn(tf.estimator.ModeKeys.EVAL, config=None)
      metrics = self._m_keras_model.evaluate(eval_data)
      res[split] = dict(zip(self._m_keras_model.metrics_names, metrics))
    return res

  def evaluate_data(self, x, y):
    """Evaluate model metrics on data.

    Parameters
    ----------
    x: Input samples. It could be:
      - A Numpy array (or array-like), or a list of arrays
        (in case the model has multiple inputs).
      - A TensorFlow tensor, or a list of tensors
        (in case the model has multiple inputs).
      - A `tf.data` dataset.
      - A generator or `keras.utils.Sequence` instance.

    y: Target data. Like the input data `x`,
      it could be either Numpy array(s) or TensorFlow tensor(s).
      It should be consistent with `x` (you cannot have Numpy inputs and
      tensor targets, or inversely).
      If `x` is a dataset, generator or
      `keras.utils.Sequence` instance, `y` should not be specified (since
      targets will be obtained from the iterator/dataset).

    Returns
    -------
    predictions: np.array
      Numpy array(s) of predictions.
    """

    return self._m_keras_model.evaluate(x, y)


  def predict(self, splits):
    if not isinstance(splits, (list, tuple)):
      splits = [splits]

    res = {}
    dataset_params = self._m_config.dataset_params
    for split in splits:
      input_fn = self._m_dataset.data_fn(
          split,
          batch_size=dataset_params["batch_size"],
          num_epochs=1,
          prefetch_buffer_size=dataset_params["prefetch_buffer_size"])
      input_data = input_fn(tf.estimator.ModeKeys.PREDICT, config=None)
      res[split] = self._m_keras_model.predict(input_data)
    return res

  def predict_data(self, x):
    """Generates output predictions for the input samples.

    Parameters
    ----------
    x: Input samples. It could be:
      - A Numpy array (or array-like), or a list of arrays
        (in case the model has multiple inputs).
      - A TensorFlow tensor, or a list of tensors
        (in case the model has multiple inputs).
      - A `tf.data` dataset.
      - A generator or `keras.utils.Sequence` instance.
    """

    return self._m_keras_model.predict(x)

  def _create_dataset(self):
    dataset_params = self._m_config.dataset_params

    name = dataset_params.pop('name')
    not_kwargs = {"name", "train_splits", "dev_splits", "test_splits"}
    kwargs = {k: v for k, v in dataset_params.items() if k not in not_kwargs}
    dataset = Dataset.create(name, **kwargs)

    logging.info("Dataset parameters: %s, meta: %s",
        json.dumps(dataset.parameters(), indent=2, sort_keys=True),
        json.dumps(dataset.meta(), indent=2, sort_keys=True))
    return dataset

  def _create_model(self):
    model_kwargs = self._m_dataset.meta()
    model_kwargs.update(self._m_config.model_params)

    name = model_kwargs.pop('name')
    model = Model.create(name, **model_kwargs)
    if not isinstance(model, KerasModel):
      raise TypeError(
          "The model created by KerasOracle is not a KerasModel instance.")

    logging.info("Model parameters: %s", json.dumps(
        model.parameters(), indent=2, sort_keys=True))
    return model

  def _create_optimizer(self):
    optimizer_params = self._m_config.optimizer_params
    algorithm = optimizer_params.pop("algorithm")
    learning_rate = optimizer_params.pop("learning_rate")
    return KerasOptimizer.create(algorithm, learning_rate, **optimizer_params)
