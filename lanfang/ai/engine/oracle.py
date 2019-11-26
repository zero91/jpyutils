from lanfang.ai.engine.base.dataset import Dataset
from lanfang.ai.engine.base.model import Model
from lanfang.ai.engine.base.optimizer import Optimizer
#from lanfang.ai.engine.base import resource
from lanfang.ai.engine.utils import hooks

import json
import os
import logging
import copy
import abc
import tensorflow as tf


class BaseOracle(abc.ABC):
  """Base Oracle class for train/test machine learning related models.
  """

  def __init__(self, params=None):
    self._load_params(params)

    self._m_dataset = Dataset.create(self._m_experiment["dataset"])
    logging.info("Dataset info: %s", json.dumps(
        self._m_dataset.get_params(), indent=2, sort_keys=True))

    model_params = copy.deepcopy(self._m_dataset.get_params())
    model_params.update(self._m_hparams["graph"])
    self._m_model = Model.create(self._m_experiment["model"], **model_params)
    logging.info("Model info: %s", json.dumps(
        self._m_model.get_params(), indent=2, sort_keys=True))

    self._m_model_dir = self._get_model_dir()
    self._m_model_operator = self.create_model_operator()

  # The following are abstract methods.
  @abc.abstractmethod
  def create_model_operator(self):
    """Create an operator to manipulate model.
    """
    pass

  @abc.abstractmethod
  def train(self, overwrite=False):
    pass

  @abc.abstractmethod
  def evaluate(self, splits):
    pass

  @abc.abstractmethod
  def predict(self, splits):
    pass

  # The following are class instance methods.
  def prepare(self):
    self._m_dataset.prepare()
    self._m_model.prepare()

  def evaluate_on_data(self, x, y):
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

    return self._m_model_operator.evaluate(x, y)

  def predict_on_data(self, x):
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

    return self._m_model_operator.predict(x)

  def _check_params(self, params):
    required_keys = ["experiment", "hparams", "computing_resources"]
    for required_key in required_keys:
      if required_key not in params:
        raise KeyError("Missing required key: %s" % (required_key))

    invalid_keys = set(params) - set(required_keys)
    if len(invalid_keys) > 0:
      raise KeyError(
          "Parameters contains invalid keys: %s." % (", ".join(invalid_keys)))

  def _load_params(self, params):
    root_path = os.path.dirname(os.path.realpath(__file__))
    param_fname = os.path.join(
        root_path, 'params', 'default_oracle_params.json')

    with open(param_fname, 'r') as fin:
      run_params = json.load(fin)

    def update_params(all_params, p):
      if p is None:
        return
      if not isinstance(p, dict):
        raise TypeError("'params' must be a dict, '%s' received" % p)

      for key, value in p.items():
        if key not in all_params or not isinstance(value, dict) \
                                 or not isinstance(all_params[key], dict):
          all_params[key] = value
        else:
          update_params(all_params[key], value)
    update_params(run_params, params)
    self._check_params(run_params)

    self._m_experiment = run_params["experiment"]
    self._m_hparams = run_params["hparams"]
    self._m_computing_resources = run_params["computing_resources"]
    logging.info("Running parameters: %s", json.dumps(
        run_params, indent=2, sort_keys=True))

  def _get_optimizer(self):
    optimizer_params = copy.deepcopy(self._m_hparams["optimizer"])
    algorithm = optimizer_params.pop("algorithm")
    learning_rate = optimizer_params.pop("learning_rate")
    return Optimizer.create(algorithm, learning_rate, **optimizer_params)

  def _get_model_dir(self):
    model_dir = "{0}/{1}/{2}/{3}".format(
        self._m_experiment["output_dir"],
        self._m_experiment["dataset"],
        self._m_experiment["model"],
        self._m_experiment["tag"])
    if not os.path.exists(model_dir):
      os.makedirs(model_dir)
    return model_dir


class KerasOracle(BaseOracle):
  """A keras based oracle."""
  def create_model_operator(self):
    model_operator = self._m_model.create_model()
    model_operator.compile(
        optimizer=self._get_optimizer(),
        loss=self._m_model.get_losses_fn(),
        metrics=self._m_model.get_metrics())

    if self._m_hparams.get("checkpoint") is not None:
      model_operator.load_weights(self._m_hparams["checkpoint"])

    return model_operator

  def train(self, overwrite_params=False):
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

    params_fname = os.path.join(self._m_model_dir, "params.json")
    if os.path.exists(params_fname) and not overwrite_params:
      with open(params_fname, "r") as fp:
        if json.load(fp) != self._m_hparams:
          raise RuntimeError("Mismatching parameters found.")
    else:
      with open(params_fname, "w") as fp:
        out = json.dumps(self._m_hparams, indent=2, sort_keys=True)
        fp.write(out)

    callbacks = []

    model_selector = self._m_computing_resources["model_selector"]
    # callback for saving checkpoint 
    checkpoint_format = "model.{epoch:02d}-{val_%s:.6f}" % (
        model_selector["eval_metric"])
    model_path = os.path.join(self._m_model_dir, checkpoint_format)
    callbacks.append(tf.keras.callbacks.ModelCheckpoint(
        model_path,
        monitor=model_selector["eval_metric"],
        save_best_only=True,
        #save_weights_only=True,
        mode=model_selector["mode"],
        save_freq=model_selector["save_checkpoints_steps"]))

    # callback for early stopping
    callbacks.append(tf.keras.callbacks.EarlyStopping(
        monitor=model_selector["eval_metric"],
        patience=model_selector["early_stopping_patient_epochs"],
        mode=model_selector["mode"]))

    prefetch_buffer_size = self._m_computing_resources["prefetch_buffer_size"]
    train_input_fn = self._m_dataset.get_input_fn(
        self._m_experiment["train_splits"],
        batch_size=self._m_hparams["compute"]["batch_size"],
        num_epochs=1, #self._m_hparams["compute"]["num_train_epochs"],
        shuffle_batches=self._m_hparams["compute"]["shuffle_batches"],
        prefetch_buffer_size=prefetch_buffer_size,
        cache=self._m_computing_resources["cache_data"])
    train_data = train_input_fn(tf.estimator.ModeKeys.TRAIN, config=None)

    dev_input_fn = self._m_dataset.get_input_fn(
        self._m_experiment["dev_splits"],
        batch_size=self._m_hparams["compute"]["batch_size"],
        prefetch_buffer_size=prefetch_buffer_size)
    dev_data = dev_input_fn(tf.estimator.ModeKeys.EVAL, config=None)

    history = self._m_model_operator.fit(
        train_data,
        epochs=self._m_hparams["compute"]["num_train_epochs"],
        shuffle=False,
        validation_data=dev_data,
        callbacks=callbacks)
    return history

  def evaluate(self, splits):
    if not isinstance(splits, (list, tuple)):
      splits = [splits]

    res = {}
    batch_size = self._m_hparams["compute"]["batch_size"]
    prefetch_buffer_size = self._m_computing_resources["prefetch_buffer_size"]
    for split in splits:
      eval_input_fn = self._m_dataset.get_input_fn(
          split, batch_size=batch_size,
          prefetch_buffer_size=prefetch_buffer_size)
      eval_data = eval_input_fn(tf.estimator.ModeKeys.EVAL, config=None)
      metrics = self._m_model_operator.evaluate(eval_data)
      res[split] = dict(zip(self._m_model_operator.metrics_names, metrics))
    return res

  def predict(self, splits):
    if not isinstance(splits, (list, tuple)):
      splits = [splits]

    res = {}
    prefetch_buffer_size = self._m_computing_resources["prefetch_buffer_size"]
    for split in splits:
      input_fn = self._m_dataset.get_input_fn(
          self._m_experiment["dev_splits"],
          batch_size=self._m_hparams["compute"]["batch_size"],
          prefetch_buffer_size=prefetch_buffer_size)
      input_data = input_fn(tf.estimator.ModeKeys.PREDICT, config=None)
      res[split] = self._m_model_operator.predict(input_data)
    return res


class EstimatorOracle(BaseOracle):
  def train(self):
    train_hooks = []

    batch_size = self._m_hparams["compute"]["batch_size"]
    stat_n_steps = self._m_computing_resources["save_summary_steps"]
    train_hooks.append(
        hooks.ExamplesPerSecondHook(
            batch_size=batch_size,
            every_n_steps=stat_n_steps))
    """
      tf.train.CheckpointSaverHook(
        self._m_model_dir,
        save_steps=run_info["save_checkpoints_steps"],
        listeners=[
          hooks.BestCheckpointKeeper(
            self._m_model_dir,
            eval_fn=self.evaluate,
            eval_sets=run_info["dev_sets"],
            metric_name=run_info["checkpoint_selector"]["eval_metric"],
            higher_is_better=\
              run_info["checkpoint_selector"]["higher_is_better"])
        ]),
      hooks.early_stop_if_necessary_hook(
        self._m_estimator,
        metric_name=run_info["early_stopping"]["eval_metric"],
        max_steps_without_gain=\
          run_info["early_stopping"]["max_steps_without_gain"],
        higher_is_better=run_info["early_stopping"]["higher_is_better"],
        eval_dir=self._m_estimator.eval_dir(
          self._get_eval_sets_key(run_info["dev_sets"])),
        min_steps=run_info["early_stopping"]["min_steps"],
        run_every_secs=None,
        run_every_steps=run_info["save_checkpoints_steps"])
    """

  def _build_estimator(self):
    session_config = tf.compat.v1.ConfigProto()
    session_config.allow_soft_placement = True
    session_config.gpu_options.allow_growth = True

    run_config = tf.estimator.RunConfig(
        model_dir=self._m_model_dir,
        tf_random_seed=self._m_computing_resources["random_seed"],
        save_summary_steps=self._m_computing_resources["save_summary_steps"],
        save_checkpoints_steps=\
            self._m_computing_resources["save_checkpoints_steps"],
        save_checkpoints_secs=None,
        keep_checkpoint_max=self._m_computing_resources["keep_checkpoint_max"],
        log_step_count_steps=\
            self._m_computing_resources["log_step_count_steps"],
        session_config=session_config)

    model = self._m_model.get_model()

    model.compile(
        optimizer=self._get_optimizer(),
        loss=self._m_model.get_losses_fn(),
        metrics=self._m_model.get_metrics())

    keras_estimator = tf.keras.estimator.model_to_estimator(
        keras_model=model,
        model_dir=self._m_model_dir,
        config=run_config)
    return keras_estimator


class __Expired(object):
  def _wrap_model_fn(self, features, labels, mode, params):
    spec = self._m_model(features, labels, mode, params)
    scaffold = self._build_scaffold(params)
    return tf.estimator.EstimatorSpec(
      mode=spec.mode,
      predictions=spec.predictions,
      loss=spec.loss,
      train_op=spec.train_op,
      eval_metric_ops=spec.eval_metric_ops,
      scaffold=scaffold)

  def _build_scaffold(self, params):
    scaffold_info = self._m_model.get_scaffold_params(params)
    for scaffold_key in scaffold_info:
      for key, value in scaffold_info[scaffold_key].items():
        try:
          if resource.Resource.match(value):
            logging.info("Match Resource for '%s'", value)
            resource_manager = resource.Resource.create(value, params=params)
            value = resource_manager.read(
                value, self._m_model, self._m_dataset, params)
          else:
            # For future
            pass
          scaffold_info[scaffold_key][key] = value
        except KeyError as ke:
          logging.warning("Exception occurred: %s", ke)
          continue
    return tf.train.Scaffold(**scaffold_info)

  def export(self):
    serving_input_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(
        self._m_model.get_features(self._m_params["hparams"]))

    self._m_estimator.export_saved_model(
      os.path.join(self._m_model_dir, "export"),
      serving_input_fn)

    tf.reset_default_graph()
    with tf.Session() as sess:
      features = serving_input_fn().features
      spec = self._m_model(
          features, None, tf.estimator.ModeKeys.PREDICT, self._m_hparams)
      logging.info("Features %s", {k: v.name for k, v in features.items()})
      logging.info("Predictions %s", spec.predictions.name)
      tf.train.write_graph(
          sess.graph_def, self._m_model_dir, 'graph_eval.pbtxt')
    return os.path.join(self._m_model_dir, 'graph_eval.pbtxt')
