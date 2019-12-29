from lanfang.runner.base import SharedData
from lanfang.runner.base import SharedScope
from lanfang.runner.base import RunnerContext
from lanfang.runner.multi_task_config import MultiTaskConfig
from lanfang.utils import disk

import os
import datetime
import json


class RecordRunnerContext(RunnerContext):
  """Context for runner to record input/output parameters.
  """

  def __init__(self):
    self._m_data = SharedData(shared_scope=SharedScope.PROCESS)

  def get_params(self):
    return {}

  def set_params(self, params):
    if params is None:
      return
    if not isinstance(params, dict):
      raise TypeError("Parameter 'params' must be a dict, "
          "but received %s(%s)" % (type(params), params))
    if len(params) > 0:
      raise KeyError("Find unknown params: %s" % (",".join(params)))

  def get_input(self, name):
    try:
      return self._m_data[name]["input"]
    except KeyError as ke:
      return {}

  def set_input(self, name, value):
    if not isinstance(value, dict):
      raise TypeError("Parameter 'value' must be a dict, "
          "but received %s(%s)" % (type(value), value))

    params = self._m_data.get(name, {})
    params.update({"input": value})
    self._m_data.update({name: params})

  def get_output(self, name):
    try:
      return self._m_data[name]["output"]
    except KeyError as ke:
      return {}

  def set_output(self, name, value):
    if not isinstance(value, dict):
      raise TypeError("Parameter 'value' must be a dict, "
          "but received %s(%s)" % (type(value), value))

    params = self._m_data.get(name, {})
    params.update({"output": value})
    self._m_data.update({name: params})

  def save(self, checkpoint_path, max_checkpoint_num=5):
    if checkpoint_path is None:
      return

    if not os.path.exists(checkpoint_path):
      os.makedirs(checkpoint_path)

    if not os.path.isdir(checkpoint_path):
      raise IOError("Target checkpoint path '%s' is not a directory." % (
          checkpoint_path))

    timestamp = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
    checkpoint_file = os.path.join(
        checkpoint_path, "record_context-{}.json".format(timestamp))
    with open(checkpoint_file, 'w') as fout:
      json.dump(dict(self._m_data), fout, indent=2, sort_keys=True)

    disk.keep_files_with_pattern(
        path_dir=checkpoint_path,
        pattern="record_context-\d{8}.\d{6}.json",
        max_keep_num=max_checkpoint_num,
        reverse=True)

    return checkpoint_file

  def restore(self, checkpoint_path):
    with open(checkpoint_path, 'r') as fin:
      for task_name, params in json.load(fin).items():
        if "input" in self._m_data:
          self.set_input(task_name, params["input"])

        if "output" in self._m_data:
          self.set_output(task_name, params["output"])
    return self


class DependentRunnerContext(RecordRunnerContext):
  """Runner context which can manage dependent parameters.

  Parameters
  ----------
  task_config_file: str
    Config file path.
  """

  def __init__(self, *, task_config_file, **params):
    super(self.__class__, self).__init__()
    self._m_config = MultiTaskConfig.create(task_config_file, **params)

  @property
  def params(self):
    return self._m_config.get_params()

  def get_params(self):
    return self._m_config.get_param_values()

  def set_params(self, params):
    self._m_config.set_params(params)
    self._m_data.update(self._m_config.get_config())

  def set_input(self, name, value):
    raise RuntimeError(
        "Can't set the input of task '%s' for MultiTaskRunnerContext, "
        "check the code logic." % name)

  def set_output(self, name, value):
    self._m_config.update_output(name, value)
    self._m_data.update(self._m_config.get_config())

  def save(self, checkpoint_path, max_checkpoint_num=5):
    if checkpoint_path is None:
      return

    if not os.path.exists(checkpoint_path):
      os.makedirs(checkpoint_path)

    if not os.path.isdir(checkpoint_path):
      raise IOError("Target checkpoint path '%s' is not a directory." % (
          checkpoint_path))

    timestamp = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
    checkpoint_file = os.path.join(
        checkpoint_path, "dependent_context-{}.json".format(timestamp))
    with open(checkpoint_file, 'w') as fout:
      json.dump(self._m_config.get_config(), fout, indent=2, sort_keys=True)

    disk.keep_files_with_pattern(
        path_dir=checkpoint_path,
        pattern="dependent_context-\d{8}.\d{6}.json",
        max_keep_num=max_checkpoint_num,
        reverse=True)

    return checkpoint_file

  def restore(self, checkpoint_path):
    with open(checkpoint_path, 'r') as fin:
      for task_name, params in json.load(fin).items():
        if task_name not in self._m_data:
          raise KeyError("Can't find task '%s'" % (task_name))
        self.set_output(task_name, params["output"])
    return self
