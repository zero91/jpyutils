from lanfang import utils

import abc
import re
import collections
import json
import threading
import copy
import logging

import _jsonnet as jsonnet


class MultiTaskConfig(abc.ABC):
  """Configuration for multiple tasks.
  """

  @abc.abstractmethod
  def get_params(self):
    """Get all the parameters needed for this configuration.
    """
    pass

  @abc.abstractmethod
  def set_params(self, params: dict):
    """Set all the parameters needed for this configuration.
    """
    pass

  @abc.abstractmethod
  def update_params(self, params: dict):
    """Update all the parameters needed for this configuration.
    """
    pass

  @abc.abstractmethod
  def update_output(self, task_name: str, output_values: dict):
    """Update the output values of a task.
    """
    pass

  @abc.abstractmethod
  def get_config(self):
    """Get the latest configuration.
    """
    pass


class MultiTaskJsonnetConfig(MultiTaskConfig):
  """Jsonnet format configuration for multiple tasks.
  """

  def __init__(self, config_file):
    self._m_template = re.compile(r"\<\%=(.*?)\%\>")
    self._m_config = None
    self._m_config_updated = True
    # Save all the update values
    self._m_config_output_update_values = collections.defaultdict(dict)
    with open(config_file, 'r') as fin:
      (self._m_all_params, self._m_internal_text,
          self._m_jsonnet_text) = self._initialize(fin.read())
      self._check_config()

    self._m_required_params = {}
    for uniq_id, reg_match in self._m_all_params.items():
      template_value = reg_match.group(1).strip()
      if template_value[0] == '$':
        continue
      self._m_required_params[uniq_id] = template_value
    self._m_required_params_values = {}

    self._m_fetcher_key, self._m_fetcher_snippet = self._make_fetcher_snippet()
    self._m_lock = threading.Lock()

  def get_params(self):
    return set(self._m_required_params.values())

  def set_params(self, params):
    invalid_params = set(params) - self.get_params()
    if len(invalid_params) > 0:
      raise KeyError("Find unknown params: %s" % (",".join(invalid_params)))

    missing_params = self.get_params() - set(params)
    if len(missing_params) > 0:
      raise KeyError("These parameters must be set: %s" % (
          ','.join(missing_params)))

    if params != self._m_required_params_values:
      self._m_lock.acquire()
      self._m_config_updated = True
      self._m_required_params_values = copy.deepcopy(params)
      self._m_lock.release()

  def update_params(self, params):
    invalid_params = set(params) - self.get_params()
    if len(invalid_params) > 0:
      raise KeyError("Find unknown params: %s" % (",".join(invalid_params)))

    self._m_lock.acquire()
    self._m_required_params_values.update(params)
    self._m_config_updated = True
    self._m_lock.release()

  def get_config(self):
    self._m_lock.acquire()
    try:
      if not self._m_config_updated:
        return copy.deepcopy(self._m_config)

      missing_params = self.get_params() - set(self._m_required_params_values)
      if len(missing_params) > 0:
        raise KeyError("You need to set these parameters first: %s" % (
            ','.join(missing_params)))

      self._m_config_updated = False

      # step 1. Fetch jsonnet syntax template values.
      snippet = ""
      def _insert_snippet(text, snippet):
        right_brace_idx = text.rindex('}')
        return text[:right_brace_idx] + ", " + snippet + text[right_brace_idx:]

      if self._m_config is None:
        for param_name, param_value in self._m_required_params_values.items():
          snippet += "local {} = {};\n".format(
              param_name, json.dumps(param_value))
        snippet += _insert_snippet(
            self._m_jsonnet_text, self._m_fetcher_snippet)
      else:
        snippet += _insert_snippet(
            json.dumps(self._m_config), self._m_fetcher_snippet)

      refvalues = json.loads(
          jsonnet.evaluate_snippet("snippet", snippet))[self._m_fetcher_key]

      # step 2. Generate new created dependent config.
      snippet = ""
      for uniq_id, value in refvalues.items():
        snippet += "local {} = {};\n".format(uniq_id, json.dumps(value))
      for uniq_id, param_name in self._m_required_params.items():
        param_value = self._m_required_params_values[param_name]
        snippet += "local {} = {};\n".format(uniq_id, json.dumps(param_value))
      snippet += self._m_internal_text

      self._m_config = json.loads(jsonnet.evaluate_snippet("snippet", snippet))

      # step 3. Re-update all the update history values.
      for task_name, output in self._m_config_output_update_values.items():
        self._m_config[task_name]["output"].update(output)

    finally:
      self._m_lock.release()
    return copy.deepcopy(self._m_config)

  def update_output(self, task_name, output_values):
    if not isinstance(output_values, dict):
      raise TypeError("Parameter 'output_values' must be a dict, "
          "but received %s (%s)" % (type(output_values), output_values))

    self.get_config()
    invalid_keys = set(output_values) - set(self._m_config[task_name]["output"])
    if len(invalid_keys) > 0:
      raise KeyError("In the output of task '%s', find invalid keys: %s" % (
          task_name, ", ".join(invalid_keys)))

    missing_keys = set(self._m_config[task_name]["output"]) - set(output_values)
    if len(missing_keys) > 0:
      logging.warning("Task '%s' didn't output values for keys: %s" % (
          task_name, ", ".join(missing_keys)))

    self._m_lock.acquire()
    try:
      self._m_config_updated = True
      self._m_config[task_name]["output"].update(output_values)
      self._m_config_output_update_values[task_name].update(output_values)
    finally:
      self._m_lock.release()

  def _initialize(self, text):
    candidate_arguments = {} # record all possible arguments
    def record_lookup(reg_match):
      local_var = utils.random.random_str(16)
      while text.find(local_var) != -1:
        local_var = utils.random.random_str(16)
      candidate_arguments[local_var] = reg_match
      return local_var
    candidate_text = self._m_template.sub(record_lookup, text)

    fake_vars = "\n".join(["local %s = 0;" % v for v in candidate_arguments])
    fake_text = jsonnet.evaluate_snippet("snippet", fake_vars + candidate_text)

    arguments = {}
    for local_var, reg_match in candidate_arguments.items():
      if local_var in fake_text:
        # Recover unsatisfied match.
        candidate_text = candidate_text.replace(local_var, reg_match.group())
      else:
        arguments[local_var] = reg_match

    jsonnet_text = candidate_text
    for local_var, reg_match in arguments.items():
      jsonnet_text = jsonnet_text.replace(local_var, reg_match.group(1).strip())

    return arguments, candidate_text, jsonnet_text

  def _make_fetcher_snippet(self):
    fetcher_key = utils.random.random_str(32)
    while self._m_jsonnet_text.find(fetcher_key) != -1:
      fetcher_key = utils.random.random_str(32)

    fetcher_snippet = "%s: {" % (fetcher_key)
    have_item = False
    for uniq_id, reg_match in self._m_all_params.items():
      if uniq_id in self._m_required_params:
        continue
      if have_item:
        fetcher_snippet += ", "
      else:
        have_item = True
      fetcher_snippet += "{}: {}".format(uniq_id, reg_match.group(1).strip())
    fetcher_snippet += "}"
    return fetcher_key, fetcher_snippet

  def _check_config(self):
    # output of task shouldn't contains template.
    setting_1 = "\n".join(["local %s = 123;" % v for v in self._m_all_params])
    setting_2 = "\n".join(["local %s = 999;" % v for v in self._m_all_params])

    config_1 = jsonnet.evaluate_snippet(
        "snippet", setting_1 + self._m_internal_text)
    config_2 = jsonnet.evaluate_snippet(
        "snippet", setting_2 + self._m_internal_text)

    output_1 = {t: p["output"] for t, p in json.loads(config_1).items()}
    for task, params in json.loads(config_2).items():
      if params["output"] != output_1[task]:
        ks = {k for k, v in params["output"].items() if v != output_1[task][k]}
        raise ValueError(
            "Output config of task '%s' contains unallowed template "
            "for keys: %s." % (task, ", ".join(ks)))
        
      if set(params) != {"input", "output"}:
        raise KeyError(
            "Output config of task '%s' must contains and only contains keys "
            "'input' and 'output', but received: %s" % (task, set(params)))

    # input of task with template shouldn't refer to other task's input.
    for uniq_id, reg_match in self._m_all_params.items():
      items = reg_match.group(1).strip().split('.')
      if items[0] == '$' and len(items) >= 3 and items[2] == 'input':
        raise ValueError("Template '%s' refer to other task's input." % (
            reg_match.group(1).strip()))
