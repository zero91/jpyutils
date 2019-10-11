#TODO:
# 1. Add voluptuous.infer function to infer value type from default values.
#

from lanfang import runner

import functools
import inspect
import logging
import copy
import json
import time

import voluptuous


class TaskSchema(object):
  """Task schema manager."""

  def __init__(self, *, locals=None, globals=None):
    """
    Parameters
    ----------
    locals: dict
      Maintain local parameters, format (param_name, param_type).

    globals: dict
      Maintain global parameters which will be shared with other tasks,
      format the same as 'locals'.

    """
    if locals is None:
      self._m_locals = {}
    else:
      if not isinstance(locals, dict):
        raise TypeError("Parameter 'locals' must be a dict.")
      self._m_locals = copy.deepcopy(locals)

    if globals is None:
      self._m_globals = {}
    else:
      if not isinstance(globals, dict):
        raise TypeError("Parameter 'globals' must be a dict.")
      self._m_globals = copy.deepcopy(globals)

    self._m_all_items = copy.deepcopy(self._m_locals)
    self._m_all_items.update(self._m_globals)
    if len(self._m_all_items) != len(self._m_locals) + len(self._m_globals):
      duplicate_fields = set(self._m_locals) & set(self._m_globals)
      raise KeyError("globals and locals have the same fields: %s" % (
          ", ".join(sorted(duplicate_fields))))

    self._m_checker = voluptuous.Schema(self._m_all_items)

  def check(self, value):
    return self._m_checker(value)

  def partial_check(self, value):
    if value is None:
      return
    if not isinstance(value, dict):
      raise TypeError("Parameter 'value' must be a dict.")

    for k in value:
      if k not in self._m_all_items:
        raise KeyError("key '%s' is not allowed for this schema." % (k))

    value_schema_dict = {name: self._m_all_items[name] for name in value}
    return voluptuous.Schema(value_schema_dict)(value)

  @property
  def locals(self):
    return list(self._m_locals.keys())

  @property
  def globals(self):
    return list(self._m_globals.keys())

  @property
  def all(self):
    return list(self._m_all_items.keys())


class TaskRegister(object):
  __tasks__ = []

  def __init__(self, output_schema, *,
                     name=None,
                     input_schema=None,
                     pre_hooks=None,
                     post_hooks=None,
                     encoding="utf-8",
                     daemon=None,
                     append_log=False,
                     input_default=None,
                     output_default=None):

    if isinstance(output_schema, TaskSchema):
      self._m_output_schema = output_schema
    else:
      self._m_output_schema = TaskSchema(globals=output_schema)
    self._m_output_default = {} if output_default is None else output_default
    self._m_output_schema.partial_check(self._m_output_default)

    self._m_name = name

    self._m_input_default = input_default if input_default is not None else {}
    if input_schema is not None:
      if isinstance(input_schema, TaskSchema):
        self._m_input_schema = input_schema
      else:
        self._m_input_schema = TaskSchema(globals=input_schema)
      self._m_input_schema.partial_check(self._m_input_default)
    else:
      self._m_input_schema = None

    self._m_pre_hooks = []
    self._m_post_hooks = []
    for var_type, param_hooks, self_hooks in [
            ("pre_hooks", pre_hooks, self._m_pre_hooks),
            ("post_hooks", post_hooks, self._m_post_hooks)]:
      if param_hooks is None:
        continue
      try:
        self_hooks.extend(iter(param_hooks))
      except TypeError as te:
        logging.warning("Parameter '%s' must be an iterable object.", var_type)
        raise te

    self._m_encoding = encoding
    self._m_daemon = daemon
    self._m_append_log = append_log
    self._m_task = None

  def __call__(self, target, **kwargs):
    return self.register(target, **kwargs)

  def register(self, target, **kwargs):
    if self._m_task is not None:
      raise RuntimeError("Only one task can be registered.")
    self._m_task = {}

    name = self._m_name
    if callable(target):
      target, pre_hooks, post_hooks = self._add_func_task(target)
      if self._m_name is None:
        name = target.__name__
    else:
      if self._m_name is None:
        raise ValueError("Parameter 'name' of this Register is None "
            "which is not allowed when the target is a command.")
      target, pre_hooks, post_hooks = self._add_cmd_task(target)

    pre_hooks.extend(self._m_pre_hooks)
    post_hooks.extend(self._m_post_hooks)

    self._m_task["name"] = name
    self._m_task["output_schema"] = self._m_output_schema
    self._m_task["input_default"] = self._m_input_default
    self._m_task["output_default"] = self._m_output_default
    self._m_task["exec_params"] = {
      "target": target,
      "name": name,
      "pre_hooks": pre_hooks,
      "post_hooks": post_hooks,
      "append_log": self._m_append_log,
      "encoding": self._m_encoding,
      "daemon": self._m_daemon,
    }
    self.__tasks__.append(self._m_task)
    return target

  def _add_cmd_task(self, cmd):
    pre_hooks = []
    if self._m_input_schema is not None:
      def check_cmd_input(cmd, params):
        self._m_input_schema.check(params)
      pre_hooks.append(check_cmd_input)
    self._m_task["input_schema"] = self._m_input_schema

    post_hooks = [self._check_output]
    return cmd, pre_hooks, post_hooks

  def _add_func_task(self, func):
    func_schema, func_default = self._inspect_func_schema(func)
    if self._m_input_schema is None:
      input_schema = TaskSchema(globals=func_schema)
      input_schema.partial_check(self._m_input_default)
    else:
      input_schema = self._m_input_schema
    self._m_task["input_schema"] = input_schema

    input_schema.partial_check(func_default)
    for param_name, param_value in func_default.items():
      if param_name in self._m_input_default:
        logging.warning("Default value of parameter '{}' has been reset "
                        "from '{}' to '{}', type = {}".format(
                    param_name, param_value,
                    self._m_input_default[param_name],
                    func_schema[param_name]))
        continue
      self._m_input_default[param_name] = param_value

    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
      # check input
      params = dict(zip(inspect.signature(func).parameters, args))
      params.update(kwargs)
      input_schema.check(params)

      output = func(*args, **kwargs)

      # check output
      self._check_output(output)
      return output
    return func_wrapper, [], []

  def _check_output(self, output_values):
    self._m_output_schema.check(output_values)

  def _inspect_func_schema(self, func):
    sig = inspect.signature(func)
    func_schema = {}
    func_default = {}
    for name, param in sig.parameters.items():
      if param.annotation == inspect.Parameter.empty:
        func_schema[name] = object
      else:
        func_schema[name] = param.annotation

      if param.default != inspect.Parameter.empty:
        func_default[name] = param.default
    return func_schema, func_default

  @classmethod
  def spawn(cls, feed_dict={}, signature_map=None, runner_creater=None):
    helper = _TaskRegisterHelper(cls, signature_map)

    task_params, required_params, task_relations = helper.analysis()

    missing_params = required_params - set(feed_dict)
    if len(missing_params) > 0:
      raise ValueError("Required parameter%s %s missing: %s" % (
          's' if len(missing_params) > 1 else '',
          "are" if len(missing_params) > 1 else "is",
          ", ".join(missing_params)))

    global_key = runner.multi_task_runner.MultiTaskParams.__GLOBAL_KEY__
    extra_params = set(feed_dict) - set(task_params[global_key])
    if len(extra_params) > 0:
      raise ValueError("Extra parameter%s %s received: %s, "
          "which is not allowed." % (
              's' if len(extra_params) > 1 else '',
              "are" if len(extra_params) > 1 else "is",
              ", ".join(extra_params)))
    for param_name, param_value in feed_dict.items():
      task_params[global_key][param_name] = param_value

    if runner_creater is not None:
      if not callable(runner_creater):
        raise TypeError("Parameter 'runner_creater' must be a callable object.")
      scheduler = runner_creater(task_params)
    else:
      scheduler = runner.MultiTaskRunner(params=task_params)

    for task in cls.__tasks__:
      scheduler.add(depends=task_relations[task['name']], **task["exec_params"])
    return scheduler


class _TaskRegisterHelper(object):
  def __init__(self, register_cls, signature_map=None):
    self._m_cls = register_cls
    self._m_global_params_key = \
        runner.multi_task_runner.MultiTaskParams.__GLOBAL_KEY__
    self._m_signature_map = self._parse_signature_map(signature_map)

  def _parse_signature_map(self, signature_map):
    if signature_map is None:
      return {}

    if not isinstance(signature_map, dict):
      raise TypeError("Parameter 'signature_map' must be a dict.")

    new_signature_map = {}
    for task_name, (input_map, output_map) in signature_map.items():
      if input_map is None: 
        input_map = {}
      if not isinstance(input_map, dict):
        raise TypeError("input map of task '%s' must be a dict, "
            "but received %s[%s]" % (task_name, type(input_map), input_map))

      if output_map is None:
        output_map = {}
      if not isinstance(output_map, dict):
        raise TypeError("output map of task '%s' must be a dict, "
            "but received %s[%s]" % (task_name, type(output_map), output_map))

      new_signature_map[task_name] = {
        "input": copy.deepcopy(input_map),
        "output": copy.deepcopy(output_map),
      }
    self._check_signature_map(new_signature_map)
    return new_signature_map

  def _check_signature_map(self, signature_map):
    signature_map = copy.deepcopy(signature_map)
    for task in self._m_cls.__tasks__:
      if task['name'] not in signature_map:
        continue

      for io_type in ["input", "output"]:
        signature_set = set(signature_map[task['name']][io_type])
        global_param_set = set(task[io_type + '_schema'].globals)

        invalid_sig = signature_set - global_param_set
        if len(invalid_sig) == 0:
          continue

        local_param_set = set(task[io_type + '_schema'].locals)
        if len(local_param_set & invalid_sig) > 0:
          raise ValueError("Local %s parameters '%s' of task '%s' "
              "can not be set by using signature map" % (
                  io_type, ", ".join(sorted(local_param_set & invalid_sig)),
                  task['name']))
        else:
          raise ValueError("Find invalid %s signature map values '%s' "
              "of task '%s' which is not exists" % (
                  io_type, ", ".join(invalid_sig), task['name']))
      signature_map.pop(task['name'])

    if len(signature_map) > 0:
      raise ValueError("Find %d nonexistent task%s: %s in signature." % (
          len(signature_map),
          "s" if len(signature_map) > 1 else "",
          ", ".join(sorted(signature_map))))

  def analysis(self):
    task_params = self._extract_task_params()
    task_params, task_relations = self._extract_task_relations(task_params)
    task_params, required_params = self._fill_default_value(task_params)
    return task_params, required_params, task_relations

  def _extract_task_params(self):
    """Extract basic input and output parameters from all tasks.
    """
    task_params = {self._m_global_params_key: {}}
    for task in self._m_cls.__tasks__:
      if task["input_schema"] is None:
        inputs = {}
      else:
        inputs = {item: None for item in task["input_schema"].all}

      if task["output_schema"] is None:
        outputs = {}
      else:
        outputs = {item: None for item in task["output_schema"].all}
      task_params[task["name"]] = {"input": inputs, "output": outputs}
    return task_params

  def _extract_task_relations(self, task_params):
    """Extract task relations and modify the basic task parameters dict
    to reflect its dependency relations.

    For each input parameter of all tasks:
      Scan the outputs of all tasks, if the name of the parameter
      is appeared exactly once, the dependency relation is set between
      the two tasks. If it appreared more than once, a ValueError
      exception is raised since it don't know which one can be used.
      If the name of the parameter is never appeared, a global parameter
      with the same name is added.

    """
    task_params = copy.deepcopy(task_params)
    task_relations = {}
    for task in self._m_cls.__tasks__:
      task_relations[task["name"]] = []
      if task["input_schema"] is None:
        continue

      for param_name in task["input_schema"].globals:
        param_map_name = self._find_map_name(task['name'], "input", param_name)
        from_task_info = self._find_param_src_task(task['name'], param_name)

        if from_task_info is not None:
          from_task = from_task_info.split(".")[0]
          if from_task not in task_relations[task['name']]:
            task_relations[task['name']].append(from_task)
          task_params[task['name']]["input"][param_map_name] = from_task_info

        else:
          task_params[task['name']]["input"][param_name] = param_map_name
          task_params[self._m_global_params_key][param_map_name] = None

    return task_params, task_relations

  def _find_map_name(self, task_name, io_type, param_name):
    if task_name not in self._m_signature_map:
      return param_name
    return self._m_signature_map[task_name][io_type].get(param_name, param_name)

  def _find_param_src_task(self, task_name, param_name, io_type="input"):
    param_map_name = self._find_map_name(task_name, io_type, param_name)

    tasks = []
    for task in self._m_cls.__tasks__:
      if task['name'] == task_name:
        continue
      for out in task["output_schema"].globals:
        if param_map_name == self._find_map_name(task['name'], "output", out):
          tasks.append(task['name'] + "." + out)

    if len(tasks) > 1:
      raise ValueError("Parameter '{}' from task '{}' "
          "depends on {} tasks/outputs: {}".format(param_name,
                      task_name, len(tasks), ", ".join(tasks)))
    elif len(tasks) == 1:
      return tasks[0]
    else:
      return None

  def _fill_default_value(self, task_params):
    task_params = copy.deepcopy(task_params)
    required_params = set()

    # set default values of global parameters
    for param in task_params[self._m_global_params_key]:
      default_values = []

      for task in self._m_cls.__tasks__:
        for key, value in task_params[task["name"]]["input"].items():
          if value != param or key not in task["input_default"]:
            continue
          if task["input_default"][key] not in default_values:
            default_values.append(task["input_default"][key])

      if len(default_values) == 1:
        task_params[self._m_global_params_key][param] = default_values[0]
      else:
        if len(default_values) > 1:
          logging.warning("Find multiple default values of parameter '%s': %s, "
              "set it to None.", param, repr(default_values))
        required_params.add(param)

    # set default values of local parameters
    for task in self._m_cls.__tasks__:
      for key, value in task_params[task['name']]["input"].items():
        if value is not None: # not a local parameter
          continue
        if key in task["input_default"]:
          task_params[task['name']]["input"][key] = task["input_default"][key]
        else:
          raise ValueError("Local parameter '%s' of task '%s' "
              "should set a default value. Set it a global parameter if "
              "you don't want to set a default value." % (key, task['name']))

      for key, value in task_params[task['name']]["output"].items():
        if value is None and key in task["output_default"]:
          task_params[task['name']]["output"][key] = task["output_default"][key]

    return task_params, required_params
