"""
The :mod:`lanfang.runner` module includes utilities for running tasks.
"""

from lanfang.runner.base import RunnerContext
from lanfang.runner.base import RunnerHook
from lanfang.runner.base import RunnerStatus
from lanfang.runner.base import Runner
from lanfang.runner.cmd_runner import ArgumentParser
from lanfang.runner.cmd_runner import CmdRunner
from lanfang.runner.func_runner import FuncThreadRunner
from lanfang.runner.func_runner import FuncProcessRunner
from lanfang.runner.multi_task_dependency import TopologicalGraph
from lanfang.runner.multi_task_dependency import DynamicTopologicalGraph
from lanfang.runner.multi_task_config import MultiTaskConfig
from lanfang.runner.multi_task_config import MultiTaskJsonnetConfig
from lanfang.runner.multi_task_runner import MultiTaskRunner
from lanfang.runner.task_register import TaskRegister
from lanfang.runner.task_register import TaskSchema
from lanfang.runner.task_loader import TaskLoader

from lanfang import utils as _utils


for multi_config_class in _utils.func.subclasses(
        MultiTaskConfig, recursive=True):
  for ext in multi_config_class.exts():
    MultiTaskConfig.register(ext, multi_config_class)


del _utils

__all__ = [_s for _s in dir() if not _s.startswith('_')]
