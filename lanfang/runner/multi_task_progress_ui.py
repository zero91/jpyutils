from lanfang.runner.base import RunnerStatus

import sys
import datetime
import time
import abc


class MultiTaskProgressUI(abc.ABC):

  @abc.abstractmethod
  def display(self, reuse=True):
    pass

  @abc.abstractmethod
  def clear(self):
    pass


class MultiTaskTableProgressUI(MultiTaskProgressUI):
  """Display multiple task status as a table.
  """

  color = {
    RunnerStatus.DISABLED : "\033[%s30;1mDisabled\033[0m",
    RunnerStatus.WAITING  : "\033[%s36;1mWaiting\033[0m ",
    RunnerStatus.READY    : "\033[%s34;1mReady\033[0m   ",
    RunnerStatus.RUNNING  : "\033[%s33;1mRunning\033[0m ",
    RunnerStatus.DONE     : "\033[%s32;1mDone\033[0m    ",
    RunnerStatus.FAILED   : "\033[%s31;1mFailed\033[0m  ",
    RunnerStatus.KILLED   : "\033[%s35;1mKilled\033[0m  ",
    RunnerStatus.CANCELED : "\033[%s38;5;162;1mCanceled\033[0m"
  }

  def __init__(self, runner_inventory, runner_dependency,
                                       update_interval=0.1,
                                       output_stream=sys.stderr):
    self._m_runner_inventory = runner_inventory
    self._m_runner_dependency = runner_dependency
    self._m_task_names = self._m_runner_inventory.list()
    self._m_unrelated_tasks = set(self._m_task_names) - set(
        self._m_runner_dependency.get_nodes())

    self._m_update_interval = update_interval
    self._m_output_stream = output_stream
    self._m_dynamic_display = (hasattr(output_stream, 'isatty') and
                               output_stream.isatty())

    self._m_column_length = {
      'id': len(str(len(self._m_task_names) - 1)),
      'task_name': min(32, max(map(len, self._m_task_names + [""]))),
      'start_time': 14, # format: mm.dd HH:MM:SS
      'elapsed_time': 8,
      'attempts': 3
    }
    self._m_row_separator = '-' * (sum(self._m_column_length.values()) + 26)
    self._m_last_update = 0

  def display(self, reuse=True):
    if not self._m_dynamic_display:
      return

    if reuse is True and \
            time.time() - self._m_last_update < self._m_update_interval:
      return

    if self._m_last_update > 0:
      self._m_output_stream.write("\033[0J")

    for order_id, task_name in enumerate(self._m_task_names):
      task_str = self._get_formated_task(order_id, task_name)
      if order_id == 0:
        self._m_output_stream.write("%s\n" % (self._m_row_separator))
      self._m_output_stream.write(task_str + "\n")
      self._m_output_stream.write("%s\n" % (self._m_row_separator))

    if reuse and len(self._m_task_names) > 0:
      self._m_output_stream.write(
          "\033[%dA" % (len(self._m_task_names) * 2 + 1))
    self._m_last_update = time.time()

  def clear(self):
    if self._m_last_update > 0:
      self._m_output_stream.write("\033[0J")

  def _get_formated_task(self, order_id, task_name):
    task_info = self._m_runner_inventory.get_info(task_name)
    task_str = ""

    # column 1. Task ID.
    task_str += "[%s]." % (str(order_id).zfill(self._m_column_length['id']))

    # column 2. Task Name.
    task_name = task_name[: self._m_column_length['task_name']]
    task_str += " " + task_name.ljust(self._m_column_length['task_name'])

    # column 3. Task Status.
    if task_name in self._m_unrelated_tasks:
      codes = "3;4;"
    else:
      codes = ""
    task_str += " | " + self.__class__.color[task_info["status"]] % (codes)

    if task_info["start_time"] is not None:
      # column 4. Task Start Time.
      start_time = datetime.datetime.fromtimestamp(task_info["start_time"])
      task_str += " | " + start_time.strftime("%m.%d %H:%M:%S")

      # column 5. Task Elapsed Time.
      if task_info['elapsed_time'] is None:
        elapsed_time = "0.00"
      else:
        elapsed_time = "%.2f" % (task_info['elapsed_time'])
      task_str += " | " + elapsed_time.ljust(
          self._m_column_length['elapsed_time'])

      # column 6. Task Retry Info.
      attempts_info = "{}/{}".format(*task_info["attempts"])
      task_str += " | " + attempts_info.ljust(self._m_column_length['attempts'])

    # column 7. Task Depends.
    if task_info["status"] in [RunnerStatus.WAITING, RunnerStatus.CANCELED]:
      if task_name not in self._m_unrelated_tasks:
        depends = self._m_runner_dependency.depends(task_name)
      else:
        depends = []
      depend_tasks_str = ",".join(depends)
      if len(depend_tasks_str) > 32:
        tasks_ids = map(self._m_task_names.index, depends)
        depend_tasks_str = ",".join(map(str, sorted(tasks_ids)))
      task_str += " | " + depend_tasks_str
    return task_str
