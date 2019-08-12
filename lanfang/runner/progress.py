from lanfang.runner.common import TaskStatus
from lanfang.utils import terminal

import collections
import sys
import datetime


_TASK_STATUS_COLOR_MAP = {
  TaskStatus.DISABLED : ("Disabled", "black",  None),
  TaskStatus.WAITING  : ("Waiting",  "cyan",   None),
  TaskStatus.READY    : ("Ready",    "blue",   None),
  TaskStatus.RUNNING  : ("Running",  "yellow", None),
  TaskStatus.DONE     : ("Done",     "green",  None),
  TaskStatus.FAILED   : ("Failed",   "red",    None),
  TaskStatus.KILLED   : ("Killed",   "purple", None),
  TaskStatus.CANCELED : ("Canceled", "red",    None)
}


class TableDisplay(object):
  """Display tasks status in a table.

  Parameters
  ----------
  dependency: dependency.TopologicalGraph

  runner_dict: dict
    Task runner dict, such as
      {
        "task_name": {
          "status": status,
          "runner": runner
        }
      }
  """

  def __init__(self, dependency, runner_dict, update_interval=1200):
    if len(runner_dict) == 0:
      raise ValueError("No task exists, please check")

    self._m_task_list = dependency.get_nodes()
    self._m_task2id = {name: tid for tid, name in enumerate(self._m_task_list)}
    self._m_runner_dict = runner_dict

    self._m_running_tasks = set()
    for task_name, runner in self._m_runner_dict.items():
      if runner["status"] != TaskStatus.DISABLED:
        self._m_running_tasks.add(task_name)

    self._m_task2str = collections.defaultdict(str)
    self._m_dependency = dependency
    self._m_column_length = self.__initialize_length_info(self._m_task_list)
    self._m_row_separator = self.__initialize_row_sep(self._m_column_length)

    self._m_write_times = 0
    self._m_update_interval = update_interval
    self._m_cursor_pos = None
    self._m_table_rows = 2 * len(self._m_task_list) + 1

  def __initialize_length_info(self, tasks):
    max_id_length = len(str(len(tasks) - 1))
    task_name_max_length = min(32, max(map(len, tasks)))
    column_length = {
      'id': max_id_length,
      'task_name': task_name_max_length,
      'status': 8,
      'start_time': 14, # format: mm.dd HH:MM:SS
      'elapsed_time': 9,
      'try': 3
    }
    return column_length

  def __initialize_row_sep(self, length_info):
    row_separator_len = 4 + sum(length_info.values())
    row_separator_len += 3 * (len(self._m_column_length) - 2)
    return '-' * row_separator_len

  def __del__(self):
    if hasattr(self, "_m_write_times") and self._m_write_times > 0:
      sys.stderr.write("\033[%dB" % (self._m_table_rows - self._m_cursor_pos))

    if self._m_cursor_pos is not None:
      sys.stderr.write("\33[?25h")

  def write(self, refresh=False, out_stream=sys.stderr):
    if self._m_cursor_pos is None:
      self._m_cursor_pos = 0
      sys.stderr.write("\33[?25l")

    fout = out_stream
    """Display all tasks status as a table."""
    if refresh and self._m_write_times > 0:
      if self._m_cursor_pos < self._m_table_rows:
        fout.write("\033[%dB\n" % (self._m_table_rows - self._m_cursor_pos))
      self._m_write_times = 0
      self._m_cursor_pos = 0

    if self._m_write_times % self._m_update_interval == 0:
      self.__print_full(fout)
    else:
      for task_name in self._m_task_list:
        task_show_str = self.__fetch_task_str(task_name)
        if task_show_str == self._m_task2str[task_name]:
          continue
        move = self._m_task2id[task_name] * 2 + 1
        move -= self._m_cursor_pos
        if move == 0:
          fout.write("\033[K%s\033[%dA\n" % (task_show_str, move + 1))
        else:
          fout.write("\033[%dB\033[K%s\033[%dA\n" % (
            move, task_show_str, move + 1))

        self._m_task2str[task_name] = task_show_str
    self._m_write_times += 1

  def __print_full(self, out_stream):
    if self._m_cursor_pos > 0:
      out_stream.write("\033[%dA" % (self._m_cursor_pos))
    out_stream.write("\033[K%s\n" % (self._m_row_separator))
    for task_name in self._m_task_list:
      task_show_str = self.__fetch_task_str(task_name)
      self._m_task2str[task_name] = task_show_str
      out_stream.write("\033[K%s\n\033[K%s\n" % (
        task_show_str, self._m_row_separator))
    out_stream.write("\033[%dA" % (self._m_table_rows - self._m_cursor_pos))

  def __fetch_task_str(self, task_name):
    status = self._m_runner_dict[task_name]["status"]
    task_info = self._m_runner_dict[task_name]["runner"].info

    task_show_str_list = list()

    # column 1. Task ID.
    task_id = str(self._m_task2id[task_name])
    task_id = "[%s]." % (task_id.zfill(self._m_column_length['id']))
    task_show_str_list.append(task_id)

    # column 2. Task Name.
    task_show_str_list.append(
      task_name.ljust(self._m_column_length['task_name']))

    # column 3. Task Status.
    task_show_str_list.append('|')
    highlight = True
    if status == TaskStatus.DONE:
      self._m_running_tasks.discard(task_name)

    status_desc, font_color, bg_color = _TASK_STATUS_COLOR_MAP[status]
    status_desc_str = terminal.tint(
      status_desc.ljust(self._m_column_length['status']),
      font_color=font_color, bg_color=bg_color, highlight=highlight)
    task_show_str_list.append(status_desc_str)

    if task_info['start_time'] is not None:
      # column 4. Task Start Time.
      task_show_str_list.append('|')
      start_time = datetime.datetime.fromtimestamp(task_info['start_time'])
      task_show_str_list.append(start_time.strftime("%m.%d %H:%M:%S"))

      # column 5. Task Elapsed Time.
      task_show_str_list.append('|')
      elapsed_time = task_info['elapsed_time']
      if elapsed_time is None:
        elapsed_time = 0.0
      task_show_str_list.append(
        ("%.2f" % elapsed_time).ljust(self._m_column_length['elapsed_time']))

      # column 6. Task Retry Info.
      task_show_str_list.append('|')
      task_show_str_list.append(
        task_info['try'].ljust(self._m_column_length['try']))

    if status in [TaskStatus.WAITING, TaskStatus.CANCELED]:
      # [optional] column 4. Task Depends Info if needed.
      task_show_str_list.append('|')
      depends = self._m_running_tasks & self._m_dependency.depends(task_name)
      depend_tasks_str = ",".join(depends)
      if len(depend_tasks_str) > 48:
        tasks_ids = map(self._m_task2id.get, depends)
        depend_tasks_str = ",".join(map(str, sorted(tasks_ids)))
      task_show_str_list.append(depend_tasks_str)
    return " ".join(task_show_str_list)
