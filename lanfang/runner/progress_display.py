from .common import TaskStatus
from ..utils import terminal

import collections
import operator
import sys
import datetime

_TaskStatusColor = {
    TaskStatus.DISABLED : ("Disabled", "black",  None),
    TaskStatus.WAITING  : ("Waiting",  "cyan",   None),
    TaskStatus.READY    : ("Ready",    "blue",   None),
    TaskStatus.RUNNING  : ("Running",  "yellow", None),
    TaskStatus.DONE     : ("Done",     "green",  None),
    TaskStatus.FAILED   : ("Failed",   "red",    None),
    TaskStatus.KILLED   : ("Killed",   "purple", None)
}

class TableProgressDisplay(object):
    """Display tasks status in a table.

    Parameters
    ----------
    dependency_manager: multi_task_runner.DependencyManager
        Manage tasks' dependency relations.

    runner_dict: dict
        Task runner dict, key is task's name, value is as {"status": status, "runner": runner}.

    """
    def __init__(self, dependency_manager, runner_dict, update_interval=1200):
        if len(runner_dict) == 0:
            raise ValueError("No task exists, please check")

        self._m_task_info = dependency_manager.get_task_info()
        self._m_task_list = sorted(self._m_task_info, key=lambda n: self._m_task_info[n]["run_id"])
        self._m_runner_dict = runner_dict

        self._m_running_tasks = set()
        for task_name, runner in self._m_runner_dict.items():
            if runner["status"] != TaskStatus.DISABLED:
                self._m_running_tasks.add(task_name)

        self._m_task2str = collections.defaultdict(str)
        max_id_length = len(str(max(map(lambda d: d["run_id"], self._m_task_info.values()))))
        self._m_task_info_length = {
            'id': max_id_length,
            'task_name': min(32, max(map(len, self._m_task_info))),
            'status': 8,
            'start_time': 14, # format: mm.dd HH:MM:SS
            'elapsed_time': 9,
            'try': 3
        }
        row_separator_len = 4 + sum(self._m_task_info_length.values())
        row_separator_len += 3 * (len(self._m_task_info_length) - 2)
        self._m_row_separator = '-' * row_separator_len

        self._m_display_times = 0
        self._m_update_interval = update_interval
        self._m_cursor_pos = None

    def __del__(self):
        if hasattr(self, "_m_display_times") and self._m_display_times > 0:
            sys.stderr.write("\033[%dB" % (2 * len(self._m_task_info) + 1 - self._m_cursor_pos))

        if self._m_cursor_pos is not None:
            sys.stderr.write("\33[?25h")

    def display(self, refresh=False):
        if self._m_cursor_pos is None:
            self._m_cursor_pos = 0
            sys.stderr.write("\33[?25l")

        """Display all tasks status as a table."""
        fout = sys.stderr
        if refresh and self._m_display_times > 0:
            if 2 * len(self._m_task_info) + 1 > self._m_cursor_pos:
                fout.write("\033[%dB\n" % (2 * len(self._m_task_info) + 1 - self._m_cursor_pos))
            self._m_display_times = 0
            self._m_cursor_pos = 0

        if self._m_display_times % self._m_update_interval == 0:
            if self._m_cursor_pos > 0:
                fout.write("\033[%dA" % (self._m_cursor_pos))
            fout.write("\033[K%s\n" % (self._m_row_separator))
            for task_name in self._m_task_list:
                task_show_str = self.__fetch_task_str(task_name)
                self._m_task2str[task_name] = task_show_str
                fout.write("\033[K%s\n\033[K%s\n" % (task_show_str, self._m_row_separator))
            fout.write("\033[%dA" % (2 * len(self._m_task_info) + 1 - self._m_cursor_pos))
        else:
            for task_name in self._m_task_list:
                task_show_str = self.__fetch_task_str(task_name)
                if task_show_str == self._m_task2str[task_name]:
                    continue
                move = self._m_task_info[task_name]["run_id"] * 2 + 1 - self._m_cursor_pos
                if move == 0:
                    fout.write("\033[K%s\033[%dA\n" % (task_show_str, move + 1))
                else:
                    fout.write("\033[%dB\033[K%s\033[%dA\n" % (move, task_show_str, move + 1))
                self._m_task2str[task_name] = task_show_str

            for task_name in self._m_task_list:
                task_status = self._m_runner_dict[task_name]["status"]
                if task_status not in [TaskStatus.DISABLED,
                                       TaskStatus.DONE,
                                       TaskStatus.FAILED,
                                       TaskStatus.KILLED]:
                    new_cursor_pos = self._m_task_info[task_name]["run_id"] * 2 + 1
                    if new_cursor_pos != self._m_cursor_pos:
                        if new_cursor_pos - self._m_cursor_pos > 1:
                            fout.write("\033[%dB" % (new_cursor_pos - self._m_cursor_pos - 1))
                        fout.write("\n")
                    self._m_cursor_pos = new_cursor_pos
                    break
        self._m_display_times += 1

    def __fetch_task_str(self, task_name):
        task_status = self._m_runner_dict[task_name]["status"]
        task_info = self._m_runner_dict[task_name]["runner"].info

        task_show_str_list = list()
        task_show_str_list.append("[%s]." % (str(self._m_task_info[task_name]["run_id"]).zfill(\
                                                 self._m_task_info_length['id'])))
        task_show_str_list.append(task_name.ljust(self._m_task_info_length['task_name']))

        highlight = True
        if task_status == TaskStatus.DONE:
            self._m_running_tasks.discard(task_name)

        status_desc, font_color, bg_color = _TaskStatusColor[task_status]
        status_desc_str = terminal.tint(status_desc.ljust(self._m_task_info_length['status']),
                                        font_color=font_color,
                                        bg_color=bg_color,
                                        highlight=highlight)
        task_show_str_list.append('|')
        task_show_str_list.append(status_desc_str)

        if task_info['start_time'] is not None:
            task_show_str_list.append('|')

            start_time = datetime.datetime.fromtimestamp(task_info['start_time'])
            task_show_str_list.append(start_time.strftime("%m.%d %H:%M:%S"))

            task_show_str_list.append('|')
            elapsed_time = task_info['elapsed_time']
            if elapsed_time is None:
                elapsed_time = 0.0
            task_show_str_list.append(("%.2f" % elapsed_time).ljust(\
                                                self._m_task_info_length['elapsed_time']))
            task_show_str_list.append('|')
            task_show_str_list.append(task_info['try'].ljust(
                                                self._m_task_info_length['try']))

        if task_status == TaskStatus.WAITING:
            task_show_str_list.append('|')
            depend_task_set = self._m_running_tasks & self._m_task_info[task_name]["depends"]

            depend_tasks_str = ",".join(depend_task_set)
            if len(depend_tasks_str) > 48:
                tasks_id_list = map(lambda name: self._m_task_info[name]["run_id"], depend_task_set)
                depend_tasks_str = ",".join(map(str, sorted(tasks_id_list)))
            task_show_str_list.append(depend_tasks_str)
        return " ".join(task_show_str_list)

