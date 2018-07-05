"""Tools for display tasks' status friendly."""
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .common import TaskStatus
from ..utils import terminal

import collections
import operator
import datetime
import sys
import abc

_TaskStatusColor = {
    TaskStatus.DISABLED : ("Disabled", "white"),
    TaskStatus.WAITING  : ("Waiting",  "yellow"),
    TaskStatus.READY    : ("Ready",    "blue"),
    TaskStatus.RUNNING  : ("Running",  "cyan"),
    TaskStatus.DONE     : ("Done",     "green"),
    TaskStatus.FAILED   : ("Failed",   "red"),
    TaskStatus.KILLED   : ("Killed",   "purple")
}

class TableProgressDisplay(object):
    """Display tasks' status in a table.

    """
    def __init__(self, dependency_manager, task_runner_dict, update_interval=500):
        """Create a new instance.

        Parameters
        ----------
        dependency_manager: multi_task_runner.TaskDependencyManager
            Manage tasks' dependency relations.

        task_runner_dict: dict
            Task runner dict, key is task's name, value is as (status, runner).

        """
        self._m_tasks_info = dependency_manager.get_tasks_info()[1]
        self._m_tasks_list = sorted(self._m_tasks_info,
                                    key=lambda name: self._m_tasks_info[name][1])
        self._m_task_runner_dict = task_runner_dict

        self._m_running_task_set = set()
        for task_name, (task_status, _) in self._m_task_runner_dict.items():
            if task_status != TaskStatus.DISABLED:
                self._m_running_task_set.add(task_name)

        self._m_tasks_show = collections.defaultdict(str)
        max_id_length = len(str(max(map(operator.itemgetter(1), self._m_tasks_info.values()))))
        self._m_task_info_length = {
            'id': max_id_length,
            'task_name': max(map(len, self._m_tasks_info)),
            'status': 8,
            'start_time': 19, # format: YYYY-mm-dd HH:MM:SS
            'elapse_time': 9,
            'try_info': 5
        }
        self._m_display_times = 0
        self._m_update_interval = update_interval

    def display(self):
        """Display all tasks' status as a table.

        """
        self._m_display_times += 1
        fout = sys.stderr
        if self._m_display_times % self._m_update_interval == 1:
            if self._m_display_times > 1:
                fout.write("\033[%dA" % (2 * len(self._m_tasks_info) + 1))
            row_separator_len = sum(self._m_task_info_length.values()) + 3
            row_separator_len += 3 * len(self._m_task_info_length)
            fout.write("%s\n" % ('-' * row_separator_len))
            for task_name in self._m_tasks_list:
                task_show_str = self.__fetch_task_str(task_name)
                self._m_tasks_show[task_name] = task_show_str
                fout.write("%s\n" % (task_show_str))
                fout.write("%s\n" % ('-' * row_separator_len))
            return

        for task_name in self._m_tasks_list:
            task_show_str = self.__fetch_task_str(task_name)
            if task_show_str != self._m_tasks_show[task_name]:
                move = (len(self._m_tasks_info) - self._m_tasks_info[task_name][1]) * 2
                fout.write("\033[%dA\33[K%s\033[%dB\n" % (move, task_show_str, move - 1))

    def __fetch_task_str(self, task_name):
        task_status = self._m_task_runner_dict[task_name][0]
        task_info = self._m_task_runner_dict[task_name][1].info

        task_show_str_list = list()
        task_show_str_list.append("[%s]." % (str(self._m_tasks_info[task_name][1]).zfill(\
                                                 self._m_task_info_length['id'])))
        task_show_str_list.append(task_name.ljust(self._m_task_info_length['task_name']))

        if task_status == TaskStatus.DISABLED:
            highlight = False
        else:
            highlight = True

        if task_status == TaskStatus.DONE:
            self._m_running_task_set.discard(task_name)

        status_desc, status_color = _TaskStatusColor[task_status]
        status_desc_str = terminal.tint(status_desc.ljust(self._m_task_info_length['status']),
                                        font_color=status_color, highlight=highlight)
        task_show_str_list.append('|')
        task_show_str_list.append(status_desc_str)

        if task_info['start_time'] is not None:
            task_show_str_list.append('|')
            task_show_str_list.append(task_info['start_time'].strftime("%Y.%m.%d %H:%M:%S"))

            task_show_str_list.append('|')
            task_show_str_list.append(("%.2f" % (task_info['elapse_time'])).ljust(\
                                                self._m_task_info_length['elapse_time']))
            task_show_str_list.append('|')
            task_show_str_list.append(task_info['try_info'].ljust(
                                                self._m_task_info_length['try_info']))

        if task_status == TaskStatus.WAITING:
            task_show_str_list.append('|')
            depend_task_set = self._m_running_task_set & self._m_tasks_info[task_name][2]

            depend_tasks_str = ",".join(depend_task_set)
            if len(depend_tasks_str) > 48:
                tasks_id_list = map(lambda name: self._m_tasks_info[name][1], depend_task_set)
                depend_tasks_str = ",".join(map(str, sorted(tasks_id_list)))
            task_show_str_list.append(depend_tasks_str)
        return " ".join(task_show_str_list)

