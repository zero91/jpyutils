"""Tools for display tasks' status friendly."""

# Author: Donald Cheung <jianzhang9102@gmail.com>

from jpyutils.utils import shell

import collections
import operator
import datetime
import sys

from .base import TaskStatus

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

    Parameters
    ----------
    dependency_manager: TaskDependencyManager
        Tasks' dependency relations manager.

    task_runner_dict: dict
        Task runner dict, key is task's name, value is as (status, runner).
    """
    def __init__(self, dependency_manager, task_runner_dict):
        self.__tasks_info = dependency_manager.get_tasks_info()[1]
        self.__tasks_list = sorted(self.__tasks_info, key=lambda name: self.__tasks_info[name][1])
        self.__task_runner_dict = task_runner_dict

        self.__running_task_set = set()
        for task_name, (task_status, _) in self.__task_runner_dict.iteritems():
            if task_status != TaskStatus.DISABLED:
                self.__running_task_set.add(task_name)

        self.__tasks_show = collections.defaultdict()

        max_id_length = len(str(max(map(operator.itemgetter(1), self.__tasks_info.values()))))
        self.__task_info_length = {
                                    'id': max_id_length,
                                    'task_name': max(map(len, self.__tasks_info)),
                                    'status': 8,
                                    'start_time': 19, # format: YYYY-mm-dd HH:MM:SS
                                    'elapse_time': 9,
                                    'try_info': 5
                                    }
        self.__display_times = 0

    def display(self):
        """Display all tasks' status as a table.
        """
        self.__display_times += 1
        fout = sys.stderr
        if self.__display_times % 100 == 1:
            if self.__display_times > 1:
                fout.write("\033[%dA" % (2 * len(self.__tasks_info) + 1))
            row_separator_len = sum(self.__task_info_length.values()) + 3
            row_separator_len += 3 * len(self.__task_info_length)
            fout.write("%s\n" % ('-' * row_separator_len))
            for task_name in self.__tasks_list:
                task_show_str = self.__fetch_task_str(task_name)
                self.__tasks_show[task_name] = task_show_str
                fout.write("%s\n" % (task_show_str))
                fout.write("%s\n" % ('-' * row_separator_len))
            return

        for task_name in self.__tasks_list:
            task_show_str = self.__fetch_task_str(task_name)
            if task_show_str != self.__tasks_show[task_name]:
                move = (len(self.__tasks_info) - self.__tasks_info[task_name][1]) * 2
                fout.write("\033[%dA\33[K%s\033[%dB\n" % (move, task_show_str, move - 1))

    def __fetch_task_str(self, task_name):
        task_status = self.__task_runner_dict[task_name][0]
        task_info = self.__task_runner_dict[task_name][1].get_info()

        task_show_str_list = list()
        task_show_str_list.append("[%s]." % (str(self.__tasks_info[task_name][1]).zfill(\
                                                self.__task_info_length['id'])))
        task_show_str_list.append(task_name.ljust(self.__task_info_length['task_name']))

        if task_status == TaskStatus.DISABLED:
            highlight = False
        else:
            highlight = True

        if task_status == TaskStatus.DONE:
            self.__running_task_set.discard(task_name)

        status_desc, status_color = _TaskStatusColor[task_status]
        status_desc_str = shell.tint(status_desc.ljust(self.__task_info_length['status']),
                                            font_color=status_color, highlight=highlight)
        task_show_str_list.append('|')
        task_show_str_list.append(status_desc_str)

        if task_info['start_time'] is not None:
            task_show_str_list.append('|')
            task_show_str_list.append(task_info['start_time'].strftime("%Y.%m.%d %H:%M:%S"))

            task_show_str_list.append('|')
            task_show_str_list.append(("%.2f" % (task_info['elapse_time'])).ljust(\
                                                    self.__task_info_length['elapse_time']))
            task_show_str_list.append('|')
            task_show_str_list.append(task_info['try_info'].ljust(
                                                    self.__task_info_length['try_info']))

        if task_status == TaskStatus.WAITING:
            task_show_str_list.append('|')
            depend_task_set = self.__running_task_set & self.__tasks_info[task_name][2]

            depend_tasks_str = ",".join(depend_task_set)
            if len(depend_tasks_str) > 48:
                tasks_id_list = map(lambda name: str(self.__tasks_info[name][1]), depend_task_set)
                depend_tasks_str = ",".join(sorted(tasks_id_list))
            task_show_str_list.append(depend_tasks_str)
        return " ".join(task_show_str_list)

