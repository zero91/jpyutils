"""Manage a batch of tasks' topological relations."""

# Author: Donald Cheung <jianzhang9102@gmail.com>

import collections
import copy

class TaskDependencyManager(object):
    """Manage a batch of tasks' topological relations.
    """
    def __init__(self):
        self.__dependency_info = collections.defaultdict(set)
        self.__task_id = collections.defaultdict(list) # task_name => (initial ID, running ID)
        self.__is_valid = None

    @classmethod
    def from_data(cls, task_depends_list):
        """Class method.  Create a `TaskDependencyManager' object from `task_depends_list'.

        Parameters
        ----------
        task_depends_list: list
            A sequence of tasks, each item is format (task_name, depends).

        Returns
        -------
        instance: TaskDependencyManager
            Instance constructed from input argument.
        """
        instance = cls()
        for task_name, depends in task_depends_list:
            instance.add_dependency(task_name, depends)
        return instance

    def add_dependency(self, task_name, depend_tasks=None):
        """Add one dependency relation.

        Parameters
        ----------
        task_name: string
            Task's name.

        depend_tasks: string/list/set/dict/collections.defaultdict
            Tasks which need to be done before `task_name'.

        Returns
        -------
        instance: TaskDependencyManager
            Current instance's reference.
        """
        self.__task_id[task_name] = [len(self.__task_id), None]

        if depend_tasks is None or len(depend_tasks) == 0:
            depend_tasks_set = set()
        elif isinstance(depend_tasks, (list, set, dict, collections.defaultdict)):
            depend_tasks_set = set(depend_tasks)
        elif isinstance(depend_tasks, str):
            depend_tasks_set = set(map(str.strip, depend_tasks.split(',')))
        else:
            self.__task_id.pop(task_name)
            raise TypeError("depend_tasks's data type does not support")

        self.__is_valid = None
        self.__dependency_info[task_name] |= depend_tasks_set
        return self

    def get_dependency(self, task_name):
        """Get dependency tasks of `task_name'.

        Parameters
        ----------
        task_name: string
            Task's name

        Returns
        -------
        depend_set: set
            Set of jobs which need to be done before `task_name'.
        """
        return self.__dependency_info.get(task_name, set())

    def get_tasks_info(self):
        """Get all tasks' informations.

        Returns
        -------
        is_valid: boolean
            Whether all tasks' relations is topological.

        tasks_info: collections.defaultdict
            All tasks' information. key is the task's name, value contains
                (initial_id, running_id, depend_set, reverse_depend_set)
        """

        self.is_topological()
        tasks = collections.defaultdict(list)
        for task_name, (initial_id, running_id) in self.__task_id.iteritems():
            tasks[task_name] = [initial_id, running_id, set(), set()]

        for task_name, depends_set in self.__dependency_info.iteritems():
            tasks[task_name][2] = copy.deepcopy(self.__dependency_info[task_name])
            for depend_task_name in depends_set:
                tasks[depend_task_name][3].add(task_name)
        return self.__is_valid, tasks

    def is_topological(self):
        """Test whether current relations is topological.

        Returns
        -------
        is_topological: boolean
            True is current relations is topological, otherwise False.
        """

        if self.__is_valid is not None:
            return self.__is_valid

        dependency_info = copy.deepcopy(self.__dependency_info)
        reverse_dependency_info = collections.defaultdict(set)
        for name, depends_set in dependency_info.iteritems():
            for depend in depends_set:
                reverse_dependency_info[depend].add(name)

        cur_task_id = 0
        while len(dependency_info) > 0:
            ready_list = list()
            for name, depend_set in dependency_info.iteritems():
                if len(depend_set) == 0:
                    ready_list.append(name)

            if len(ready_list) == 0:
                self.__is_valid = False
                return False

            for name in sorted(ready_list, key=lambda name: self.__task_id[name][0]):
                self.__task_id[name][1] = cur_task_id
                cur_task_id += 1
                if name in reverse_dependency_info:
                    for depend_name in reverse_dependency_info[name]:
                        dependency_info[depend_name].remove(name)
                    reverse_dependency_info.pop(name)
                dependency_info.pop(name)
        self.__is_valid = True
        return True

    def __parse_single_task_id(self, task_str):
        if task_str.isdigit():
            return int(task_str)
        elif task_str in self.__task_id:
            return self.__task_id[task_str][1]
        elif len(task_str) > 0:
            raise ValueError("task str's format does not support [{0}]".format(task_str))
        return None

    def parse_tasks(self, tasks_list_str):
        """Parse a string into full tasks with its dependency relations.

        Parameters
        ----------
        tasks_list_str: string
            Suppose we have a batch of tasks as [(0, 'a'), (1, 'b'), (2, 'c'), ..., (25, 'z')].

            `tasks_list_str' support following formats:
            (1) "-3,5,7-10-2,13-16,19-".
                "-3" means range from 0(start) to 3, which is "0,1,2,3".
                "7-10-2" means range from 7 to 10, step length 2, which is "7,9".
                "13-16" means range from 13 to 16, step length 1, which is "13,14,15,16."
                "19-" mean range from 19 to 25(end), which is "19,20,21,22,23,24,25".

            (2) "1-4,x,y,z"
                "1-4" mean range from 1 to 4, which is "1,2,3,4"
                "x" mean task 'x', task id is 23.
                "y" mean task 'y', task id is 24.
                "z" mean task 'z', task id is 25.
                So, above string mean jobs "1,2,3,4,23,24,25".

        Returns
        -------
        dependency_manager: TaskDependencyManager
            TaskDependencyManager which contains all the jobs specified by input argument.
        """
        self.is_topological()
        if tasks_list_str is None:
            return copy.deepcopy(self)

        tasks_set = set()
        for seg in tasks_list_str.split(','):
            seg = seg.strip()
            if seg == "":
                continue

            item_list = seg.split('-')
            if len(item_list) == 1:
                tid = self.__parse_single_task_id(item_list[0])
                if tid is not None and 0 <= tid < len(self.__task_id):
                    tasks_set.add(tid)
            elif 2 <= len(item_list) <=  3:
                start = self.__parse_single_task_id(item_list[0])
                stop = self.__parse_single_task_id(item_list[1])
                if len(item_list) == 3:
                    step = self.__parse_single_task_id(item_list[2])
                else:
                    step = 1

                if start is None:
                    start = 0
                if stop is None:
                    stop = len(self.__task_id) - 1
                if step is None:
                    step = 1
                tasks_set |= set(range(start, stop + 1, step))
            else:
                raise ValueError('format of the task str [{0}] does not support'.format(seg))

        valid_tasks_list = filter(lambda t: self.__task_id[t][1] in tasks_set, self.__task_id)
        valid_tasks_list.sort(key=lambda t: self.__task_id[t][0])

        valid_tasks_set = set(valid_tasks_list)
        valid_tasks_depends = list()
        for task in valid_tasks_list:
            valid_tasks_depends.append(valid_tasks_set & self.__dependency_info[task])
        return self.__class__.from_data(zip(valid_tasks_list, valid_tasks_depends))

