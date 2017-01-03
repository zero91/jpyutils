# Author: Donald Cheung <jianzhang9102@gmail.com>
import collections
import copy

class TaskDependencyManager(object):
    def __init__(self):
        self.__dependency_info = collections.defaultdict(set)
        self.__task_id = collections.defaultdict(list) # task_name => (initial ID, running ID)
        self.__is_valid = None

    @classmethod
    def from_data(cls, task_depends_list):
        instance = cls()
        for task_name, depends in task_depends_list:
            instance.add_dependency(task_name, depends)
        return instance

    def add_dependency(self, task_name, depend_tasks=None):
        self.__task_id[task_name] = [len(self.__task_id), None]

        if depend_tasks is None or len(depend_tasks) == 0:
            depend_tasks_set = set()
        elif isinstance(depend_tasks, (list, set, dict, collections.defaultdict)):
            depend_tasks_set = set(depend_tasks)
        elif isinstance(depend_tasks, str):
            depend_tasks_set = set(depend_tasks.split(','))
        else:
            self.__task_id.pop(task_name)
            raise TypeError("depend_tasks's data type does not support")

        self.__is_valid = None
        self.__dependency_info[task_name] |= depend_tasks_set
        return self

    def get_dependency(self, task_name):
        return self.__dependency_info.get(task_name, set())

    def get_tasks_info(self):
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
                    stop = len(task_id) - 1
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
    
