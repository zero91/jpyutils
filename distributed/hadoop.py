"""Tools for hadoop.
"""

# Author: Donald Cheung <jianzhang9102@gmail.com>

from .. import utils
from .. import runner

import threading
import subprocess
import time
import sys
import re
import signal
import os

class Hadoop:
    """Provide a set of useful methods for doing hadoop tasks.

    Parameters
    ----------
    hadoop_path: string
        The hadoop's environment directory, should contains bin path.

    """
    def __init__(self, hadoop_path = None):
        self.__hadoop_path = hadoop_path
        self.__running_list = list()
        self.__access_lock = threading.Lock()
        self.__streaming_killing_cmd = None
        self.__streaming_job_id = None

    def set_hadoop_path(self, hadoop_path):
        """Set hadoop's environment directory.

        Parameters
        ----------
        hadoop_path: string
            Set hadoop's environment directory, must contains bin path.

        """
        if os.path.isdir(hadoop_path) and os.path.isfile("%s/bin/hadoop" % hadoop_path):
            self.__hadoop_path = hadoop_path
        else:
            raise ValueError("Hadoop path [%s] is illegal" % (hadoop_path))

    def fetch_env(self):
        """Get the path of hadoop environment

        Returns
        -------
        env : string
            The hadoop's environment path. If it does't exist, return None.
        """
        stdout_value, stderr_value = subprocess.Popen('which hadoop', \
                                                       shell=True, \
                                                       stdout=subprocess.PIPE, \
                                                       stderr=subprocess.PIPE).communicate()
        if len(stdout_value) > 0:
            return os.path.dirname(os.path.dirname(stdout_value))
        return None

    def create(self, input_path, output_path, mapper, reducer, jobname, files=None,
                    map_task_num=743, map_capacity=743,
                    reduce_task_num=743, reduce_capacity=743, memory_limit=1200,
                    partitioner="org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
                    map_output_separator='\t', map_output_key_fields=1,
                    partition_key_fields=1, multiple_output=False,
                    ignore_separator=False, priority='NORMAL', method="streaming"):
        """Generating a hadoop command.

        Parameters
        ----------
        input_path: string
            Input path of the the task. Using ',' to separate multiple paths.

        output_path: string
            Output path of the task.

        mapper: string
            Mapper command of the task.

        reducer: string
            Reducer command of the task. If there aren't any, set None.

        jobname: string
            The task's jobname.

        files: string
            The task's local files needed to be uploaded. Using ',' to separate multiple ones.

        method: string
            The task's executing method. Can be ``streaming`` or ``hce``.
        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        cmd = '%s/bin/hadoop %s ' % (self.__hadoop_path, method)
        cmd += ' -D mapred.job.name="%s" ' % jobname
        cmd += ' -D mapred.map.tasks=%d ' % map_task_num
        cmd += ' -D mapred.reduce.tasks=%d ' % reduce_task_num
        cmd += ' -D mapred.job.map.capacity=%d ' % map_capacity
        cmd += ' -D mapred.job.reduce.capacity=%d ' % reduce_capacity
        cmd += ' -D stream.memory.limit=%d ' % memory_limit
        cmd += ' -D map.output.key.field.separator="%s" ' % map_output_separator
        cmd += ' -D num.key.fields.for.partition=%s ' % partition_key_fields
        cmd += ' -D stream.num.map.output.key.fields=%d ' % map_output_key_fields
        cmd += ' -D mapred.job.priority=%s ' % priority

        if ignore_separator:
            cmd += ' -D mapred.textoutputformat.ignoreseparator=true '

        cmd += ' -partitioner %s ' % partitioner
        if multiple_output:
            cmd += ' -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat '

        if files is not None and len(files.strip()) != 0:
            cmd += ' '.join([' -file %s ' % fname for fname in files.split(',')])

        cmd += ' -mapper "%s" ' % mapper.replace("\"", "\\\"")
        if reducer is None:
            cmd += ' -reducer "NONE" '
        else:
            cmd += ' -reducer "%s" ' % reducer.replace("\"", "\\\"")
        cmd += ' '.join([' -input %s ' % fin for fin in input_path.split(',')])
        cmd += ' -output %s ' % output_path
        cmd += ' -cacheArchive /share/python2.7.tar.gz#python '
        return cmd

    def join(self, left_input_path, left_fields_num, left_key_list,
                    right_input_path, right_fields_num, right_key_list, output_path,
                    left_value_list = None, right_value_list = None, method = "left",
                    jobname = "join_data",
                    map_task_num = 743, map_capacity = 743,
                    reduce_task_num = 743, reduce_capacity = 743, memory_limit = 1200):
        """Generating a hadoop streaming command for joining two type of files.

        Parameters
        ----------
        left_input_path: string
            The join task's left input path. Using ',' to separate multiple paths.

        left_fields_num: integer
            Left input files' fields number.

        left_key_list: string
            Left input's key list using to join right input.

        right_input_path: string
            The join task's right input path. If contains multiple path,
            using ',' to separate then.

        right_fields_num: integer
            Right input files' fields number.

        right_key_list: string
            Right input's key list using to join left input.

        output_path: string
            The streaming task's output path.

        left_value_list: string
            The output fields of left input. If None, output all of the fields.
            If contains multiple fields, using ',' to separate them.

        right_value_list: string
            The output fields of right input. If None, output all of the fields.
            If contains multiple fields, using ',' to separate them.

        method: string
            The join method, support "left", "right", "inner" currently.

        jobname: string
            The streaming task's jobname.
        """
        left_key_list = self.__join_fields_list(left_key_list, left_fields_num)
        left_value_list = self.__join_fields_list(left_value_list, left_fields_num)
        right_key_list = self.__join_fields_list(right_key_list, right_fields_num)
        right_value_list = self.__join_fields_list(right_value_list, right_fields_num)

        left_input_pattern = "\|".join(["\(.*\)\(%s\)" % p.replace('*', '.*') \
                                                    for p in left_input_path.split(',')])
        right_input_pattern = "\|".join(["\(.*\)\(%s\)" % p.replace('*', '.*') \
                                                    for p in right_input_path.split(',')])

        map_cmd = "python/bin/python _join_mapred.py "
        map_cmd += " -e mapper -m %s " % (method)
        map_cmd += " --left_input_pattern \'%s\' " % left_input_pattern
        map_cmd += " --left_key_list %s " % (",".join(map(lambda k: str(k), left_key_list)))
        map_cmd += " --left_value_list %s " % (",".join(map(lambda k: str(k), left_value_list)))
        map_cmd += " --left_fields_num %d " % left_fields_num

        map_cmd += " --right_input_pattern \'%s\' " % right_input_pattern
        map_cmd += " --right_key_list %s " % (",".join(map(lambda k: str(k), right_key_list)))
        map_cmd += " --right_value_list %s " % (",".join(map(lambda k: str(k), right_value_list)))
        map_cmd += " --right_fields_num %d " % right_fields_num

        reduce_cmd = "python/bin/python _join_mapred.py "
        reduce_cmd += " -e reducer -m %s " % (method)
        reduce_cmd += " --left_value_num %d " % len(left_value_list)
        reduce_cmd += " --right_value_num %d " % len(right_value_list)

        join_mapred_file = "%s/_join_mapred.py" % os.path.abspath(os.path.dirname(__file__))
        return self.create(input_path = "%s,%s" % (left_input_path, right_input_path),
                           output_path = output_path,
                           mapper = map_cmd,
                           reducer = reduce_cmd,
                           jobname = jobname,
                           files = join_mapred_file,
                           map_task_num = map_task_num,
                           reduce_task_num = reduce_task_num,
                           map_capacity = map_capacity,
                           reduce_capacity = reduce_capacity,
                           partition_key_fields = 1,
                           map_output_key_fields = 2,
                           memory_limit = memory_limit)

    def run(self, cmd, clear_output=False, verbose=True):
        """Running a command.

        Parameters
        ----------
        cmd: string
            The command to run.
        """
        pre_sigint_handler = signal.signal(signal.SIGINT, self.__kill_handler)
        pre_sigterm_handler = signal.signal(signal.SIGTERM, self.__kill_handler)

        if clear_output:
            output_path = streaming_cmd.split(" -output ")[-1].strip().split(' ')[0]
            self.remove_path(output_path)

        task = runner.task_runner.TaskRunner(cmd)

        self.__access_lock.acquire()
        idx = len(self.__running_list)
        self.__running_list.append([task, None, None])
        self.__access_lock.release()

        task.start()
        while task.stderr is None:
            time.sleep(0.1)

        while True:
            line = task.stderr.readline()
            if len(line) == 0:
                break

            if "INFO mapred.JobClient: Running job" in line:
                self.__running_list[idx][2] = line.strip().split(" ")[-1]

            if "-kill" in line:
                self.__running_list[idx][1] = line.strip().split('mapred.JobClient:')[-1].strip()

            if verbose:
                sys.stderr.write(line)

            if "Streaming Job Failed!" in line:
                signal.signal(signal.SIGINT, pre_sigint_handler)
                signal.signal(signal.SIGTERM, pre_sigterm_handler)
                return -1

        while task.is_alive():
            time.sleep(0.1)

        signal.signal(signal.SIGINT, pre_sigint_handler)
        signal.signal(signal.SIGTERM, pre_sigterm_handler)
        if task.returncode != 0:
            return -1
        return 0

    def remove_path(self, path):
        """Remove a path.

        Parameters
        ----------
        path: string
            The path to be removed.

        Returns
        -------
        result : boolean
            Return True if the path been removed successful, otherwise return False.
        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        remove_cmd = "%s/bin/hadoop fs -rmr %s" % (self.__hadoop_path, path)
        stdout_value, stderr_value = subprocess.Popen(remove_cmd,
                                                      shell=True, \
                                                      stdout=subprocess.PIPE, \
                                                      stderr=subprocess.PIPE).communicate()
        if len(stderr_value) > 0:
            return False
        return True

    def list_path(self, path, pattern = r'.*', line_sep = '\n'):
        """List a path's subdirectory or files.

        Parameters
        ----------
        path: string
            The parent directory.

        pattern : string
            The pattern which the subdirectory or files should be satisfied.

        line_sep : string
            The separator to separate the string returned from hadoop fs list command.
        
        Returns
        -------
        file_list : list
            All subdirectory or files of the path.

        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        file_list = list()
        if type(path) in (list, set, dict):
            for p in path:
                file_list.extend(self.list_path(p, pattern))
            return file_list

        if type(path) != str:
            return list()
        list_cmd = "%s/bin/hadoop fs -ls %s" % (self.__hadoop_path, path)
        stdout_value, stderr_value = subprocess.Popen(list_cmd, shell=True, \
                                                      stdout=subprocess.PIPE, \
                                                      stderr=subprocess.PIPE).communicate()
        pattern_inst = re.compile('(%s)(/){0,1}(%s)' % (path.replace('*', '.*'), pattern))
        file_list = list()
        for line in stdout_value.split(line_sep):
            line = line.strip()
            last_space = line.rfind(' /')
            if last_space == -1 or last_space >= len(line):
                continue

            fname = line[last_space + 1:]
            if re.match(pattern_inst, fname):
                file_list.append(fname)

        if len(stderr_value) > 0:
            sys.stderr.write("[%s] %s\n" % (utils.shell.tint("ERROR", "red"), stderr_value))
        return file_list

    def list_path_size(self, path, line_sep = '\n'):
        """List a path's size.

        Parameters
        ----------
        path: string
            The path to be calculated.

        line_sep : string
            The separator to separate the string returned from hadoop fs list command.

        Returns
        -------
        path_size_list : list
            All path size which satisfied the specified path, format [(path, size), ... ]

        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        file_list = list()
        if type(path) in (list, set, dict):
            for p in path:
                file_list.extend(self.list_path_size(p, pattern))
            return file_list

        if type(path) != str:
            return list()
        list_cmd = "%s/bin/hadoop fs -dus %s" % (self.__hadoop_path, path)
        stdout_value, stderr_value = subprocess.Popen(list_cmd, shell=True, \
                                                      stdout=subprocess.PIPE, \
                                                      stderr=subprocess.PIPE).communicate()
        path_size_list = list()
        for line in stdout_value.split(line_sep):
            fields = line.strip().split('\t')
            if len(fields) != 2:
                continue
            (path_name, size) = fields

            fname = path_name.split('//', 1)[1]
            fname = fname[fname.index('/') : ]
            path_size_list.append((fname, int(size)))

        if len(stderr_value) > 0:
            sys.stderr.write("[%s] %s\n" % (utils.shell.tint("ERROR", "red"), stderr_value))
        return path_size_list

    def distcp(self, src, dest, src_userpwd = None,
                                dest_userpwd = None,
                                map_capacity = 500,
                                map_speed = 10485760):

        """Copy files from one hadoop cluster to another cluster.

        Parameters
        ----------
        src : string
            The source path of the source hadoop cluster.
        dest : string
            The dest path of the dest hadoop cluster.

        src_userpwd : string
            Source hadoop cluster's user and password.

        dest_userpwd : string
            Dest hadoop cluster's user and password.

        Returns
        -------
        result : boolean
            True is succeed, otherwise False.
        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        distcp_cmd = "%s/bin/hadoop distcp" % self.__hadoop_path
        distcp_cmd += " -D mapred.job.map.capacity=%d " % map_capacity
        distcp_cmd += " -D distcp.map.speed.kb=%d " % map_speed

        if src_userpwd is not None: distcp_cmd += " -su %s " % src_userpwd
        if dest_userpwd is not None: distcp_cmd += " -du %s " % dest_userpwd
        distcp_cmd += " %s %s " % (src, dest)

        distcp_process = subprocess.Popen(distcp_cmd, shell=True, \
                                          stdout=subprocess.PIPE, \
                                          stderr=subprocess.PIPE)
        stdout_value, stderr_value = distcp_process.communicate()
        if distcp_process.returncode != 0:
            sys.stderr.write("[%s] %s\n" % (utils.shell.tint("ERROR", "red"), stderr_value))
            return False
        return True

    def fetch_content(self, path, output_file=subprocess.PIPE, error_file=subprocess.PIPE):
        """Fetch a content from a path.

        Parameters
        ----------
        path : string
            The path to get content.

        Returns
        -------
        stdout_value : string
            stdout value of the result.
        
        stderr_value : string
            stderr value of the result.
        """
        if self.__hadoop_path is None:
            self.__hadoop_path = self.fetch_env()
        if self.__hadoop_path is None:
            raise ValueError("Hasn't set the hadoop's environment path yet")

        cat_cmd = "%s/bin/hadoop fs -cat %s" % (self.__hadoop_path, path)
        stdout_value, stderr_value = subprocess.Popen(cat_cmd, shell = True, \
                                                      stdout = output_file, \
                                                      stderr = error_file).communicate()
        return stdout_value, stderr_value

    def __kill_handler(self, signum, stack):
        sys.stderr.write("\nReceive signal %d, all tasks begin to terminate.\n" % signum)

        for (task, kill_cmd, jobid) in self.__running_list:
            if task is not None and task.is_alive():
                task.suicide()

            if kill_cmd is None:
                continue

            stdout_value, stderr_value = subprocess.Popen(kill_cmd, shell=True,
                                                          stdout=subprocess.PIPE,
                                                          stderr=subprocess.PIPE).communicate()
            if len(stderr_value) > 0:
                sys.stderr.write("Terminate hadoop job [%s] succeed\n" % jobid)
            else:
                sys.stderr.write("Terminate hadoop job [%s] failed\n" % jobid)
        exit(-1)

    def __join_fields_list(self, key_list, fields_num):
        if key_list == None:
            return range(0, fields_num)

        fields_list = key_list[:]
        if type(key_list) == str:
            fields_list = [int(k) for k in key_list.split(',')]
        return filter(lambda col: 0 <= col < fields_num, fields_list)

