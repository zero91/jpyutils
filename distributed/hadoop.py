"""Tools for using hadoop more conveniently.
"""
# Author: Donald Cheung
from jpyutils import runner

import os
import sys
import signal
import subprocess
import collections
import md5
import threading
import time
import re
import copy
import shutil
import xml.etree.ElementTree as ET

class Hadoop(object):
    """Tools for using hadoop more conveniently.
    """
    def __init__(self, hadoop_env_list=None):
        """Constructor of class Hadoop.

        Parameters
        ----------
        hadoop_env_list: list/tuple/set/dict, optional
            List of hadoop environment. Element should be length 2 if in type list/tuple/set.
            For example, 
                [
                    ("cluster1", "/local/environment/path/of/cluster1"), 
                    ("cluster2", "/local/environment/path/of/cluster2"),
                    ....
                    ("clusterk", "/local/environment/path/of/clusterk")
                ]
        """
        self.__hadoop_env = collections.defaultdict(dict)
        self.__env_name_list = list()

        default_env = self.__find_default_env()
        if default_env is not None:
            self.add_hadoop_env("default", default_env)

        if isinstance(hadoop_env_list, (list, tuple, set)):
            length_list = list(set(map(len, hadoop_env_list)))
            if len(length_list) > 1 or (len(length_list) == 1 and length_list[0] != 2):
                raise ValueError("format of parameter `hadoop_env_list' does not supported")
            for name, path in hadoop_env_list:
                self.__env_name_list.append(name)
                self.add_hadoop_env(name, path)

        elif isinstance(hadoop_env_list, dict):
            for name, path in hadoop_env_list.iteritems():
                self.__env_name_list.append(name)
                self.add_hadoop_env(name, path)

        self.__default_streaming_param = {
            'job_name':           'streamingjob',
            'map_task_num':       743,
            'reduce_task_num':    743,
            'map_capacity':       743,
            'reduce_capacity':    743,
            'memory_limit':       1200,
            'map_output_key_sep': '\t',
            'map_sorted_key_num': 1,
            'partition_key_num':  1,
            'priority':           'NORMAL',
            'ignore_separator':   'false',
            'partitioner':        'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
            'mapper':             'cat',
            'reducer':            'NONE',
            'multiple_output':    False,
            'output_format':      'org.apache.hadoop.mapred.TextOutputFormat',
        }
        self.__necessary_info4clean = list()
        self.__access_lock = threading.Lock()

    def __find_default_env(self):
        process = subprocess.Popen(['which', 'hadoop'], stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE)
        stdout_value, stderr_value = process.communicate()
        if process.returncode != 0:
            return None
        return os.path.dirname(os.path.dirname(stdout_value))

    def set_default_env(self, name):
        """Set an existing hadoop environment as default.

        Parameters
        ----------
        name: string
            Alias name of hadoop environment.

        """
        if name not in self.__hadoop_env:
            raise KeyError("Hadoop environment [{0}] not exists!".format(name))
        self.__hadoop_env["default"] = copy.deepcopy(self.__hadoop_env[name])

    def add_hadoop_env(self, name, path, update=False):
        """Add one hadoop environment.

        Parameters
        ----------
        name: string
            Alias name for this new hadoop environment.

        path: string
            Root path of local hadoop environment, subdirectory must contains `conf' and `bin'
            directory.

        update: boolean, optional 
            If `name' is already exists, setting `update' to be True will replace old environment.

        """
        if not update and name in self.__hadoop_env:
            raise UserWarning("{0} is already exists, old value {1}, new value {2}".format(
                                    name, self.__hadoop_env[name], path))
        path = os.path.realpath(path)
        self.__hadoop_env[name]["path"] = path
        for hadoop_property in ET.parse("{0}/conf/hadoop-site.xml".format(path)).getroot():
            item_name = hadoop_property.find('name').text
            item_value = hadoop_property.find('value').text
            self.__hadoop_env[name][item_name] = item_value
        self.__env_name_list.append(name)

    def remove_hadoop_env(self, name):
        """Remove a hadoop environment from this instance.

        Parameters
        ----------
        name: string
            Alias name for this hadoop environment.

        Returns
        -------
        succeed: integer
            Return 0 if succeed, otherwise return 1.

        """
        if name not in self.__hadoop_env:
            return 1
        self.__env_name_list.remove(name)
        self.__hadoop_env.pop(name)
        return 0

    def get_hadoop_env(self, name=None):
        """Get hadoop environment info.

        Parameters
        ----------
        name: string, optional
            Alias name for this hadoop environment.
            If set None, return all internal hadoop environment.

        Returns
        -------
        hadoop_env_info: dict/list of dict
            If `name' is not None, return specified hadoop environment for `name'.
            Otherwise return all list of hadoop environment.

        """
        if name is not None:
            return self.__hadoop_env.get(name, None)

        hadoop_env_list = list() 
        for name in self.__env_name_list:
            hadoop_env_list.append((name, self.__hadoop_env[name]))
        return hadoop_env_list

    def __using_hadoop_env(self, hadoop_env):
        if len(self.__hadoop_env) == 0:
            raise UserWarning(
                        'at least one hadoop environment should be set implicitly or explicitly')
        if hadoop_env is not None:
            if hadoop_env not in self.__hadoop_env:
                raise KeyError("Can't find hadoop environment [{0}]".format(hadoop_env))
            else:
                return hadoop_env
        return self.__env_name_list[0]

    def streaming(self, input_path, output_path, **params):
        """Generate a hadoop streaming command.

        Parameters
        ----------
        input_path: string/list/tuple/set/dict
            Input path of the streaming job. If type string, multiple path should be separated
            by a comma.

        output_path: string
            Output path of the streaming job.

        mapper: string, optional
            Map command of the task, default value is "cat".

        reducer: string, optional
            Reduce command of the task. If there aren't any, set None. Default value is None.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        job_name: string, optional
            Streaming job's name.

        ignore_separator: boolean, optional
            If task's output value is empty, set true will discard tab and empty value.

        multiple_output: boolean, optional
            Set True if streaming job need to output multiple files.

        archives: string/list/tuple/set/dict, optional
            Add archives file for streaming job. If type string, multiple archives should be
            separated by a comma.

        upload_files: string/list/tuple/set/dict, optional
            Add local files for streaming job.

        map_task_num: integer, optional
            Streaming job's map task number, default value is 743.

        reduce_task_num: integer, optional
            Streaming job's reduce task number, default value is 743.

        map_capacity: integer, optional
            Streaming job's map capacity, default value is 743.

        reduce_capacity: integer, optional
            Streaming job's reduce capacity, default value is 743.

        memory_limit: integer, optional
            Streaming job's memory limit in MB, default value is 1200. 

        map_output_key_sep: string, optional
            Streaming job's map output key's separator, default value is '\t'.

        partition_key_num:  integer, optional
            Streaming job's map output partition key number, default value is 1.

        map_sorted_key_num: integer, optional
            Streaming job's map output sorting key number, default value is 1.

        priority: string, optional
            Streaming job's priority, default value is 'NORMAL'. Should be one of
                    "VERY_LOW", "LOW", "NORMAL", "HIGH", "VERY_HIGH"

        partitioner: string, optional
            Streaming job's partitioner method.
            Default value is org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

        output_format: string, optional
            Streaming job's output format. Notice that its value can be set indirectly by
            `multiple_output'. Default value is org.apache.hadoop.mapred.lib.TextOutputFormat.

        Returns
        -------
        command: string
            streaming command in string.

        """
        streaming_conf = self.__default_streaming_param.copy()

        hadoop_env = self.__using_hadoop_env(params.get('hadoop_env', None))
        streaming_conf['hadoop_env'] = self.__hadoop_env[hadoop_env]['path']
        params.pop('hadoop_env', None)

        if "ignore_separator" in params and params['ignore_separator'] is True:
            streaming_conf['ignore_separator'] = 'true'
            params.pop('ignore_separator')

        if "multiple_output" in params and params['multiple_output'] is True:
            streaming_conf['output_format'] = \
                    'org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat'
            params.pop('multiple_output')

        archives = list()
        archives.append("/share/python2.7.tar.gz#python")
        if 'archives' in params and params['archives'] is not None:
            archives_list = list()
            if isinstance(params['archives'], basestring):
                archives_list = map(str.strip, params['archives'].split(','))
            elif isinstance(params['archives'], (list, tuple, set, dict)):
                archives_list = map(str.strip, params['archives'])
            else:
                raise ValueError("unsupported type for archives {0}".format(params['archives']))
            archives.extend(filter(lambda a: len(a) > 0, archives_list))
            params.pop('archives')
        streaming_conf['archives'] = ' -cacheArchive '.join(archives)

        file_param = ""
        if "upload_files" in params and params['upload_files'] is not None:
            upload_files = params['upload_files']
            upload_files_list = list()
            if isinstance(upload_files, basestring):
                upload_files_list = map(str.strip, upload_files.split(','))
            elif isinstance(upload_files, (list, tuple, set, dict)):
                upload_files_list = map(str.strip, upload_files)
            else:
                raise ValueError('unsupported type for upload_files [{0}]'.format(upload_files))
            upload_files_list = filter(lambda f: len(f) > 0, upload_files_list)
            if len(upload_files_list) > 0:
                file_param = " ".join(map(lambda f: "-file {0}".format(f), upload_files_list))
            params.pop('upload_files')
        streaming_conf['file_param'] = file_param

        input_path_list = list()
        if isinstance(input_path, basestring):
            input_path_list = map(str.strip, input_path.split(','))
        elif isinstance(input_path, (list, tuple, set, dict)):
            input_path_list = map(str.strip, input_path)
        else:
            raise ValueError('unsupported type for input_path [{0}]'.format(input_path))

        input_path_list = sorted(set(filter(lambda i: len(i) > 0, input_path_list)))
        if len(input_path_list) == 0:
            raise ValueError("at least one valid input path should be specified")

        streaming_conf['input_path'] = " -input ".join(input_path_list)
        streaming_conf['output_path'] = output_path
        for param, value in params.iteritems():
            if param in streaming_conf and value is not None:
                streaming_conf[param] = value

        streaming_conf['mapper'] = streaming_conf['mapper'].replace('\"', '\\\"')
        streaming_conf['reducer'] = streaming_conf['reducer'].replace('\"', '\\\"')

        streaming_cmd = '{hadoop_env}/bin/hadoop streaming' \
                ' -D mapred.job.name="{job_name}"' \
                ' -D mapred.map.tasks={map_task_num}' \
                ' -D mapred.reduce.tasks={reduce_task_num}' \
                ' -D mapred.job.map.capacity={map_capacity}' \
                ' -D mapred.job.reduce.capacity={reduce_capacity}' \
                ' -D stream.memory.limit={memory_limit}' \
                ' -D map.output.key.field.separator="{map_output_key_sep}"' \
                ' -D num.key.fields.for.partition={partition_key_num}' \
                ' -D stream.num.map.output.key.fields={map_sorted_key_num}' \
                ' -D mapred.job.priority={priority}' \
                ' -D mapred.textoutputformat.ignoreseparator={ignore_separator}' \
                ' -partitioner {partitioner}' \
                ' -cacheArchive {archives}' \
                ' -mapper "{mapper}"' \
                ' -reducer "{reducer}"' \
                ' -input {input_path}' \
                ' -output {output_path}' \
                ' -outputformat {output_format}' \
                ' {file_param}'
        return streaming_cmd.format(**streaming_conf)

    def __find_hadoop_env(self, hadoop_bin_path):
        hadoop_env_path = os.path.dirname(os.path.dirname(os.path.realpath(hadoop_bin_path)))
        for env_name in self.__env_name_list:
            if self.__hadoop_env[env_name]['path'] == hadoop_env_path:
                return env_name
        new_env_name = md5.md5(hadoop_env_path).hexdigest()
        self.add_hadoop_env(new_env_name, hadoop_env_path)
        return new_env_name

    def run_streaming(self, cmd, clear_output=False, retry=1, verbose=True):
        """Running a streaming command.

        Parameters
        ----------
        cmd: string
            The streaming command to run.

        clear_output: boolean, optional
            Whether or not clear output path before running this job.

        retry: integer, optional
            Try at most `retry' times.

        verbose: boolean, optional
            Output streaming job's log if set True.
            
        Returns
        -------
        returncode: integer
            Exit code of this hadoop streaming job.

        """
        pre_sigint_handler = signal.signal(signal.SIGINT, self.__kill_handler)
        pre_sigterm_handler = signal.signal(signal.SIGTERM, self.__kill_handler)

        hadoop_env = self.__find_hadoop_env(cmd.split(' streaming ', 1)[0].strip())
        if clear_output:
            output_path = cmd.split(" -output ")[-1].strip().split(' ', 1)[0]
            self.remove(output_path, hadoop_env)

        process = runner.TaskRunner(cmd, shell=True, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE,
                                                     retry=retry)
        self.__access_lock.acquire()
        idx = len(self.__necessary_info4clean)
        self.__necessary_info4clean.append([process, hadoop_env, None])
        process.start()
        self.__access_lock.release()

        while process.stderr is None:
            time.sleep(0.1)

        while True:
            line = process.stderr.readline()
            if len(line) == 0 and not process.is_alive():
                break

            if "INFO mapred.JobClient: Running job" in line:
                jobid = line.strip().split(" ")[-1]
                self.__necessary_info4clean[idx][2] = jobid
            verbose and sys.stderr.write(line)

        while process.is_alive():
            time.sleep(0.1)

        signal.signal(signal.SIGINT, pre_sigint_handler)
        signal.signal(signal.SIGTERM, pre_sigterm_handler)
        return process.returncode

    def remove(self, hadoop_path, hadoop_env=None):
        """Remove a path.

        Parameters
        ----------
        hadoop_path: string
            The path to be removed.

        hadoop_env: string, optional
            Alias name of hadoop environment.

        Returns
        -------
        result: integer 
            0 if the path been removed successfully, otherwise non zero.

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        return subprocess.Popen(["{0}/bin/hadoop".format(self.__hadoop_env[hadoop_env]['path']),
                                            "fs", "-rmr", hadoop_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).wait()

    def mkdir(self, hadoop_path, hadoop_env=None):
        """Create a path.

        Parameters
        ----------
        hadoop_path: string
            The path to be created.

        hadoop_env: string, optional
            Alias name of hadoop environment.

        Returns
        -------
        result: integer 
            0 if the path been created successfully, otherwise non zero.

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        return subprocess.Popen(["{0}/bin/hadoop".format(self.__hadoop_env[hadoop_env]['path']),
                                            "fs", "-mkdir", hadoop_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).wait()

    def fetch_content(self, hadoop_path, default=None, hadoop_env=None):
        """Fetch a content from a path.

        Parameters
        ----------
        hadoop_path : string
            File's hadoop path.

        default: string, optional
            If `hadoop_path' does not exist, raise an exception if `default' is None,
            otherwise return `default'.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        Returns
        -------
        generator: generator
            A generator, which generate content result.
        
        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        proc = subprocess.Popen(["{0}/bin/hadoop".format(self.__hadoop_env[hadoop_env]['path']),
                                            "fs", "-cat", hadoop_path
                                ],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        for line in iter(proc.stdout.readline, b''):
            yield line

        if proc.returncode != 0:
            if default is not None:
                yield default
            else:
                raise UserWarning("Fetch content of [{0}] failed, ERROR INFO [{1}] ".format(
                                            hadoop_path, proc.stderr.read()))

    def list_path(self, path, pattern=None, hadoop_env=None):
        """List hadoop paths.

        Parameters
        ----------
        path: string/list/tuple/set/dict
            The parent directory.

        pattern : string, optional
            The pattern which the subdirectory or files should be satisfied.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        Returns
        -------
        sub_path_list: list
            All subdirectories or files of the path and its timestamp, format as:
                    [(path1, timestamp1), (path2, timestamp2), ...]

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        if isinstance(path, basestring):
            path_list = path.split(',')
            if len(path_list) > 1:
                return self.list_path(path_list, pattern, hadoop_env)
            hadoop_path = self.__hadoop_env[hadoop_env]['path']
            process = subprocess.Popen(["{0}/bin/hadoop".format(hadoop_path), "fs", "-ls", path],
                                                    stdout=subprocess.PIPE,
                                                    stderr=subprocess.PIPE)
            stdout_val, stderr_val = process.communicate()
            if process.returncode != 0:
                raise ValueError("can't access {0}, ERROR {1}".format(path, stderr_val))

            sub_path_list = list()
            if pattern is None:
                pattern = r'.*'
            pattern_inst = re.compile('(%s)(/){0,1}(%s)' % (path.replace('*', '.*'), pattern))
            for line in stdout_val.strip().split('\n'):
                fields = line.split()
                if len(fields) != 8:
                    continue

                day, hour, fname = fields[-3:]
                if re.match(pattern_inst, fname):
                    ftime = time.strptime("{0} {1}".format(day, hour), "%Y-%m-%d %H:%M")
                    sub_path_list.append((fname, int(time.mktime(ftime))))
            return sub_path_list

        elif isinstance(path, (list, tuple, set, dict)):
            path_res = list()
            for p in path:
                path_res.append(self.list_path(p, pattern, hadoop_env))
            return path_res

        else:
            raise ValueError("format of {0} does not supported".format(path))

    def list_path_size(self, path, hadoop_env=None):
        """List size of paths.

        Parameters
        ----------
        path: string/list/tuple/set/dict
            The path to be listed.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        Returns
        -------
        generator: generator
            A generator, which generate all path size list.
            Format as follows:
                [(sub1_path1, size1), (sub1_path2, size2), ...]

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        path_list = list()
        if isinstance(path, basestring):
            path_list = map(str.strip, path.split(','))
        elif isinstance(path, (list, tuple, set, dict)):
            path_list = map(str.strip, path)
        else:
            raise ValueError("format of {0} does not supported".format(path))

        hadoop_env_path = self.__hadoop_env[hadoop_env]['path']
        cmd = ["{0}/bin/hadoop".format(hadoop_env_path), "fs", "-dus"]
        cmd.extend(path_list)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for line in iter(proc.stdout.readline, b''):
            fields = line.strip().split('\t')
            if len(fields) != 2:
                continue
            (path_name, size) = fields
            fname = path_name.split('//', 1)[1]
            fname = fname[fname.index('/'):]
            yield (fname, int(size))

    def distcp(self, src_hadoop_env, src_path, dest_hadoop_env, dest_path, clear_output=False,
                                                                           hadoop_env=None,
                                                                           verbose=True,
                                                                           priority='NORMAL',
                                                                           map_capacity=500,
                                                                           map_speed=10485760):
        """Copy files between two hadoop clusters.

        Parameters
        ----------
        src_hadoop_env: string
            Hadoop environment's alias name of source path.

        src_path: string
            Source path.

        dest_hadoop_env: string
            Hadoop environment's alias name of dest path.

        dest_path: string
            Dest path.

        clear_output: boolean, optional
            Whether or not to remove output path.

        hadoop_env: string, optional
            Hadoop environment's alias name form executing distcp job.
            Default to be equal to dest_hadoop_env.

        verbose: boolean, optional
            Output all distcp command log if set True.

        priority: string, optional
            distcp job's priority.

        map_capacity: integer, optional
            distcp job's map capacity.

        map_speed: integer, optional
            distcp job's copy speed.

        Returns
        -------
        result: integer
            distcp job's exit code.

        """
        pre_sigint_handler = signal.signal(signal.SIGINT, self.__kill_handler)
        pre_sigterm_handler = signal.signal(signal.SIGTERM, self.__kill_handler)

        if hadoop_env is None:
            hadoop_env = dest_hadoop_env

        if clear_output:
            self.remove(dest_path, dest_hadoop_env)

        distcp_conf = dict()
        distcp_conf['hadoop_env'] = self.__hadoop_env[hadoop_env]['path']
        distcp_conf['map_capacity'] = map_capacity
        distcp_conf['map_speed'] = map_speed
        distcp_conf['priority'] = priority
        distcp_conf['src_ugi'] = self.__hadoop_env[src_hadoop_env]['hadoop.job.ugi']
        distcp_conf['dest_ugi'] = self.__hadoop_env[dest_hadoop_env]['hadoop.job.ugi']
        distcp_conf['src_fs_default_name'] = self.__hadoop_env[src_hadoop_env]['fs.default.name']
        distcp_conf['dest_fs_default_name'] = self.__hadoop_env[dest_hadoop_env]['fs.default.name']
        distcp_conf['src_path'] = src_path
        distcp_conf['dest_path'] = dest_path

        distcp_cmd = '{hadoop_env}/bin/hadoop distcp' \
                            ' -D mapred.job.map.capacity={map_capacity}' \
                            ' -D distcp.map.speed.kb={map_speed}' \
                            ' -D mapred.job.priority={priority}' \
                            ' -su {src_ugi}' \
                            ' -du {dest_ugi}' \
                            ' {src_fs_default_name}{src_path}' \
                            ' {dest_fs_default_name}{dest_path}'
        process = runner.TaskRunner(distcp_cmd.format(**distcp_conf), stdout=subprocess.PIPE,
                                                                      stderr=subprocess.PIPE,
                                                                      shell=True)
        self.__access_lock.acquire()
        idx = len(self.__necessary_info4clean)
        self.__necessary_info4clean.append([process, hadoop_env, None])
        process.start()
        self.__access_lock.release()

        while process.stderr is None:
            time.sleep(0.1)

        while True:
            line = process.stderr.readline()
            if len(line) == 0 and not process.is_alive():
                break

            if "INFO mapred.JobClient: Running job" in line:
                jobid = line.strip().split(" ")[-1]
                self.__necessary_info4clean[idx][2] = jobid
            verbose and sys.stderr.write(line)

        while process.is_alive():
            time.sleep(0.1)

        signal.signal(signal.SIGINT, pre_sigint_handler)
        signal.signal(signal.SIGTERM, pre_sigterm_handler)
        return process.returncode

    def __kill_handler(self, signum, stack):
        sys.stderr.write("\nReceive signal {0}, all jobs begin to terminate.\n".format(signum))
        for (process, hadoop_env, jobid) in self.__necessary_info4clean:
            if process is not None and process.is_alive():
                process.terminate()

            if jobid is None:
                continue

            tracker =self.__hadoop_env[hadoop_env]["mapred.job.tracker"]
            hadoop_path = self.__hadoop_env[hadoop_env]['path']
            returncode = subprocess.Popen(["{0}/bin/hadoop".format(hadoop_path), "job",
                                                "-D", "mapred.job.tracker={0}".format(tracker),
                                                "-kill", jobid
                                            ],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE).wait()
            if returncode == 0:
                sys.stderr.write("Terminate hadoop job {0} succeed\n".format(jobid))
            else:
                sys.stderr.write("Terminate hadoop job {0} failed\n".format(jobid))
        exit(1)

    def __join_keys(self, keys):
        if isinstance(keys, int):
            key_list = [keys]
        elif isinstance(keys, basestring):
            key_list = map(int, keys.split(','))
        elif isinstance(keys, (list, tuple, set, dict)):
            key_list = list(keys)
        else:
            raise ValueError("unsupported format of [{0}].".format(keys))

        if len(key_list) == 0:
            raise ValueError("at least one key should be specified.")
        return key_list

    def __join_values(self, values):
        value_list = list()
        if values is None:
            return value_list

        if isinstance(values, int):
            value_list = [(values, None)]
        elif isinstance(values, basestring):
            for value in values.split(','):
                if value.isdigit():
                    value_list.append((int(value), None))
                else:
                    idx, default = value.split(':')
                    value_list.append((int(idx), default))
        elif isinstance(values, (list, tuple, set, dict)):
            for value in list(values):
                if isinstance(value, int):
                    value_list.append((value, None))
                elif isinstance(value, (list, tuple)) and len(value) == 2:
                    value_list.append((value[0], value[1]))
                else:
                    raise ValueError("unsupported format of [{0}]".format(values))
        else:
            raise ValueError("unsupported format of [{0}].".format(values))
        return value_list

    def __join_input_pattern(self, path):
        path_list = list()
        if isinstance(path, basestring):
            path_list = path.split(',')
        elif isinstance(path, (list, tuple, set, dict)):
            path_list = list(path)
        return "\|".join(["\(.*\)\({0}\)".format(p.replace('*', '.*')) for p in path_list])

    def streaming_join(self, left_input,
                             left_key,
                             left_value,
                             right_input,
                             right_key,
                             right_value,
                             output_path,
                             method="left",
                             **params):
        """Generating a hadoop streaming command for joining two path of data.
        Most of this method's parameters are the same as `streaming' method, except followings.

        Parameters
        ----------
        left_input: string/list/tuple/set/dict
            Left input data path.

        left_key: integer/string/list/tuple/set/dict
            Keys of left input data.

        right_input: string/list/tuple/set/dict
            Right input data path.

        right_key: integer/string/list/tuple/set/dict
            Keys of right input data.

        left_value: integer/string/list/tuple/set/dict
            Values of left input data.

        right_value: integer/string/list/tuple/set/dict
            Values of right input data.

        output_path: string
            Output path of streaming job.

        method: string
            Joining method, should be one of `left', `right', `inner'.

        Returns
        -------
        comand: string
            Streaming job command.

        """
        left_key_list = self.__join_keys(left_key)
        left_value_list = self.__join_values(left_value)
        right_key_list = self.__join_keys(right_key)
        right_value_list = self.__join_values(right_value)

        map_cmd_param = dict()
        map_cmd_param["method"] = method
        map_cmd_param["left_pattern"] = self.__join_input_pattern(left_input)
        map_cmd_param["left_key"] = ",".join(map(str, left_key_list))
        map_cmd_param["left_value"] = ",".join(map(lambda (i, d): "{0}:{1}".format(i, d),
                                                            left_value_list))
        map_cmd_param["right_pattern"] = self.__join_input_pattern(right_input)
        map_cmd_param["right_key"] = ",".join(map(str, right_key_list))
        map_cmd_param["right_value"] = ",".join(map(lambda (i, d): "{0}:{1}".format(i, d),
                                                            right_value_list))

        map_cmd = 'python/bin/python _join_mapred.py -e map -m {method}' \
                        ' --left_pattern \'{left_pattern}\'' \
                        ' --left_key {left_key}' \
                        ' --left_value \'{left_value}\'' \
                        ' --right_pattern \'{right_pattern}\'' \
                        ' --right_key {right_key}' \
                        ' --right_value \'{right_value}\''

        reduce_cmd_param = dict()
        reduce_cmd_param['method'] = method
        reduce_cmd_param['left_value'] = map_cmd_param['left_value']
        reduce_cmd_param['right_value'] = map_cmd_param['right_value']

        reduce_cmd = 'python/bin/python _join_mapred.py -e reduce -m {method}' \
                        ' --left_value \'{left_value}\'' \
                        ' --right_value \'{right_value}\''

        upload_files = "{0}/_join_mapred.py".format(os.path.realpath(os.path.dirname(__file__)))
        input_path_list = list()
        if isinstance(left_input, basestring):
            input_path_list.extend(left_input.split(','))
        elif isinstance(left_input, (list, tuple, set, dict)):
            input_path_list.extend(left_input)

        if isinstance(right_input, basestring):
            input_path_list.extend(right_input.split(','))
        elif isinstance(right_input, (list, tuple, set, dict)):
            input_path_list.extend(right_input)

        return self.streaming(input_path=input_path_list,
                              output_path=output_path,
                              mapper=map_cmd.format(**map_cmd_param),
                              reducer=reduce_cmd.format(**reduce_cmd_param),
                              upload_files=upload_files,
                              map_sorted_key_num=2,
                              **params)

    def download(self, hadoop_path, local_path, clear_output=False, hadoop_env=None):
        """Download hadoop data to local path.

        Parameters
        ----------
        hadoop_path: string
            Hadoop data path.

        local_path: string
            Local saving path.

        clear_output: boolean, optional
            Whether or not to remove local output path.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        Returns
        -------
        returncode: integer
            Exit code of this hadoop downloading job.

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        if clear_output is True:
            shutil.rmtree(local_path, ignore_errors=True)

        local_parent_path = os.path.dirname(os.path.realpath(local_path))
        if not os.path.exists(local_parent_path):
            os.makedirs(local_parent_path)

        return subprocess.Popen(["{0}/bin/hadoop".format(self.__hadoop_env[hadoop_env]['path']),
                                            "fs", "-get", hadoop_path, local_path
                                ],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).wait()

    def upload(self, local_path, hadoop_path, clear_output=False, hadoop_env=None, verbose=True):
        """Upload a local data to hadoop.

        Parameters
        ----------
        local_path: string
            Local data path.

        hadoop_path: string
            Hadoop upload path.

        clear_output: boolean, optional
            Whether or not to remove hadoop output path.

        hadoop_env: string, optional
            Hadoop environment's alias name.

        verbose: boolean, optional
            Output all distcp command log if set True.

        Returns
        -------
        returncode: integer
            Exit code of this hadoop upload job.

        """
        hadoop_env = self.__using_hadoop_env(hadoop_env)
        if clear_output is True:
            self.remove(hadoop_path, hadoop_env=hadoop_env)

        if verbose is True:
            proc_stderr = None
            proc_stdout = None
        else:
            proc_stdout = subprocess.PIPE
            proc_stderr = subprocess.PIPE

        return subprocess.Popen(["{0}/bin/hadoop".format(self.__hadoop_env[hadoop_env]['path']),
                                            "fs", "-put", local_path, hadoop_path
                                ],
                                stdout=proc_stdout,
                                stderr=proc_stderr).wait()
