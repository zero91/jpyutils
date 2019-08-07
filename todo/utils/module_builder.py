#coding: utf-8
"""Fetch and processing opensources tools for self use.

Currently support following tools:
----------------------------------
glog
gtest
protobuf
gflags
boost
zeromq
cityhash -> No history version, only using latest version

TODO LIST:
1) cpp-btree-1.0.1 (google)
2) hiredis

----------------------------------
"""
from .. import runner
from . import check

import re
import os
import shutil

class ModuleBuilder(object):
    @staticmethod
    def build(repo_path, name, version, clever=True):
        """
        Parameters
        ----------
        repo_path : string
            Target saving path of repository.

        name : string
            repository name.

        version : string
            repository version.

        clever : boolean, optional (default True)
            Automatically calculate repository saving path. If set True, paste `name' to
            `repo_path' if `name' is not the leaf direcotory of `repo_path', otherwise
            using `repo_path'.

        Returns
        -------
        run_code: integer
            Build tasks' exit code.

        Notes
        -----
        minimum version
            protobuf: 3.0.2

            cityhash: latest

        """
        if clever is True and os.path.basename(repo_path) != name:
            repo_path = "{0}/{1}".format(repo_path, name)

        build_path = os.path.realpath("{0}/{1}".format(repo_path, version))
        if os.path.exists(build_path) and len(list(os.walk(build_path))) > 0:
            return 0

        code_path = "{0}/code".format(repo_path)
        if os.path.exists(code_path):
            shutil.rmtree(code_path, ignore_errors=True)

        module_build_commands = {
            "glog": (
                {
                    "command": ["git", "clone", "https://github.com/google/glog", code_path],
                },
                {
                    "command": ["git", "checkout", "v{0}".format(version)],
                    "cwd": code_path
                },
                {
                    "command": ["./configure", "--prefix={0}".format(build_path)],
                    "cwd": code_path
                },
                {
                    "command": ["autoreconf", "-i"],
                    "cwd": code_path
                },
                {
                    "command": ["make", "-j4"],
                    "cwd": code_path
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                }
            ),
            "gtest": (
                {
                    "command": ["git", "clone", "https://github.com/google/googletest",
                                                        code_path],
                },
                {
                    "command": ["git", "checkout", "release-{0}".format(version)],
                    "cwd": code_path,
                },
                {
                    "command": ["cmake", "-DCMAKE_INSTALL_PREFIX={0}".format(build_path), "."],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "-j4"],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                }
            ),
            "protobuf": (
                {
                    "command": ["git", "clone", "https://github.com/google/protobuf", code_path],
                },
                {
                    "command": ["git", "checkout", "v{0}".format(version)],
                    "cwd": code_path,
                },
                {
                    "command": ["./autogen.sh"],
                    "cwd": code_path,
                },
                {
                    "command": ["./configure", "--prefix={0}".format(build_path)],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "-j4"],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "check", "-j4"],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                },
            ),
            "gflags": (
                {
                    "command": ["git", "clone", "https://github.com/gflags/gflags", code_path],
                },
                {
                    "command": ["git", "checkout", "v{0}".format(version)],
                    "cwd": code_path
                },
                {
                    "command": ["cmake", "-DCMAKE_INSTALL_PREFIX={0}".format(build_path), "."],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "-j4"],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                }
            ),
            "boost": (
                {
                    "command": ["wget", "-c", "-P", code_path, "https://nchc.dl.sourceforge.net"\
                                        "/project/boost/boost/{0}/boost_{1}.tar.gz".format(
                                                    version, version.replace(".", "_")),
                                                    "--no-check-certificate" ],
                },
                {
                    "command": ["tar", "-C", code_path, "-xzf", "{0}/boost_{1}.tar.gz".format(
                                                code_path, version.replace(".", "_"))],
                },
                {
                    "command": ["./bootstrap.sh", "--prefix={0}".format(build_path)],
                    "cwd": "{0}/boost_{1}".format(code_path, version.replace(".", "_")),
                },
                {
                    "command": ["./b2", "install"],
                    "cwd": "{0}/boost_{1}".format(code_path, version.replace(".", "_")),
                }
            ),
            "zeromq": (
                {
                    "command": ["git", "clone", "https://github.com/zeromq/libzmq", code_path],
                },
                {
                    "command": ["git", "checkout", "v{0}".format(version)],
                    "cwd": code_path,
                },
                {
                    "command": ["./autogen.sh"],
                    "cwd": code_path,
                },
                {
                    "command": ["./configure", "--prefix={0}".format(build_path)],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "-j4"],
                    "cwd": code_path,
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                },
            ),
            "cityhash" : (
                {
                    "command": ["git", "clone", "https://github.com/google/cityhash", code_path],
                },
                {
                    "command": ["./configure", "--prefix={0}".format(build_path)],
                    "cwd": code_path,
                },
                {
                    "command": "make all check CXXFLAGS=\"-g -O3\"",
                    "cwd": code_path,
                    "shell": True,
                },
                {
                    "command": ["make", "install"],
                    "cwd": code_path
                },
            ),
        }

        if name not in module_build_commands:
            raise KeyError("repo [{0}] does not been supported yet".format(name))

        builder = runner.MultiTaskRunner(parallel_degree=1)
        for command_info in module_build_commands[name]:
            builder.add(name=" ".join(command_info['command']), **command_info)

        pub_real_path = os.path.realpath("{0}/publish".format(repo_path))
        if not os.path.exists(pub_real_path) \
                    or check.check_version(version, os.path.basename(pub_real_path)):
            command = ["ln", "-snf", version, "publish"]
            builder.add(command=command, cwd=repo_path, name=" ".join(command))
        return builder.run()

