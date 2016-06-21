"""Fetch and processing opensources tools for self use.

Currently support following tools:
----------------------------------
boost
glog
gtest
gflags
protobuf
----------------------------------
"""

import sys
import os

from jpyutils.runner import task_runner

class OpenSources(object):

    def __init__(self, root, name, version, log=None):
        self.__root = os.path.abspath(root)
        self.__name = name
        self.__version = version
        self.__log = log
        self.__target_root = "%s/%s/%s" % (self.__root, self.__name, self.__version)

    def build(self, update=False):
        if not update and os.path.isdir("%s" % (self.__target_root)):
            return 0
        exec("self._%s__build_%s()" % (self.__class__.__name__, self.__name), {}, {"self" : self})

    def __build_boost(self):
        remote_fname = "boost_%s" % (self.__version.replace('.', '_'))
        url = "http://jaist.dl.sourceforge.net/project/boost/boost/%s/%s.tar.gz" % \
                        (self.__version, remote_fname)

        runner = task_runner.MultiTaskRunner(log = self.__log)
        runner.add(command = "rm -rf %s" % (self.__target_root), name = "remove_path")

        runner.add(command = "mkdir -p %s" % (self.__target_root),
                   depends = "remove_path",
                   name = "create_path")

        runner.add(command = "cd %s && curl -# -O %s" % (self.__target_root, url),
                   depends = "create_path",
                   name = "download")

        runner.add(command = "tar -xzf %s/%s.tar.gz -C %s" % \
                            (self.__target_root, remote_fname, self.__target_root),
                   depends = "download",
                   name = "uncompress")

        runner.add(command = "cd %s/%s && ./bootstrap.sh --prefix=%s" % \
                                (self.__target_root, remote_fname, self.__target_root),
                   depends = "uncompress",
                   name = "bootstrap")

        runner.add(command = "cd %s/%s && ./b2 install" % (self.__target_root, remote_fname),
                   depends = "bootstrap",
                   name = "install")

        runner.add(command = "rm -rf %s/%s && rm -f %s/%s.tar.gz" % (self.__target_root, \
                                remote_fname, self.__target_root, remote_fname),
                   depends = "install",
                   name = "clear")

        return runner.run()

    def __build_glog(self):
        runner = task_runner.MultiTaskRunner(log = self.__log)
        runner.add(command = "rm -rf %s" % (self.__target_root), name = "remove_path")

        runner.add(command = "git clone https://github.com/google/glog %s/glog" % \
                                        (self.__target_root),
                   depends = "remove_path",
                   name = "download")

        runner.add(command = "cd %s/glog && git checkout v%s" % \
                                        (self.__target_root, self.__version),
                   depends = "download",
                   name = "checkout")

        runner.add(command = "cd %s/glog && ./configure --prefix=%s" % \
                                        (self.__target_root, self.__target_root),
                   depends = "checkout",
                   name = "configure")

        runner.add(command = "cd %s/glog && make" % (self.__target_root),
                   depends = "configure",
                   name = "make")

        runner.add(command = "cd %s/glog && make install" % (self.__target_root),
                   depends = "make",
                   name = "install")

        runner.add(command = "rm -rf %s/glog" % (self.__target_root),
                   depends = "install",
                   name = "clear")

        return runner.run()

    def __build_gtest(self):
        runner = task_runner.MultiTaskRunner(log = self.__log)
        runner.add(command = "rm -rf %s" % (self.__target_root), name = "remove_path")

        runner.add(command = "mkdir -p %s/lib" % (self.__target_root),
                   depends = "remove_path",
                   name = "create_path")

        runner.add(command = "git clone https://github.com/google/googletest %s/googletest" % \
                                        (self.__target_root),
                   depends = "create_path",
                   name = "download")

        runner.add(command = "cd %s/googletest && git checkout release-%s" % \
                                        (self.__target_root, self.__version),
                   depends = "download",
                   name = "checkout")

        runner.add(command = "cd %s/googletest && cmake ." % (self.__target_root),
                   depends = "checkout",
                   name = "cmake")

        runner.add(command = "cd %s/googletest && make" % (self.__target_root),
                   depends = "cmake",
                   name = "make")

        runner.add(command = "cd %s/googletest && cp -r include %s && cp *.a %s/lib" % \
                                (self.__target_root, self.__target_root, self.__target_root),
                   depends = "make",
                   name = "install")

        runner.add(command = "rm -rf %s/googletest" % (self.__target_root),
                   depends = "install",
                   name = "clear")

        return runner.run()

    def __build_gflags(self):
        runner = task_runner.MultiTaskRunner(log = self.__log)
        runner.add(command = "rm -rf %s" % (self.__target_root), name = "remove_path")

        runner.add(command = "git clone https://github.com/gflags/gflags %s/gflags" % \
                                        (self.__target_root),
                   depends = "remove_path",
                   name = "download")

        runner.add(command = "cd %s/gflags && git checkout v%s" % \
                                        (self.__target_root, self.__version),
                   depends = "download",
                   name = "checkout")

        runner.add(command = "cd %s/gflags && cmake ." % (self.__target_root),
                   depends = "checkout",
                   name = "cmake")

        runner.add(command = "cd %s/gflags && make" % (self.__target_root),
                   depends = "cmake",
                   name = "make")

        runner.add(command = "cd %s && mv gflags/include . && mv gflags/lib ." % \
                                (self.__target_root),
                   depends = "make",
                   name = "install")

        runner.add(command = "rm -rf %s/gflags" % (self.__target_root),
                   depends = "install",
                   name = "clear")

        return runner.run()

    def __build_protobuf(self):
        runner = task_runner.MultiTaskRunner(log = self.__log)
        runner.add(command = "rm -rf %s" % (self.__target_root), name = "remove_path")

        runner.add(command = "git clone https://github.com/google/protobuf %s/protobuf" % \
                                        (self.__target_root),
                   depends = "remove_path",
                   name = "download")

        runner.add(command = "git clone https://github.com/google/googletest %s/protobuf/gtest"\
                                         % (self.__target_root),
                   depends = "download",
                   name = "fetch_gtest")

        runner.add(command = "cd %s/protobuf && git checkout v%s " \
                                    " && cd gtest && git checkout release-1.7.0" % \
                                        (self.__target_root, self.__version),
                   depends = "fetch_gtest",
                   name = "checkout")

        runner.add(command = "cd %s/protobuf && ./autogen.sh" % (self.__target_root),
                   depends = "checkout",
                   name = "autogen")

        runner.add(command = "cd %s/protobuf && ./configure --prefix=%s" % \
                                        (self.__target_root, self.__target_root),
                   depends = "autogen",
                   name = "configure")

        runner.add(command = "cd %s/protobuf && make" % (self.__target_root),
                   depends = "configure",
                   name = "make")

        runner.add(command = "cd %s/protobuf && make check" % (self.__target_root),
                   depends = "make",
                   name = "check")

        runner.add(command = "cd %s/protobuf && make install" % (self.__target_root),
                   depends = "check",
                   name = "install")

        runner.add(command = "rm -rf %s/protobuf" % (self.__target_root),
                   depends = "install",
                   name = "clear")

        return runner.run()
