#!/usr/bin/env python2

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RedisGearsSetup(paella.Setup):
    def __init__(self, nop=False, with_python=True):
        paella.Setup.__init__(self, nop=nop)
        self.with_python = with_python

    def common_first(self):
        self.install_downloaders()

        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git openssl")

    def debian_compat(self):
        self.install("build-essential autotools-dev autoconf libtool gawk")

        self.install("lsb-release")
        self.install("zip unzip")

        # pip cannot build gevent on ARM
        self.install("python-psutil")
        if self.dist == 'ubuntu' and int(self.ver.split('.')[0]) < 20:
            self.install("python-gevent")
        else:
            self.pip_install("gevent")

    def redhat_compat(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")

        self.install("redhat-lsb-core")
        self.install("zip unzip")
        self.install("libatomic file")

        self.run("%s/bin/getepel" % READIES)

        self.run("""
            dir=$(mktemp -d /tmp/tar.XXXXXX)
            (cd $dir; wget -q -O tar.tgz http://redismodules.s3.amazonaws.com/gnu/gnu-tar-1.32-x64-centos7.tgz; tar -xzf tar.tgz -C /; )
            rm -rf $dir
            """)

        # pip cannot build gevent on ARM
        self.install("python-gevent python-ujson")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil || true")
        self.install("python2-psutil")

    def fedora(self):
        self.group_install("'Development Tools'")

        self.install("which libatomic file")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil || true")
        self.install("python2-psutil")

        self.install("python2-ujson")
        # self.pip_install("gevent")

    def linux_last(self):
        self.install("valgrind")

    def macos(self):
        self.install("libtool autoconf automake")

        self.install("openssl readline coreutils")
        if not self.has_command("redis-server"):
            self.install("redis")
        self.install("binutils") # into /usr/local/opt/binutils
        self.install_gnu_utils()

        # self.pip_install("gevent")

    def common_last(self):
        if self.with_python:
            self.run(self.python + " {ROOT}/build/cpython/system-setup.py {NOP}".
                     format(ROOT=ROOT, NOP="--nop" if self.runner.nop else ""),
                     output=True)
        self.run("PYTHON=%s %s/bin/getrmpytools" % (self.python, READIES))
        self.run("pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest || true")
        # redis-py-cluster should be installed from git due to redis-py dependency
        # self.pip_install("--no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabs/RAMP@master")

        # self.pip_install("-r %s/paella/requirements.txt" % READIES)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
parser.add_argument('--with-python', action="store_true", default=True, help='no operation')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop, with_python=args.with_python).setup()
