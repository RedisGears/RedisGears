#!/usr/bin/env python3

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

        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git openssl")

    def debian_compat(self):
        self.run("%s/bin/getgcc" % READIES)
        self.install("autotools-dev autoconf libtool")

        self.install("lsb-release")
        self.install("zip unzip gawk")

        # pip cannot build gevent on ARM
        if self.platform.is_arm() and self.dist == 'ubuntu' and self.os_version[0] < 20:
            self.install("python3-gevent")
        else:
            self.pip_install("gevent")

    def redhat_compat(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("autoconf automake libtool")

        self.install("redhat-lsb-core")
        self.install("zip unzip")
        self.install("libatomic file")

        self.run("%s/bin/getepel" % READIES)

        if self.arch == 'x64':
            self.install_linux_gnu_tar()

        if self.platform.is_arm() or self.dist == 'centos' and self.os_version[0] == 8:
            self.install("python3-gevent python3-ujson")
        else:
            self.pip_install("gevent ujson")

    def fedora(self):
        self.run("%s/bin/getgcc" % READIES)

        self.install("libatomic file")

        self.install("python3-ujson")
        self.pip_install("gevent")

    def linux_last(self):
        self.install("valgrind")

    def macos(self):
        self.install("make libtool autoconf automake")

        self.install("openssl readline coreutils")
        if not self.has_command("redis-server"):
            self.install("redis")
        self.install("binutils") # into /usr/local/opt/binutils
        self.install_gnu_utils()

        self.pip_install("gevent")

    def common_last(self):
        if self.with_python:
            self.run("{PYTHON} {ROOT}/build/cpython/system-setup.py {NOP}".
                     format(PYTHON=self.python, ROOT=ROOT, NOP="--nop" if self.runner.nop else ""),
                     output=True)
        self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
parser.add_argument('--with-python', action="store_true", default=True, help='with Python')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop, with_python=args.with_python).setup()
