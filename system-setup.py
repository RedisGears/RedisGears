#!/usr/bin/env python

import sys
import os
import popen2
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

os.environ["PYTHONWARNINGS"] = 'ignore:DEPRECATION::pip._internal.cli.base_command'

#----------------------------------------------------------------------------------------------

class RedisGearsSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")
        
        self.install("git")

    def debian_compat(self):
        self.install("build-essential autotools-dev autoconf libtool")
        self.install("zlib1g-dev libssl-dev libreadline-dev")
        self.install("xxd")
        self.install("lsb-release")
        self.install("zip unzip")
        self.install("python-psutil")
        self.install("python-gevent") # pip cannot build gevent on ARM
        self.install("libffi-dev")
        self.pip_install("pipenv")

    def redhat_compat(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")
        self.install("zlib-devel openssl-devel readline-devel")
        self.install("redhat-lsb-core")
        self.install("vim-common") # for xxd
        self.install("zip unzip")
        self.install("which") # required by pipenv (on docker)
        self.install("libffi-devel") # required for python 3.7
        self.install("python2-gevent") # pip cannot build gevent on ARM

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil")
        self.install("python2-psutil")

        self.install("python2-gevent")
        self.pip_install("pipenv")

    def fedora(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")
        self.install("zlib-devel openssl-devel readline-devel")
        self.install("vim-common") # for xxd
        self.install("libffi-devel")

        self.pip_install("pipenv")
        self.pip_install("gevent")

    def macosx(self):
        r, w, e = popen2.popen3('xcode-select -p')
        if r.readlines() == []:
            fatal("Xcode tools are not installed. Please run xcode-select --install.")
        self.install("libtool autoconf automake")
        self.install("zlib openssl readline")
        
        self.pip_install("pipenv")
        self.pip_install("gevent")

    def common_last(self):
        if not self.has_command("ramp"):
            self.pip_install("git+https://github.com/RedisLabs/RAMP --upgrade")
        if not self.has_command("RLTest"):
            self.pip_install("git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("redis-py-cluster")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop).setup()
