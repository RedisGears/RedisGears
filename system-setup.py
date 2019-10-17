#!/usr/bin/env python2

import sys
import os
import popen2
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

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
        self.install("vim-common") # for xxd
        self.install("lsb-release")
        self.install("zip unzip")

        # pip cannot build gevent on ARM
        self.install("python-psutil python-gevent")
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

        # pip cannot build gevent on ARM
        self.install("python-gevent python2-ujson")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil")
        self.install("python2-psutil")
        
        self.pip_install("pipenv")

    def fedora(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")
        self.install("zlib-devel openssl-devel readline-devel")
        self.install("vim-common") # for xxd
        self.install("libffi-devel")

        self.install("python2-ujson")
        self.pip_install("pipenv gevent")

    def macosx(self):
        r, w, e = popen2.popen3('xcode-select -p')
        if r.readlines() == []:
            fatal("Xcode tools are not installed. Please run xcode-select --install.")
        self.install("libtool autoconf automake llvm")
        self.install("zlib openssl readline")
        self.install("redis")
        self.install("binutils") # into /usr/local/opt/binutils
        
        self.pip_install("pipenv gevent")

    def common_last(self):
        # RLTest before RAMP due to redis<->redis-py-cluster version dependency
        self.pip_install("git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("git+https://github.com/RedisLabs/RAMP@master")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop).setup()
