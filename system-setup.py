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
        self.install_downloaders()

        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git")

    def debian_compat(self):
        self.install("build-essential autotools-dev autoconf libtool gawk")
        self.install("libbz2-dev liblzma-dev lzma-dev libncurses5-dev libsqlite3-dev uuid-dev zlib1g-dev libssl-dev libreadline-dev libffi-dev")
        if sh("apt-cache search libgdbm-compat-dev") != "":
            self.install("libgdbm-compat-dev")
        self.install("libgdbm-dev")
        self.install("tcl-dev tix-dev tk-dev")

        self.install("vim-common") # for xxd
        self.install("lsb-release")
        self.install("zip unzip")

        # pip cannot build gevent on ARM
        self.install("python-psutil python-gevent")
        self.pip_install("pipenv")

    def redhat_compat(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")

        self.install("bzip2-devel expat-devel gdbm-devel glibc-devel gmp-devel libffi-devel libuuid-devel ncurses-devel "
            "openssl-devel readline-devel sqlite-devel xz-devel zlib-devel")
        self.install("tcl-devel tix-devel tk-devel")

        self.install("redhat-lsb-core")
        self.install("vim-common") # for xxd
        self.install("zip unzip")
        self.install("which") # required by pipenv (on docker)
        self.install("libatomic file")

        self.run("wget -q -O /tmp/epel-release-latest-7.noarch.rpm http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm")
        self.run("rpm -Uv /tmp/epel-release-latest-7.noarch.rpm ")

        # pip cannot build gevent on ARM
        self.install("python-gevent python-ujson")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil")
        self.install("python2-psutil")

        self.pip_install("pipenv")

    def fedora(self):
        self.group_install("'Development Tools'")
        self.install("autoconf automake libtool")
        self.install("bzip2-devel expat-devel gdbm-devel glibc-devel gmp-devel libffi-devel libnsl2-devel libuuid-devel ncurses-devel "
            "openssl-devel readline-devel sqlite-devel xz-devel zlib-devel")
        self.install("tcl-devel tix-devel tk-devel")

        self.install("vim-common") # for xxd
        self.install("which libatomic file")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil")
        self.install("python2-psutil")

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
        self.install_gnu_utils()

        self.pip_install("pipenv gevent")

    def common_last(self):
        # this is due to rmbuilder older versions. should be removed once fixed.
        self.run("pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest rmtest semantic-version || true")
        # redis-py-cluster should be installed from git due to redis-py dependency
        self.pip_install("--no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master")
        # the following can be probably installed from pypi
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabs/RAMP@master")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop).setup()
