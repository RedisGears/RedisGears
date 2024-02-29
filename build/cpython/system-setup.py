#!/usr/bin/env python2

import sys
import os
import argparse

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "../.."))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class Python3Setup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        if self.osnick != "centos9":
            self.install_downloaders()

        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git openssl")

    def debian_compat(self):
        self.run("%s/bin/getgcc" % READIES)
        self.install("autotools-dev autoconf libtool gawk")
        self.install("libbz2-dev liblzma-dev lzma-dev libncurses5-dev libsqlite3-dev uuid-dev zlib1g-dev libssl-dev libreadline-dev libffi-dev")
        if sh("apt-cache search libgdbm-compat-dev") != "":
            self.install("libgdbm-compat-dev")
        self.install("libgdbm-dev")

        self.install("lsb-release")

    def redhat_compat(self):
        if self.osnick == "centos9":
            self.install("bzip2-devel expat-devel gdbm-devel glibc-devel gmp-devel libffi-devel libuuid-devel ncurses-devel "
                "openssl-devel readline-devel sqlite-devel xz-devel zlib-devel libatomic file")
            return

        self.run("%s/bin/getepel" % READIES)
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("autoconf automake libtool")

        self.install("bzip2-devel expat-devel gdbm-devel glibc-devel gmp-devel libffi-devel libuuid-devel ncurses-devel "
            "openssl-devel readline-devel sqlite-devel xz-devel zlib-devel")

        self.install("redhat-lsb-core")
        self.install("libatomic file")

        if self.arch == 'x64':
            self.install_linux_gnu_tar()

    def fedora(self):
        self.run("%s/bin/getgcc" % READIES)
        self.install("autoconf automake libtool")
        self.install("bzip2-devel expat-devel gdbm-devel glibc-devel gmp-devel libffi-devel libnsl2-devel libuuid-devel ncurses-devel "
            "openssl-devel readline-devel sqlite-devel xz-devel zlib-devel")

        self.install("libatomic file")

    def linux_last(self):
        self.install("vim-common") # for xxd
        self.install("zip unzip")

    def macos(self):
        self.install("libtool autoconf automake")
        self.run("""
            dir=$(mktemp -d /tmp/gettext.XXXXXX)
            base=$(pwd)
            cd $dir
            wget -q -O gettext.tgz https://ftp.gnu.org/pub/gnu/gettext/gettext-0.20.2.tar.gz
            tar xzf gettext.tgz
            cd gettext-0.20.2
            ./configure
            make
            make install
            cd $base
            rm -rf $dir
            """)

        self.install("zlib openssl readline coreutils libiconv")
        self.install("binutils") # into /usr/local/opt/binutils
        self.install_gnu_utils()

    def common_last(self):
        pass

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for RedisGears build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

Python3Setup(nop = args.nop).setup()
