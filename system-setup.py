#!/usr/bin/env python3

import os
import sys
import platform

dist = platform.linux_distribution()
distname = dist[0].lower()
distver = dist[1]

def install(cmd):
    rc = os.system(cmd)
    if rc > 0:
        print("failed to install " + packs, file=sys.stderr)
        sys.exit(1)

def apt_install(packs):
    install("apt install -y " + packs)

def yum_install(packs, group=False):
    if not group:
        install("yum install -y " + packs)
    else:
        install("yum groupinstall -y " + packs)

def dnf_install(packs, group=False):
    if not group:
        install("dnf install -y " + packs)
    else:
        install("dnf groupinstall -y " + packs)

def pip_install(cmd):
    install("pip install " + cmd)

#----------------------------------------------------------------------------------------------

pip_install("wheel")
pip_install("setuptools --upgrade")

pip_install("git+https://github.com/RedisLabs/RAMP --upgrade")
pip_install("git+https://github.com/RedisLabsModules/RLTest.git@master")
pip_install("redis-py-cluster")

if platform.system() == 'Linux':
    if distname == 'fedora':
        dnf_install("'Development Tools'", group=True)
        dnf_install("autoconf automake libtool")
        dnf_install("zlib-devel openssl-devel readline-devel")
        dnf_install("python3-pip")

    if distname == 'ubuntu':
        apt_install("build-essential autotools-dev autoconf libtool")
        apt_install("zlib1g-dev libssl-dev libreadline-dev")
        apt_install("lsb-release")
        yum_install("zip unzip")
        apt_install("python3-pip")

    if distname == 'centos linux':
        yum_install("'Development Tools'", group=True)
        yum_install("autoconf automake libtool")
        yum_install("zlib-devel openssl-devel readline-devel")
        yum_install("redhat-lsb-core")
        yum_install("vim-common") # for xxd
        yum_install("zip unzip")
        yum_install("python36-pip");
        install("pip3.6 install --upgrade pip");

if platform.system() == 'Darwin':
    pass

install("pip3 install pipenv")
