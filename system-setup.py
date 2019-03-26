#!/usr/bin/env python3

import os
import platform

dist = platform.linux_distribution()
distname = dist[0].lower()
distver = dist[1]

if platform.system() == 'Linux':
    if distname == 'fedora':
        dnf groupinstall -y 'Development Tools'
        dnf install -y autoconf automake libtool 
        dnf install -y zlib-devel openssl-devel readline-devel
        dnf install -y python3-pip

    if distname == 'ubuntu':
        apt install -y build-essential autotools-dev autoconf libtool
        apt install -y zlib1g-dev libssl-dev libreadline-dev 
        apt install -y python3-pip

    if distname == 'centos linux':
        yum groupinstall -y 'Development Tools'
        yum install -y autoconf automake libtool 
        yum install -y zlib-devel openssl-devel readline-devel

if platform.system() == 'Darwin':
    pass

pip3 install pipenv
