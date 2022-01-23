#!/usr/bin/env python3

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "..", "..", "deps", "readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class JVMSetup(paella.Setup):
    def __init__(self, nop=False, with_python=True):
        paella.Setup.__init__(self, nop=nop)
        self.with_python = with_python

    def debian_compat(self):
        if self.osnick == 'xenial':
            self.install("openjdk-8-jdk")

    def common_last(self):
        self.install("maven")
        self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for JVM build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
parser.add_argument('--with-python', action="store_true", default=True, help='with Python')
args = parser.parse_args()

JVMSetup(nop = args.nop, with_python=args.with_python).setup()
