#!/usr/bin/env python3

import sys
import os
import argparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
READIES = os.path.join(ROOT, "deps", "readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class JVMSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    # assumes we've already run system-setup at the top
    def common(self):
        self.install("maven")
        self.run("%s/bin/getredis --version 6" % READIES)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

JVMSetup(nop=args.nop).setup()
