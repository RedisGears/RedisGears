#!/usr/bin/env python

import redis
import argparse

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Run gears scripts on Redis(Gears)')

parser.add_argument(
    '--host', default='localhost',
    help='redis host')

parser.add_argument(
    '--port', default=6379, type=int,
    help='redis port')

parser.add_argument('path', help='scripts paths', nargs='+',)

parser.add_argument(
    '--password', default=None,
    help='redis password')

parser.add_argument(
    '--nonblocking', default=False, type=bool,
    help='set unblocking run')

args = parser.parse_args()

r = redis.Redis(args.host, args.port, password=args.password)
for p in args.path:
    f = open(p, 'rt')
    script = f.read()
    q = ['rg.pyexecute', script]
    if args.unblocking:
        q += ['unblocking']
    res = r.execute_command(*q)
    print res
    print ''
    f.close()
