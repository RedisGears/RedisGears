#!/usr/bin/env python

import redis
import time
import argparse

NUM_REQ = 100000

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Gears Write Behind test')

parser.add_argument('--host', default='localhost', help='Redis host')
parser.add_argument('--port', default=6379, type=int, help='Redis port')
parser.add_argument('--password', default=None, help='Redis password')

args = parser.parse_args()

conn = redis.Redis(args.host, args.port, password=args.password)

reqs = ['hset person2:%d first_name foo last_name bar age 31' % i for i in range(NUM_REQ)]
dels = ['del person2:%d' % i for i in range(NUM_REQ)]

start = time.time()
i = 0
for r in reqs:
    p = conn.pipeline(transaction=False)
    p.execute_command(r)
    p.wait(1, 10)
    p.execute()
    i += 1
    if i % 100 == 0:
        print("hset " + str(i))

i = 0
for d in dels:
    p = conn.pipeline(transaction=False)
    p.execute_command(d)
    p.wait(1, 10)
    p.execute()
    i += 1
    if i % 100 == 0:
        print("del " + str(i))

end = time.time()

took = ((end - start)* 1000)
avg = took / NUM_REQ

print('took : %sms' % str(took))
print('avg : %sms' % str(avg))
