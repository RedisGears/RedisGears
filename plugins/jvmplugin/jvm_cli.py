import redis
import sys

r = redis.Redis()

with open(sys.argv[2], 'rb') as f:
	print(r.execute_command('RG.JEXECUTE', sys.argv[1], f.read()))
