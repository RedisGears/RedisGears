import time
blocked = {}

# Example of blocking a client until a key has expired

def block(r):
	f = gearsFuture()
	key = r[1]
	if key not in blocked.keys():
		blocked[key] = []	
	blocked[key].append(f)
	return f

def unblock(r):
	res = 0
	key = r['key']
	futures = blocked.pop(key, None)
	if futures:
		res = len([f.continueRun('%s expired' % key) for f in futures])
		blocked[key]
	return res

GB('CommandReader').map(block).register(trigger='WaitForKeyExpiration', mode='sync')
GB().map(unblock).register(eventTypes=['expired'], mode='sync')
