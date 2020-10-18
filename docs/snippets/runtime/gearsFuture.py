import time
blocked = {}

# Example of blocking a client until a key has expired

def bc(r):
	f = gearsFuture()
	blocked[r[1]] = f
	return f

def unbc(r):
	f = blocked.pop(r['key'], None)
	if f:
		f.continueRun('%s expired' % r['key'])

GB('CommandReader').map(bc).register(trigger='WaitForKeyExpiration')
GB().foreach(unbc).register(eventTypes=['expired'])