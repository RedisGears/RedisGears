from common import gearsTest

def getSession(env):
	res = env.cmd('RG.PYDUMPSESSIONS')
	env.assertEqual(len(res), 1)
	return res[0][1]

@gearsTest(skipOnCluster=True)
def testProfile(env):
	script = '''
def map_func(r):
	return r

def filter_func(r):
	return True

def flat_map_func(r):
	return [r]

def foreach_func(r):
	pass

def aggregate_func(a, r):
	return a

def aggregateby_func(g, a, r):
	return a

def extractor_func(a):
	return str(a)

GB('CommandReader').map(map_func).filter(filter_func).flatmap(flat_map_func).foreach(foreach_func).aggregate(0, aggregate_func, aggregate_func).aggregateby(extractor_func, 0, aggregateby_func, aggregateby_func).register(trigger='test')
	'''
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', script).ok()

	env.cmd('RG.TRIGGER', 'test')

	sessionId = getSession(env)

	res = env.cmd('RG.PYPROFILE', 'STATS', sessionId)
	env.assertContains('map_func', res)
	env.assertContains('filter_func', res)
	env.assertContains('flat_map_func', res)
	env.assertContains('foreach_func', res)
	env.assertContains('aggregate_func', res)
	env.assertContains('extractor_func', res)
	env.assertContains('aggregateby_func', res)

	res = env.cmd('RG.PYPROFILE', 'STATS', sessionId, 'ncalls')
	env.assertContains('map_func', res)
	env.assertContains('filter_func', res)
	env.assertContains('flat_map_func', res)
	env.assertContains('foreach_func', res)
	env.assertContains('aggregate_func', res)
	env.assertContains('extractor_func', res)
	env.assertContains('aggregateby_func', res)

@gearsTest(skipOnCluster=True)
def testProfileWithoutProfileInfo(env):
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')").ok()
	env.expect('RG.PYPROFILE', 'STATS', '0000000000000000000000000000000000000000-0').error()

@gearsTest(skipOnCluster=True)
def testProfileReset(env):
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')").ok()
	env.cmd('RG.TRIGGER', 'test')
	sessionId = getSession(env)
	env.expect('RG.PYPROFILE', 'RESET', sessionId).ok()
	env.expect('RG.PYPROFILE', 'STATS', sessionId).error()

@gearsTest(skipOnCluster=True, envArgs={'moduleArgs': 'CreateVenv 1'})
def testPyDumpSessions(env):
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')", 'REQUIREMENTS', 'redis==4.3.1').ok()
	sessionId = getSession(env)
	env.expect('RG.PYDUMPSESSIONS').equal([['ID', sessionId, 'sessionDescription', None, 'refCount', 1, 'linked', 'primary', 'dead', 'false', 'requirementInstallationNeeded', 1, 'requirements', ['redis==4.3.1'], 'registrations', ['0000000000000000000000000000000000000000-1']]])
