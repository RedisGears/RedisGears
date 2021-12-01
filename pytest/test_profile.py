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
	env.skipOnCluster()
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', script).ok()

	env.cmd('RG.TRIGGER', 'test')

	res = env.cmd('RG.PYPROFILESTATS', '0000000000000000000000000000000000000000-0')
	env.assertContains('map_func', res)
	env.assertContains('filter_func', res)
	env.assertContains('flat_map_func', res)
	env.assertContains('foreach_func', res)
	env.assertContains('aggregate_func', res)
	env.assertContains('extractor_func', res)
	env.assertContains('aggregateby_func', res)

	res = env.cmd('RG.PYPROFILESTATS', '0000000000000000000000000000000000000000-0', 'ncalls')
	env.assertContains('map_func', res)
	env.assertContains('filter_func', res)
	env.assertContains('flat_map_func', res)
	env.assertContains('foreach_func', res)
	env.assertContains('aggregate_func', res)
	env.assertContains('extractor_func', res)
	env.assertContains('aggregateby_func', res)

def testProfileWithoutProfileInfo(env):
	env.skipOnCluster()
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')").ok()
	env.expect('RG.PYPROFILESTATS', '0000000000000000000000000000000000000000-0').error()

def testProfileReset(env):
	env.skipOnCluster()
	env.expect('RG.CONFIGSET', 'ProfileExecutions', '1').equal(['OK'])
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')").ok()
	env.cmd('RG.TRIGGER', 'test')
	env.expect('RG.PYPROFILERESET', '0000000000000000000000000000000000000000-0').ok()
	env.expect('RG.PYPROFILESTATS', '0000000000000000000000000000000000000000-0').error()

def testPyDumpSessions(env):
	env.skipOnCluster()
	env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')", 'REQUIREMENTS', 'redis==3.5.3').ok()
	env.expect('RG.PYDUMPSESSIONS').equal([['id', '0000000000000000000000000000000000000000-0', 'refCount', 1L, 'requirementInstallationNeeded', 1L, 'requirements', [['name', 'redis==3.5.3', 'refCount', 2L, 'isDownloaded', 1L, 'isInstalled', 1L, 'wheels', ['redis-3.5.3-py2.py3-none-any.whl']]]]])
