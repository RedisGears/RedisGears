from common import jvmTestDecorator
from common import putKeys

def putStream(s):
	def func(conn, **kargs):
		for k, v in s.items():
			for val in v:
				conn.execute_command('XADD', k, '*', *sum([[a,b] for a,b in val.items()], []))
	return func

@jvmTestDecorator(preExecute=putStream({'s':[{'foo':'bar', 'foo1':'bar1'}, {'foo2':'bar2', 'foo3':'bar3'}]}))
def testStreamReaderFromId(env, conn, results, **kargs):
	expectedRes = [{u'foo': u'bar', u'foo1': u'bar1'}, {u'foo2': u'bar2', u'foo3': u'bar3'}]
	for res in results:
		env.assertIn(res['value'], expectedRes)

@jvmTestDecorator()
def testStreamRegisterArgs(env, **kargs):
	res = env.cmd('RG.DUMPREGISTRATIONS')
	env.assertEqual(res[0][7][13], ['batchSize', 100L, 'durationMS', 1000L, 'stream', 's'])
