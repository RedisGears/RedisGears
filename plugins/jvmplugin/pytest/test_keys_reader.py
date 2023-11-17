from common import jvmTestDecorator
from common import putKeys

@jvmTestDecorator(preExecute=putKeys({'pref1:1':'1', 'pref2:1':'1', 'pref3:1':'1'}))
def testKeyReaderPattern(env, results, errs, **kargs):
	env.assertEqual(len(results), 1)
	env.assertEqual(results[0], 'pref1:1')

@jvmTestDecorator(preExecute=putKeys({'pref1:1':'1', 'pref2:1':'1', 'pref3:1':'1'}))
def testKeyReaderNoValues(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)

@jvmTestDecorator(preExecute=putKeys({'pref1:1':'1', 'pref2:1':'1', 'pref3:1':'1'}))
def testKeyReaderNoScan(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)

@jvmTestDecorator(preExecute=putKeys({'pref1:1':'1', 'pref2:1':'1', 'pref3:1':'1'}))
def testKeysOnlyReader(env, results, errs, **kargs):
	env.assertEqual(len(results), 3)

@jvmTestDecorator(preExecute=putKeys({'pref1:1':'1', 'pref2:1':'1', 'pref3:1':'1'}))
def testKeysOnlyReaderPattern(env, results, errs, **kargs):
	env.assertEqual(len(results), 1)	
