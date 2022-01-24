from common import jvmTestDecorator
from common import putKeys

@jvmTestDecorator()
def testShardsIDReader(env, results, errs, executionError, **kargs):
	env.assertEqual(len(errs), 0)
	env.assertEqual(len(results), 1)
	env.assertEqual(results[0], env.shardsCount)
