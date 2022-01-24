from common import jvmTestDecorator
from common import putKeys

@jvmTestDecorator()
def testExecutionError(env, executionError, **kargs):
	env.assertIn('Test Exception', executionError)

@jvmTestDecorator()
def testClassNotExistsError(env, executionError, **kargs):
	env.assertIn('gears_tests.testClassNotExistsError', executionError)

@jvmTestDecorator()
def testClassWithoutMainFunction(env, executionError, **kargs):
	env.assertIn('main', executionError)

@jvmTestDecorator()
def testClassWithInvalidMainFunction(env, executionError, **kargs):
	env.assertIn('main', executionError)

@jvmTestDecorator(preExecute=putKeys({'x':'1'}))
def testMapError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Map Error', errs[0])

@jvmTestDecorator(preExecute=putKeys({'x':'1'}))
def testFilterError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Filter Error', errs[0])

@jvmTestDecorator(preExecute=putKeys({'x':['1', '2', '3']}))
def testFlatmapError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Flatmap Error', errs[0])

@jvmTestDecorator(preExecute=putKeys({'x':'1'}))
def testAccumulateError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Accumulate Error', errs[0])

@jvmTestDecorator(preExecute=putKeys({'x':'foo'}))
def testAccumulatebyExtractorError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('AccumulatebyExtractor Error', errs[0])	

@jvmTestDecorator(preExecute=putKeys({'x':'foo'}))
def testAccumulatebyError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Accumulateby Error', errs[0])

@jvmTestDecorator(preExecute=putKeys({'x':'foo'}))
def testForeachError(env, results, errs, **kargs):
	env.assertEqual(len(results), 0)
	env.assertEqual(len(errs), 1)
	env.assertIn('Foreach Error', errs[0])

@jvmTestDecorator()
def testNoSerializableError(env, executionError, **kargs):
	env.assertIn('java.net.ServerSocket', executionError)

@jvmTestDecorator()
def testNoSerializableExecutionError(env, executionError, **kargs):
	if env.shardsCount == 1:
		env.skip()
	env.assertIn('java.net.ServerSocket', executionError)

@jvmTestDecorator()
def testNullCommandArgument(env, executionError, **kargs):
	env.assertIn('Got a NULL command', executionError)

@jvmTestDecorator()
def testNullArgumentInCommand(env, executionError, **kargs):
	env.assertIn('Got a null argument on command', executionError)

@jvmTestDecorator()
def testNullCommandArgumentInCommand(env, executionError, **kargs):
	env.assertIn('Null command given to execute', executionError)

@jvmTestDecorator()
def testNullMsgToLog(env, executionError, **kargs):
	env.assertIn('Got a NULL msg on log function', executionError)

@jvmTestDecorator()
def testNullKeyToConfigGet(env, executionError, **kargs):
	env.assertIn('Got a NULL key on configGet', executionError)

@jvmTestDecorator()
def testNullMapper(env, executionError, **kargs):
	env.assertIn('Null mapper given', executionError)

@jvmTestDecorator()
def testNullFilterer(env, executionError, **kargs):
	env.assertIn('Null filter function given', executionError)

@jvmTestDecorator()
def testNullFlatMapper(env, executionError, **kargs):
	env.assertIn('Null mapper given', executionError)

@jvmTestDecorator()
def testNullRepartitionExtractor(env, executionError, **kargs):
	env.assertIn('Null extractor given', executionError)

@jvmTestDecorator()
def testNullForeacher(env, executionError, **kargs):
	env.assertIn('Null foreach function given', executionError)

@jvmTestDecorator()
def testNullAccumulator(env, executionError, **kargs):
	env.assertIn('Null accumulator given', executionError)

@jvmTestDecorator()
def testNullExtractorToAccumulateby(env, executionError, **kargs):
	env.assertIn('Null extractor given', executionError)

@jvmTestDecorator()
def testNullAccumulatorToAccumulateby(env, executionError, **kargs):
	env.assertIn('Null accumulator given', executionError)

@jvmTestDecorator()
def testNullExtractorToLocalAccumulateby(env, executionError, **kargs):
	env.assertIn('Null extractor given', executionError)

@jvmTestDecorator()
def testNullAccumulatorToLocalAccumulateby(env, executionError, **kargs):
	env.assertIn('Null accumulator given', executionError)
