from common import jvmTestDecorator
from common import putKeys

@jvmTestDecorator()
def testAsyncRecordOnMap(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results, [env.shardsCount])

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecordOnFilter(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results, ['x'])

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecordOnForeach(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 2)
    env.assertEqual(sorted(results), sorted(['x', 'y']))

@jvmTestDecorator()
def testAsyncRecrodMapRaiseError(env, errs, **kargs):
    env.assertEqual(len(errs), env.shardsCount)
    env.assertEqual(errs, ['error'] * env.shardsCount)

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecrodFilterRaiseError(env, errs, **kargs):
    env.assertEqual(len(errs), 2)
    env.assertEqual(errs, ['error'] * 2)

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecrodForeachRaiseError(env, errs, **kargs):
    env.assertEqual(len(errs), 2)
    env.assertEqual(errs, ['error'] * 2)

@jvmTestDecorator()
def testAsyncRecrodMapRaiseExcpetion(env, errs, **kargs):
    env.assertEqual(len(errs), env.shardsCount)
    for e in errs:
        env.assertIn('java.lang.RuntimeException: error', e)

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecrodFilterRaiseExcpetion(env, errs, **kargs):
    env.assertGreaterEqual(len(errs), 1)
    env.assertIn('java.lang.RuntimeException: error', errs[0])    

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAsyncRecrodForeachRaiseExcpetion(env, errs, **kargs):
    env.assertGreaterEqual(len(errs), 1)
    env.assertIn('java.lang.RuntimeException: error', errs[0])
    
@jvmTestDecorator()
def testAsyncStepInSyncExecution(env, results, errs, conn, **kargs):
    env.assertEqual(len(errs), 0)
    env.expect('RG.TRIGGER', 'test').equal(['done'])
