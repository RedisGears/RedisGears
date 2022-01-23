from common import jvmTestDecorator
from common import putKeys

@jvmTestDecorator(preExecute=putKeys({'x':'1'}))
def testBasic(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['key'], 'x')
    env.assertEqual(results[0]['stringVal'], '1')
    
@jvmTestDecorator(preExecute=putKeys({'x':'1'}))
def testMap(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0], '1')

@jvmTestDecorator(preExecute=putKeys({'x':{'y':'1'}}))
def testForeach(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['hashVal']['test'], 'test')

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testFilter(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['key'], 'x')

@jvmTestDecorator(preExecute=putKeys({'x':'1', 'y':'2'}))
def testAccumulate(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0], 2)

@jvmTestDecorator(preExecute=putKeys({'x':['1', '2', '3']}))
def testFlatMap(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 3)
    env.assertIn('1', results)
    env.assertIn('2', results)
    env.assertIn('3', results)

@jvmTestDecorator(preExecute=putKeys({'x':'foo', 'y':'bar', 'z':'foo'}))
def testAccumulateby(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 2)
    env.assertEqual(results, [{'bar':1}, {'foo':2}])

