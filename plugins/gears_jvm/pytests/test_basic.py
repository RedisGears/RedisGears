from common import jvmTestDecorator

def addKeys(data):
    def func(conn):
        for k, v in data.items():
            if isinstance(v, str):
                conn.execute_command('set', k, v)
            elif isinstance(v, list):
                conn.execute_command('lpush', k, *v)
            elif isinstance(v, dict):
                for key, val in v.items():
                    conn.execute_command('hset', k, key, val)
    return func

@jvmTestDecorator(preExecute=addKeys({'x':'1'}))
def testBasic(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['key'], 'x')
    env.assertEqual(results[0]['stringVal'], '1')
    
@jvmTestDecorator(preExecute=addKeys({'x':'1'}))
def testMap(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0], '1')

@jvmTestDecorator(preExecute=addKeys({'x':{'y':'1'}}))
def testForeach(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['hashVal']['test'], 'test')

@jvmTestDecorator(preExecute=addKeys({'x':'1', 'y':'2'}))
def testFilter(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0]['key'], 'x')

@jvmTestDecorator(preExecute=addKeys({'x':'1', 'y':'2'}))
def testAccumulate(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 1)
    env.assertEqual(results[0], 2)

@jvmTestDecorator(preExecute=addKeys({'x':['1', '2', '3']}))
def testFlatMap(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 3)
    env.assertIn('1', results)
    env.assertIn('2', results)
    env.assertIn('3', results)

@jvmTestDecorator(preExecute=addKeys({'x':'foo', 'y':'bar', 'z':'foo'}))
def testAccumulateby(env, results, errs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(len(results), 2)
    env.assertEqual(results, [{'bar':1}, {'foo':2}])

