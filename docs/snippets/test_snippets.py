import docker
import pytest
import redis
import time

@pytest.fixture(scope='module')
def standalone(image):
    if image != None:
        print(f'Lauching Docker {image} container ', end='', flush=True)
        dc = docker.from_env()
        container = dc.containers.run(
            image,
            detach=True,
            remove=True,
            ports={
                '6379': 6379,
            }
        )
        for out in container.logs(stream=True):
            try:
                if out.lower().index(b'error'):
                    raise Exception(out)
            except ValueError:
                pass
            if out.endswith(b'Ready to accept connections\n'):
                break
            else:
                print('.', flush=True, end='')
        print(' ready!', flush=True)

    conn = redis.Redis()
    conn.ping()
    yield conn
    conn.close()

    if image != None:
        print(f'\nShutting container', end='', flush=True)
        container.stop()

@pytest.mark.parametrize(
    'snippet,expected',
    [
        ('docs/snippets/examples/automatic-expire.py', b'OK'),
        ('docs/snippets/examples/del-by-prefix.py', [[], []]),
        ('docs/snippets/examples/monte-carlo-pi.py', [[b'foo'], []]),
        ('docs/snippets/examples/notification-processing.py', b'OK'),
        ('docs/snippets/examples/stream-logger.py', b'OK'),
        ('docs/snippets/examples/word-count.py', [[], []]),
        ('docs/snippets/functions/register.py', b'OK'),
        ('docs/snippets/functions/run.py', [[], []]),
        ('docs/snippets/intro/intro-000.py', b'OK'),
        ('docs/snippets/intro/intro-001.py', [[], []]),
        ('docs/snippets/intro/intro-002.py', [[], []]),
        ('docs/snippets/intro/intro-003.py', [[], []]),
        ('docs/snippets/intro/intro-004.py', b'OK'),
        ('docs/snippets/intro/intro-005.py', [[], []]),
        ('docs/snippets/intro/intro-006.py', [[], []]),
        ('docs/snippets/intro/intro-007.py', b'OK'),
        ('docs/snippets/intro/intro-008.py', b'OK'),
        ('docs/snippets/intro/intro-009.py', [[], []]),
        ('docs/snippets/intro/intro-010.py', [[], []]),
        ('docs/snippets/intro/intro-011.py', [[], []]),
        ('docs/snippets/intro/intro-012.py', [[], []]),
        ('docs/snippets/intro/intro-013.py', [[], []]),
        ('docs/snippets/intro/intro-014.py', [[], []]),
        ('docs/snippets/intro/intro-015.py', [[], []]),
        ('docs/snippets/intro/intro-016.py', [[], []]),
        ('docs/snippets/operations/accumulate.py', [[], []]),
        ('docs/snippets/operations/aggregate.py', [[], []]),
        ('docs/snippets/operations/aggregateby.py', [[], []]),
        ('docs/snippets/operations/avg.py', [[], []]),
        ('docs/snippets/operations/batchgroupby.py', [[], []]),
        ('docs/snippets/operations/collect.py', [[], []]),
        ('docs/snippets/operations/count.py', [[], []]),
        ('docs/snippets/operations/countby.py', [[], []]),
        ('docs/snippets/operations/distinct.py', [[], []]),
        ('docs/snippets/operations/filter.py', [[], []]),
        ('docs/snippets/operations/flatmap.py', [[], []]),
        ('docs/snippets/operations/foreach.py', [[], []]),
        ('docs/snippets/operations/groupby.py', [[], []]),
        ('docs/snippets/operations/limit.py', [[], []]),
        ('docs/snippets/operations/localgroupby.py', [[], []]),
        ('docs/snippets/operations/map.py', [[], []]),
        ('docs/snippets/operations/repartition-001.py', [[], []]),
        ('docs/snippets/operations/repartition-002.py', [[], []]),
        ('docs/snippets/operations/sort.py', [[], []]),
        ('docs/snippets/readers/keysreader-register.py', b'OK'),
        ('docs/snippets/readers/keysreader-run.py', [[], []]),
        ('docs/snippets/readers/pythonreader-run-001.py', [range(6379), []]),
        ('docs/snippets/readers/pythonreader-run-002.py', [range(42), []]),
        ('docs/snippets/readers/shardidreader-run.py', [[1], []]),
        ('docs/snippets/readers/streamreader-register.py', b'OK'),
        ('docs/snippets/readers/streamreader-run.py', [[], []]),
        ('docs/snippets/readers/commandreader-register.py', b'OK'),
        ('docs/snippets/runtime/configget.py', b'OK'),
        ('docs/snippets/runtime/execute.py', b'OK'),
        ('docs/snippets/runtime/atomic.py', [[1], []]),
        ('docs/snippets/runtime/gearsconfigget.py', b'OK'),
        ('docs/snippets/runtime/hashtag.py', b'OK'),
        ('docs/snippets/runtime/log.py', [[], []]),
    ],
)

def test_snippet(standalone, snippet, expected):
    with open(snippet, 'rb') as f:
        src = f.read()
    standalone.flushall()
    r = standalone.execute_command('RG.PYEXECUTE', src)
    if type(expected) is list:
        assert len(expected[0]) == len(r[0])
        assert len(expected[1]) == len(r[1])
    else:
        assert r == expected
