import redisgears
import copy
import redisgears as rg
import cProfile
import pstats
import io
from redisgears import executeCommand as execute
from redisgears import callNext as call_next
from redisgears import getCommand as get_command
from redisgears import overrideReply as override_reply
from redisgears import atomicCtx as atomic
from redisgears import getMyHashTag as hashtag
from redisgears import registerTimeEvent as registerTE
from redisgears import gearsCtx
from redisgears import gearsFutureCtx as gearsFuture
from redisgears import log
from redisgears import setGearsSession
from redisgears import getGearsSession
from redisgears import registerGearsThread
from redisgears import isInAtomicBlock
from redisgears import config_get as configGet
from redisgears import PyFlatExecution
from redisgears import isAsyncAllow as isAsyncAllow
from redisgears import flatError as flat_error
import asyncio
from asyncio.futures import Future
from threading import Thread

globals()['str'] = str

redisgears._saveGlobals()

def createKeysOnlyReader(pattern='*', count=1000, noScan=False, patternGenerator=None):
    '''
    Create a KeysOnlyReader callback as a python reader
    pattern          - keys pattern to return
    count            - count parameter given to scan command (ignored if isPattern is false)
    noScan           - boolean indicating if exact match is require, i.e, do not use scan and just return the
                       give pattern
    patternGenerator - a callbacks to generate different pattern on each shard. If given, the callback
                       will run on each shard and the return tuple (pattern, isPattern) will be used.
                       If this argument is given the pattern and noScan arguments are ignored.
    '''
    def keysOnlyReader():
        nonlocal pattern
        nonlocal count
        nonlocal noScan
        nonlocal patternGenerator
        if patternGenerator is not None:
            pattern, noScan = patternGenerator()
        if noScan:
            try:
                if execute('exists', pattern) == 1:
                    yield pattern
            except Exception:
                return
        else:
            count = str(count)
            cursor, keys = execute('scan', '0', 'MATCH', str(pattern), 'COUNT', count)
            while cursor != '0':
                for k in keys:
                    yield k
                cursor, keys = execute('scan', cursor, 'MATCH', str(pattern), 'COUNT', count)
            for k in keys:
                yield k
    return keysOnlyReader

def shardReaderCallback():
    res = execute('RG.INFOCLUSTER')
    if res == 'no cluster mode':
        yield '1'
    else:
        yield res[1]


class GearsBuilder():
    def __init__(self, reader='KeysReader', defaultArg='*', desc=None):
        self.realReader = reader
        if(reader == 'ShardsIDReader'):
            reader = 'PythonReader'
        if(reader == 'KeysOnlyReader'):
            reader = 'PythonReader'
        self.reader = reader
        self.gearsCtx = gearsCtx(self.reader, desc)
        self.defaultArg = defaultArg

    def __localAggregateby__(self, extractor, zero, aggregator):
        self.gearsCtx.localgroupby(lambda x: extractor(x), lambda k, a, r: aggregator(k, a if a else copy.deepcopy(zero), r))
        return self

    def aggregate(self, zero, seqOp, combOp):
        '''
        perform aggregation on all the execution data.
        zero - the first value that will pass to the aggregation function
        seqOp - the local aggregate function (will be performed on each shard)
        combOp - the global aggregate function (will be performed on the results of seqOp from each shard)
        '''
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else copy.deepcopy(zero), r))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: combOp(a if a else copy.deepcopy(zero), r))
        return self

    def aggregateby(self, extractor, zero, seqOp, combOp):
        '''
        Like aggregate but on each key, the key is extracted using the extractor.
        extractor - a function that get as input the record and return the aggregated key
        zero - the first value that will pass to the aggregation function
        seqOp - the local aggregate function (will be performed on each shard)
        combOp - the global aggregate function (will be performed on the results of seqOp from each shard)
        '''
        self.__localAggregateby__(extractor, zero, seqOp)
        self.gearsCtx.groupby(lambda r: r['key'], lambda k, a, r: combOp(k, a if a else copy.deepcopy(zero), r['value']))
        return self

    def count(self):
        '''
        Count the number of records in the execution
        '''
        self.gearsCtx.accumulate(lambda a, r: 1 + (a if a else 0))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: r + (a if a else 0))
        return self

    def countby(self, extractor=lambda x: x):
        '''
        Count, for each key, the number of recors contains this key.
        extractor - a function that get as input the record and return the key by which to perform the counting
        '''
        self.aggregateby(extractor, 0, lambda k, a, r: 1 + a, lambda k, a, r: r + a)
        return self

    def sort(self, reverse=True):
        '''
        Sorting the data
        '''
        self.aggregate([], lambda a, r: a + [r], lambda a, r: a + r)
        self.map(lambda r: sorted(r, reverse=reverse))
        self.flatmap(lambda r: r)
        return self

    def distinct(self):
        '''
        Keep only the distinct values in the data
        '''
        return self.aggregate(set(), lambda a, r: a | set([r]), lambda a, r: a | r).flatmap(lambda x: list(x))

    def avg(self, extractor=lambda x: float(x)):
        '''
        Calculating average on all the records
        extractor - a function that gets the record and return the value by which to calculate the average
        '''
        # we aggregate using a tupple, the first entry is the sum of all the elements,
        # the second element is the amount of elements.
        # After the aggregate phase we just devide the sum in the amount of elements and get the avg.
        return self.map(extractor).aggregate((0, 0),
                                             lambda a, r: (a[0] + r, a[1] + 1),
                                             lambda a, r: (a[0] + r[0], a[1] + r[1])).map(lambda x: x[0] / x[1])

    def run(self, arg=None, convertToStr=True, collect=True, **kargs):
        '''
        Starting the execution
        '''
        if(convertToStr):
            self.gearsCtx.map(lambda x: str(x))
        if(collect):
            self.gearsCtx.collect()
        arg = arg if arg else self.defaultArg
        if(self.realReader == 'ShardsIDReader'):
            arg = shardReaderCallback
        if(self.realReader == 'KeysOnlyReader'):
            arg = createKeysOnlyReader(arg, **kargs)
        return self.gearsCtx.run(arg, **kargs)

    def register(self, prefix='*', convertToStr=True, collect=True, **kwargs):
        if(convertToStr):
            self.gearsCtx.map(lambda x: str(x))
        if(collect):
            self.gearsCtx.collect()
        kwargs['prefix'] = prefix # this is for backword comptability
        if 'regex' in kwargs:
            log('Using regex argument with register is deprecated and missleading, use prefix instead.', level='warning')
            kwargs['prefix'] = kwargs['regex']
        return self.gearsCtx.register(**kwargs)

def createDecorator(f):
    def deco(self, *args):
        f(self.gearsCtx, *args)
        return self
    return deco


for k in PyFlatExecution.__dict__:
    if k in GearsBuilder.__dict__:
        continue
    if '_' in k:
        continue
    setattr(GearsBuilder, k, createDecorator(PyFlatExecution.__dict__[k]))

GB = GearsBuilder

def gearsConfigGet(key, default=None):
    val = configGet(key)
    return val if val is not None else default

def genDeprecated(deprecatedName, name, target):
    def method(*argc, **nargs):
        log('%s is deprecated, use %s instead' % (str(deprecatedName), str(name)), level='warning')
        return target(*argc, **nargs)
    globals()[deprecatedName] = method

# Gears loop of async support

class GearsSession():
    def __init__(self, s):
        self.s = s
        
    def __enter__(self):
        setGearsSession(self.s)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        setGearsSession(None)

class GearsFuture(Future):
    def __init__(self, *, loop=None, gearsSession=None):
        Future.__init__(self, loop=loop)
        self.gearsSession = gearsSession
        
    def add_done_callback(self, fn, *, context=None):
        def BeforeFn(*args, **kargs):
            try:
                with GearsSession(self.gearsSession):
                    fn(*args, **kargs)
            except Exception as e:
                log(e)
        Future.add_done_callback(self, BeforeFn, context=context)
        
    def __await__(self):
        res = None
        if isInAtomicBlock():
            raise Exception("await is not allow inside atomic block")
        else:
            res = yield from Future.__await__(self)
        return res
        

loop = asyncio.new_event_loop()

loop.create_future = lambda: GearsFuture(loop=loop, gearsSession=getGearsSession())

def createFuture():
    # we need to use globals()['loop'] so the function will not break serialization
    return globals()['loop'].create_future()

def setFutureResults(f, res):
    # we need to use globals()['loop'] so the function will not break serialization
    async def setFutureRes():
        f.set_result(res)
    asyncio.run_coroutine_threadsafe(setFutureRes(), globals()['loop'])    

def setFutureException(f, exception):
    async def setException():
        if isinstance(exception, Exception):
            f.set_exception(exception)
        else:
            f.set_exception(Exception(str(exception)))
    asyncio.run_coroutine_threadsafe(setException(), globals()['loop'])

def f(loop):
    registerGearsThread()
    asyncio.set_event_loop(loop)
    loop.run_forever()

t = Thread(target=f, args=(loop,))
t.start()

def runCoroutine(cr, f=None, delay=0, s=None):
    if s is None:
        s = getGearsSession()
    async def runInternal():
        try:
            with GearsSession(s):
                if delay:
                    await asyncio.sleep(delay)
                res = await cr
        except Exception as e:
            try:
                if f is not None:
                    f.continueFailed(e)
            except Exception as e1:
                log(str(e1))
            return
        if f is not None:
            f.continueRun(res)
    asyncio.run_coroutine_threadsafe(runInternal(), globals()['loop'])
    
def profilerCreate():
    return cProfile.Profile()

def profileStart(p):
    p.enable()
    
def profileStop(p):
    p.disable()
    
def profileGetInfo(p, order):
    s = io.StringIO()
    ps = pstats.Stats(p, stream=s)
    ps.sort_stats(order)
    ps.print_stats()
    ps.print_callers()
    ps.print_callees()
    return s.getvalue()
    

genDeprecated('Log', 'log', log)
genDeprecated('ConfigGet', 'configGet', configGet)
genDeprecated('GearsConfigGet', 'gearsConfigGet', gearsConfigGet)
