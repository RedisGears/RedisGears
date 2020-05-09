import redisgears
import redisgears as rg
from redisgears import executeCommand as execute
from redisgears import atomicCtx as atomic
from redisgears import getMyHashTag as hashtag
from redisgears import registerTimeEvent as registerTE
from redisgears import gearsCtx
from redisgears import log
from redisgears import config_get as configGet
from redisgears import PyFlatExecution


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
            if execute('exists', pattern) == 1:
                yield pattern
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
        self.gearsCtx.localgroupby(lambda x: extractor(x), lambda k, a, r: aggregator(k, a if a else zero, r))
        return self

    def aggregate(self, zero, seqOp, combOp):
        '''
        perform aggregation on all the execution data.
        zero - the first value that will pass to the aggregation function
        seqOp - the local aggregate function (will be performed on each shard)
        combOp - the global aggregate function (will be performed on the results of seqOp from each shard)
        '''
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else zero, r))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: combOp(a if a else zero, r))
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
        self.gearsCtx.groupby(lambda r: r['key'], lambda k, a, r: combOp(k, a if a else zero, r['value']))
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
        self.gearsCtx.run(arg, **kargs)

    def register(self, prefix='*', convertToStr=True, collect=True, **kargs):
        if(convertToStr):
            self.gearsCtx.map(lambda x: str(x))
        if(collect):
            self.gearsCtx.collect()
        kargs['prefix'] = prefix # this is for backword comptability
        if 'regex' in kargs:
            log('Using regex argument with register is depricated and missleading, use prefix instead.', level='warning')
            kargs['prefix'] = kargs['regex']
        self.gearsCtx.register(**kargs)

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

genDeprecated('Log', 'log', log)
genDeprecated('ConfigGet', 'configGet', configGet)
genDeprecated('GearsConfigGet', 'gearsConfigGet', gearsConfigGet)
