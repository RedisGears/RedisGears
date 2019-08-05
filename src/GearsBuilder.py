import redisgears
import redisgears as rg
from redisgears import executeCommand as execute
from redisgears import registerTimeEvent as registerTE
from redisgears import gearsCtx
from redisgears import PyFlatExecution

from RestrictedPython import compile_restricted
from RestrictedPython import safe_globals
from RestrictedPython import limited_builtins
from RestrictedPython.Guards import full_write_guard

def CreatePythonReaderCallback(prefix):
    def PythonReaderCallback():
        pref = prefix
        cursor = '0'
        res = execute('scan', cursor, 'COUNT', '10000', 'MATCH', pref)
        cursor = res[0]
        keys = res[1]
        while int(cursor) != 0:
            for k in keys:
                yield k
            res = execute('scan', cursor, 'COUNT', '10000', 'MATCH', pref)
            cursor = res[0]
            keys = res[1]
        for k in keys:
                yield k
    return PythonReaderCallback


def ShardReaderCallback():
    res = execute('RG.INFOCLUSTER')
    if res == 'no cluster mode':
        yield '1'
    else:
        yield res[1]


class GearsBuilder():
    def __init__(self, reader='KeysReader', defaultArg='*', desc=None):
        self.realReader = reader
        if(reader == 'KeysOnlyReader' or reader == 'ShardsIDReader'):
            reader = 'PythonReader'
        self.reader = reader
        self.gearsCtx = gearsCtx(self.reader, desc)
        self.defaultArg = defaultArg

    def localAggregateby(self, extractor, zero, aggregator):
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
        self.localAggregateby(extractor, zero, seqOp)
        self.gearsCtx.groupby(lambda r: r['key'], lambda k, a, r: combOp(k, a if a else zero, r['value']))
        return self

    def count(self):
        '''
        Count the number of recors in the execution
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

    def run(self, arg=None, convertToStr=True, collect=True):
        '''
        Starting the execution
        '''
        if(convertToStr):
            self.gearsCtx.map(lambda x: str(x))
        if(collect):
            self.gearsCtx.collect()
        arg = arg if arg else self.defaultArg
        if(self.realReader == 'KeysOnlyReader'):
            arg = CreatePythonReaderCallback(arg)
        if(self.realReader == 'ShardsIDReader'):
            arg = ShardReaderCallback
        self.gearsCtx.run(arg)


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

def RunGearsRemoteBuilder(pipe, globalsDict):
    gb = GB(pipe.reader, pipe.defaultArg)
    for s in pipe.steps:
        s.AddToGB(gb, globalsDict)
        
def SetItem(x, i, v):
    x[i] = v
    
def GetItem(x, i):
    return x[i]

def write(ob):
    return ob

def metaclassCallback(name, bases, dict):
    ob = type(name, bases, dict)
    return ob

def getiter(obj):
    return iter(obj)

ops = {}
ops['+='] = lambda var1, var2: var1 + var2
ops['-='] = lambda var1, var2: var1 - var2
ops['*='] = lambda var1, var2: var1 * var2
ops['/='] = lambda var1, var2: var1 / var2
ops['|='] = lambda var1, var2: var1 | var2
ops['&='] = lambda var1, var2: var1 & var2

def inplacevar(op, var1, var2):
    return ops[op](var1, var2)
    
safe_globals['__builtins__']['GB'] = GB
safe_globals['__builtins__']['GearsBuilder'] = GB
safe_globals['__builtins__']['gearsCtx'] = gearsCtx
safe_globals['__builtins__']['RunGearsRemoteBuilder'] = RunGearsRemoteBuilder
safe_globals['__builtins__']['CreatePythonReaderCallback'] = CreatePythonReaderCallback
safe_globals['__builtins__']['execute'] = execute
safe_globals['__builtins__']['__metaclass__'] = metaclassCallback
safe_globals['__builtins__']['__name__'] = 'GearsExecution'
safe_globals['__builtins__']['_write_'] = write
safe_globals['__builtins__']['_getitem_'] = GetItem
safe_globals['__builtins__']['_setitem_'] = SetItem
safe_globals['__builtins__']['redisgears'] = redisgears
safe_globals['__builtins__']['_getiter_'] = getiter
safe_globals['__builtins__']['ShardReaderCallback'] = ShardReaderCallback
safe_globals['__builtins__']['registerTE'] = registerTE
safe_globals['__builtins__']['globals'] = globals
safe_globals['__builtins__']['_inplacevar_'] = inplacevar


def RunRestricted(code):
    d = safe_globals.copy()
    byte_code = compile_restricted(code, '<inline>', 'exec')
    exec(byte_code, d, d)

redisgears._saveGlobals()