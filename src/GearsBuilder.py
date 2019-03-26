import redisgears
import redisgears as rg
from redisgears import executeCommand as execute
from redisgears import registerTimeEvent as registerTE
from redisgears import gearsCtx
from redisgears import PyFlatExecution


globals()['str'] = str

redisgears._saveGlobals()


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
    def __init__(self, reader='KeysReader', defaultArg='*', keysOnly=False):
        self.keysOnly = keysOnly
        self.reader = reader if not keysOnly else 'PythonReader'
        self.gearsCtx = gearsCtx(self.reader)
        self.defaultArg = defaultArg

    def __localAggregateby__(self, extractor, zero, aggregator):
        self.gearsCtx.localgroupby(lambda x: extractor(x), lambda k, a, r: aggregator(k, a if a else zero, r))
        return self

    def aggregate(self, zero, seqOp, combOp):
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else zero, r))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: combOp(a if a else zero, r))
        return self

    def aggregateby(self, extractor, zero, seqOp, combOp):
        self.__localAggregateby__(extractor, zero, seqOp)
        self.gearsCtx.groupby(lambda r: r['key'], lambda k, a, r: combOp(k, a if a else zero, r['value']))
        return self

    def count(self):
        self.gearsCtx.accumulate(lambda a, r: 1 + (a if a else 0))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: r + (a if a else 0))
        return self

    def countby(self, extractor=lambda x: x):
        self.aggregateby(extractor, 0, lambda k, a, r: 1 + a, lambda k, a, r: r + a)
        return self

    def sort(self, reverse=True):
        self.aggregate([], lambda a, r: a + [r], lambda a, r: a + r)
        self.map(lambda r: sorted(r, reverse=reverse))
        self.flatmap(lambda r: r)
        return self

    def distinct(self):
        return self.aggregate(set(), lambda a, r: a | set([r]), lambda a, r: a | r).flatmap(lambda x: list(x))

    def avg(self, extractor=lambda x: float(x)):
        # we aggregate using a tupple, the first entry is the sum of all the elements,
        # the second element is the amount of elements.
        # After the aggregate phase we just devide the sum in the amount of elements and get the avg.
        return self.map(extractor).aggregate((0, 0),
                                             lambda a, r: (a[0] + r, a[1] + 1),
                                             lambda a, r: (a[0] + r[0], a[1] + r[1])).map(lambda x: x[0] / x[1])

    def run(self, arg=None, converteToStr=True, collect=True):
        if(converteToStr):
            self.gearsCtx.map(lambda x: str(x))
        if(collect):
            self.gearsCtx.collect()
        arg = arg if arg else self.defaultArg
        if(self.keysOnly):
            arg = CreatePythonReaderCallback(arg)
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
    GearsBuilder.__dict__[k] = createDecorator(PyFlatExecution.__dict__[k])

ExecutionBuilder = GearsBuilder
GB = GearsBuilder
EB = GearsBuilder


def ShardsGearsReader():
    return GB('PythonReader', ShardReaderCallback)


ShardsGB = ShardsGearsReader
