import redisgears
from redisgears import gearsCtx
from redisgears import PyFlatExecution


globals()['str'] = str

redisgears._saveGlobals()


def __aggregateFunction__(key, a, r, agg, zero):
    a[key] = agg(key, a[key] if key in a.keys() else zero, r)
    return a


class GearsBuilder():
    def __init__(self, reader='KeysReader', defautlPrefix='*'):
        self.gearsCtx = gearsCtx(reader)
        self.defautlPrefix = defautlPrefix

    def __localAggregate__(self, zero, seqOp):
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else zero, r))
        return self

    def __localAggregateby__(self, extractor, zero, aggregator):
        self.__localAggregate__({}, lambda a, r: __aggregateFunction__(extractor(r), a, r, aggregator, zero))
        self.gearsCtx.flatmap(lambda r: [(k, r[k]) for k in r.keys()])
        return self

    def aggregate(self, zero, seqOp, combOp):
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else zero, r))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: combOp(a if a else zero, r))
        return self

    def aggregateby(self, extractor, zero, seqOp, combOp):
        self.__localAggregateby__(extractor, zero, seqOp)
        self.gearsCtx.groupby(lambda r: r[0], lambda k, a, r: combOp(k, a if a else zero, r[1]))
        return self

    def count(self):
        self.gearsCtx.accumulate(lambda a, r: 1 + (a if a else 0))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: r + (a if a else 0))
        return self

    def countby(self, extractor):
        self.aggregateby(extractor, 0, lambda k, a, r: 1 + a, lambda k, a, r: r + a)
        return self

    def sort(self, reverse=True):
        self.aggregate([], lambda a, r: a + [r], lambda a, r: a + r)
        self.map(lambda r: sorted(r, reverse=reverse))
        self.flatmap(lambda r: r)
        return self

    def distinct(self):
        return self.aggregate(set(), lambda a, r: a | set([r]), lambda a, r: a | r).flatmap(lambda x: list(x))

    def run(self, prefix=None, converteToStr=True):
        if(converteToStr):
            self.gearsCtx.map(lambda x: str(x))
        self.gearsCtx.run(prefix if prefix else self.defautlPrefix)


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
