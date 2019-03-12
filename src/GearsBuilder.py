import redisgears
from redisgears import gearsCtx
from redisgears import PyFlatExecution

globals()['str'] = str

redisgears._saveGlobals()


class GearsBuilder():
    def __init__(self, reader='KeysReader'):
        self.gearsCtx = gearsCtx(reader)

    def aggregate(self, zero, seqOp, combOp):
        self.gearsCtx.accumulate(lambda a, r: seqOp(a if a else zero, r))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: combOp(a if a else zero, r))
        return self

    def count(self):
        self.gearsCtx.accumulate(lambda a, r: 1 + (a if a else 0))
        self.gearsCtx.collect()
        self.gearsCtx.accumulate(lambda a, r: r + (a if a else 0))
        return self

    def distinct(self):
        return self.aggregate(set(), lambda a, r: a | set([r]), lambda a, r: a | r).flatmap(lambda x: list(x))

    def run(self, converteToStr=True):
        if(converteToStr):
            self.gearsCtx.map(lambda x: str(x))
        self.gearsCtx.run()


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
