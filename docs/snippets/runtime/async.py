import asyncio

async def c(r):
    await asyncio.sleep(1)
    return r

GB('ShardsIDReader').map(c).run()
