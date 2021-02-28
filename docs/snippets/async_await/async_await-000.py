import asyncio

async def coro(r):
	await asyncio.sleep(5)

GB('ShardsIDReader').foreach(coro).run()