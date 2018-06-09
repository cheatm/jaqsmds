import time
import asyncio


async def wait(n):
    time.sleep(n)


async def wait_sleep():
    yield
    await wait(2)


async def higher_wait():
    yield
    await 

loop = asyncio.get_event_loop()
loop.run_until_complete(wait_sleep())