import time
import asyncio


async def wait(n):
    time.sleep(n)


async def wait_sleep():
    yield
    await wait(2)



from queue import deque

q = deque()
q.append("a")
q.appendleft("b")
print(q.popleft())
print(q.pop())
print(q.pop())