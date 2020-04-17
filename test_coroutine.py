import asyncio
import requests
import time
import aiohttp

# async def execute(x):
#     print('Number:', x)
#
# coroutine = execute(1)
# print('Coroutine:', coroutine)
# print('After calling execute')
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(coroutine)
# print('After calling loop')
#
#
# @asyncio.coroutine
# def hello():
#     print("Hello world!")
#     # 异步调用asyncio.sleep(1):
#     r = yield from asyncio.sleep(1)
#     print("Hello again!")
#
# # 获取EventLoop:
# loop = asyncio.get_event_loop()
# # 执行coroutine
# loop.run_until_complete(hello())
# loop.close()


start = time.time()

async def get(url):
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        return await response.text()
        # session.close()
        # return result

async def request():
    url = 'http://127.0.0.1:5000'
    print('Waiting for', url)
    result = await get(url)
    print('Get response from', url, 'Result:', result)

tasks = [asyncio.ensure_future(request()) for _ in range(5)]
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))

end = time.time()
print('Cost time:', end - start)
