import asyncio


async def execute(x):
    print('Number:', x)

coroutine = execute(1)
print('Coroutine:', coroutine)
print('After calling execute')

loop = asyncio.get_event_loop()
loop.run_until_complete(coroutine)
print('After calling loop')


@asyncio.coroutine
def hello():
    print("Hello world!")
    # 异步调用asyncio.sleep(1):
    r = yield from asyncio.sleep(1)
    print("Hello again!")

# 获取EventLoop:
loop = asyncio.get_event_loop()
# 执行coroutine
loop.run_until_complete(hello())
loop.close()