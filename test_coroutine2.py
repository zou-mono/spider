import asyncio
import time
import aiohttp
import ssl

from bs4 import BeautifulSoup

start = time.time()
urls = ['https://movie.douban.com/top250?start={}&filter=>'.format(i) for i in range(0,226,25)]


async def fetch(url):
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        return await response.text()


async def job(url):
        content = await fetch(url)
        soup = BeautifulSoup(content, 'html.parser')
        items = soup.select(".item")
        for i in items:
            print(i.select(".title")[0].text)

loop = asyncio.get_event_loop()
tasks = [asyncio.ensure_future(job(url)) for url in urls]
loop.run_until_complete(asyncio.wait(tasks))

end = time.time()
print('Cost time:', end - start)