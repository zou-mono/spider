import time
import aiohttp
import asyncio
from log4p import Log
import urllib.request, urllib.parse
import json
import os
import math
import click
import traceback
from asyncRequest import send_http

try_num = 10
log = Log(__file__)
failed_urls = []
lock = asyncio.Lock()

@click.command()
@click.option('--url', '-u',
              help='Input url. For example, http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/10/',
              required=True)
@click.option(
    '--file-name', '-f',
    help='If need to merge tiles, should offer output name of merged image. For example, 2019_image_data. If no need to merge tiles, omit.',
    required=False)
@click.option(
    '--sr', '-s',
    help='srs EPSG ID. For example, 2435',
    type=int,
    default=2435,
    required=False)
@click.option(
    '--level', '-l',
    help='tile level. For example, 8',
    type=int,
    default=0,
    required=True)
@click.option(
    '--output-path', '-o',
    help='Output folder, need the full path. For example, res/tilemaps',
    required=True)
def main(url, file_name, sr, level, output_path):
    """crawler program for tilemap data in http://suplicmap.pnr.sz."""
    start = time.time()

    if url[-1] == r"/":
        url = url[:-1]

    url_json = url + "?f=pjson"

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    level_path = output_path + "/" + str(level)
    if not os.path.exists(level_path):
        os.makedirs(level_path)

    getInfo = get_json(url_json)
    # print(getInfo)

    tile_size = getInfo['tileInfo']['rows']  # 瓦片尺寸
    x0 = getInfo['tileInfo']['origin']['x']  # 初始x
    y0 = getInfo['tileInfo']['origin']['y']  # 初始y
    xmin = getInfo['extent']['xmin']  # xmin
    ymin = getInfo['extent']['ymin']  # ymin
    xmax = getInfo['extent']['xmax']  # xmax
    ymax = getInfo['extent']['ymax']  # ymax

    lods = getInfo['tileInfo']['lods']  # lod信息
    lod = get_lod(lods, level)
    resolution = lod['resolution']
    min_col, min_row = get_col_row(x0, y0, xmin, ymax, tile_size, resolution)
    print(str(min_col) + " " + str(min_row))
    max_col, max_row = get_col_row(x0, y0, xmax, ymin, tile_size, resolution)
    print(str(max_col) + " " + str(max_row))

    log.info('开始使用协程抓取...')
    # for i in range(min_row, max_row):
    #     for j in range(min_col, max_col):
    #         tile_url = f'{url}/tile/{level}/{i}/{j}'
    #         output_img(tile_url, level_path, i, j)

    tasks = []
    loop = asyncio.ProactorEventLoop()
    itask = 0
    asyncio.set_event_loop(loop)
    # loop = asyncio.get_event_loop()
    iloop = 0
    for i in range(min_row, max_row + 1):
        for j in range(min_col, max_col + 1):
            # tile_url = url + "/tile/" + str(level) + "/" + str(i) + "/" + str(j)
            tile_url = f'{url}/tile/{level}/{i}/{j}'

            if itask >= 3000:
                loop.run_until_complete(asyncio.wait(tasks))
                tasks = []
                itask = 0
                iloop += 1
                log.debug(iloop)
                continue
            else:
                tasks.append(asyncio.ensure_future(output_img_asyc(tile_url, level_path, i, j)))
            itask += 1

    loop.run_until_complete(asyncio.wait(tasks))
    log.info('协程抓取完成.')

    log.info('开始用单线程抓取失败的url...')
    while len(failed_urls) > 0:
        furl = failed_urls.pop()
        if not output_img2(furl[0], level_path, furl[1], furl[2]):
            log.error('url:{} error:{}'.format(url, traceback.format_exc()))

    end = time.time()
    if lock.locked():
        lock.release()
    log.info('完成抓取.耗时：' + str(end - start))


def get_tile(url):
    trytime = 0
    while trytime < try_num:
        try:
            req = urllib.request.Request(url=url)
            r = urllib.request.urlopen(req)
            respData = r.read()
            return respData
        except:
            log.debug('{}请求失败！重新尝试...'.format(url))
            trytime += 1

        time.sleep(2)
        continue
    return None


def output_img(url, output_path, i, j):
    try:
        img = get_tile(url)
        with open(f'{output_path}/{i}_{j}.png', "wb") as f:
            f.write(img)
        return True
    except:
        failed_urls.append([url, i, j])
        log.error('url:{} error:{}'.format(url, traceback.format_exc()))
        return False


def output_img2(url, output_path, i, j):
    try:
        img = get_tile(url)
        with open(f'{output_path}/{i}_{j}.png', "wb") as f:
            f.write(img)
        return True
    except:
        return False


async def get_tile_async(url, output_path, i, j):
    async with aiohttp.ClientSession() as session:
        try:
            respData = await send_http(session, method="get", respond_Type="content", url=url, retries=0)
            # response = await session.post(url, data=data, headers=reqheaders)
            return respData, url, output_path, i, j
        except:
            log.error('url:{} error:{}'.format(url, traceback.format_exc()))


async def output_img_asyc(url, output_path, i, j):
    try:
        img, url, output_path, i, j = await get_tile_async(url, output_path, i, j)
        # log.info('任务载入完成.')
        # log.info('开始抓取...')
        with open(f'{output_path}/{i}_{j}.png', "wb") as f:
            f.write(img)
    except:
        await lock.acquire()
        failed_urls.append([url, i, j])
        lock.release()
        log.error('url:{} error:{}'.format(url, traceback.format_exc()))


def get_lod(lods, level):
    for lod in lods:
        if lod['level'] == level:
            return lod


def get_col_row(x0, y0, x, y, size, resolution):
    col = math.floor(math.fabs((x0 - x) / (size * resolution)))
    row = math.floor(math.fabs((y0 - y) / (size * resolution)))

    return col, row


def get_json(url):
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'suplicmap.pnr.sz',
                  'Pragma': 'no-cache'}
    # 请求不同页面的数据
    trytime = 0
    while trytime < try_num:
        try:
            req = urllib.request.Request(url=url, headers=reqheaders)
            r = urllib.request.urlopen(req)
            respData = r.read().decode('utf-8')
            # return respData
            res = json.loads(respData)
            if 'error' not in res.keys():
                return res
        except:
            # log.error('HTTP请求失败！重新尝试...')
            trytime += 1

        time.sleep(2)
        continue


if __name__ == '__main__':
    main()
