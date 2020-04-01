# -*- coding: utf8 -*-

import urllib.request, urllib.parse
from collections import OrderedDict
from unidecode import unidecode
import re
from bs4 import BeautifulSoup
import math
import csv, codecs
import geojson
import fiona
from fiona.crs import from_epsg
import pprint
import gdal

url = 'http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/10/query'
max_return = 1000 # 最大返回条数


def main():
    gdal.SetConfigOption("SHAPE_ENCODING", "utf-8")

    with fiona.open('data/1.geojson', 'r', encoding='utf-8') as c:
        # rec = next(c)
        # print(rec)

        schema = c.schema
        pprint.pprint(c.schema)

    i = 0
    record_num = 1
    feaColl = []

    while record_num > 0:
        query_clause = "OBJECTID_1 >= " + str(i * max_return) + " and OBJECTID_1 < " + str((i + 1) * max_return)
        geoObjs = get_geojson_by_query(query_clause)
        record_num = len(geoObjs['features'])

        feaColl = feaColl + geoObjs['features']
        i += 1

        schema2 = {
            'geometry': 'LineString',
            'properties': OrderedDict([('OBJECTID_1', 'int'),
                                       ('OBJECTID', 'int'),
                                       ('NAME', 'str'),
                                       ('CLASS', 'int'),
                                       ('FROM_NAME', 'str'),
                                       ('TO_NAME', 'str'),
                                       ('WIDTH', 'str'),
                                       ('UPDATE_', 'int'),
                                       ('MEMO_', 'str'),
                                       ('备注', 'str'),
                                       ('数据来源', 'str'),
                                       ('Shape_Leng', 'float'),
                                       ('唯一标识码', 'str'),
                                       ('更新人', 'str'),
                                       ('涉', 'int'),
                                       ('Shape_Le_1', 'float'),
                                       ('国标代码', 'str'),
                                       ('Shape_Length', 'float')])}

        schema3 = {
            'geometry': 'LineString',
            'properties': OrderedDict([('OBJECTID_1', 'int'),
                                       ('OBJECTID', 'int'),
                                       ('NAME', 'str'),
                                       ('CLASS', 'int'),
                                       ('FROM_NAME', 'str'),
                                       ('TO_NAME', 'str'),
                                       ('WIDTH', 'str'),
                                       ('UPDATE_', 'int'),
                                       ('MEMO_', 'str')])}

        with fiona.open('res/1.shp', 'w', schema=schema2, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
            # for feature in geoObjs['features']:
                d.write(geoObjs['features'][0])
                # d.write({
                #     'geometry': geoObjs['features'][0]['geometry'],
                #     'properties': OrderedDict([('OBJECTID_1', 1),
                #                                ('OBJECTID', 1),
                #                                ('NAME', 'str'),
                #                                ('CLASS', 1),
                #                                ('FROM_NAME', '中观'),
                #                                ('TO_NAME', 'str'),
                #                                ('WIDTH', 'str'),
                #                                ('UPDATE_', 2),
                #                                ('MEMO_', 'str')])
                # })
    print("over!")


#  Post参数到服务器获取geojson对象
def get_geojson_by_query(query_clause):
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'suplicmap.pnr.sz',
                  'Pragma': 'no-cache'}

    # 定义post的参数
    body_value = {'where': query_clause,
                  'outFields': '*',
                  'outSR': '2435',
                  'f': 'geojson'}

    # 对请求参数进行编码
    data = urllib.parse.urlencode(body_value).encode(encoding='UTF8')
    # 请求不同页面的数据
    req = urllib.request.Request(url=url, data=data, headers=reqheaders)
    r = urllib.request.urlopen(req)
    respData = r.read().decode('utf-8')
    geoObjs = geojson.loads(respData)

    return geoObjs


if __name__ == '__main__':
    main()
