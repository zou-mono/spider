# -*- coding: utf8 -*-

from multiprocessing import Pool, cpu_count
from gevent import monkey
from gevent.pool import Pool as gpool
from gevent.queue import Queue
import json
from log4p import Log
import urllib.request, urllib.parse
import os
from osgeo import gdal
from osgeo import ogr
import osgeo.osr as osr
from fiona.crs import from_epsg
import click

# url = 'http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/10/query'  # 道路边线
# output_path = 'res/道路边线.geojson'

# url = 'http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/11/query'  # 道路面
# output_path = 'res/道路面.geojson'

num_return = 1000 # 返回条数
# max_return = 10000
log = Log()

@click.command()
@click.option('--url', '-u',
              help='Input url. For example, http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/10/query',
              required=True)
@click.option(
    '--layer-name', '-n',
    help='Output layer name, which is shown in geodatabase. For example, 道路面',
    required=True)
@click.option(
    '--output-path', '-o',
    help='Output file geodatabase, need the full path. For example, res/data.gdb',
    required=True)
def main(url, layer_name, output_path):

    outdriver=ogr.GetDriverByName('FileGDB')
    if os.path.exists(output_path):
        gdb = outdriver.Open(output_path, 1)
    else:
        gdb = outdriver.CreateDataSource(output_path)

    query_clause = "OBJECTID_1 = 0"
    geoObjs = get_json_by_query(url, query_clause)
    temp_layer = geoObjs.GetLayer()
    out_layerDefn = temp_layer.GetLayerDefn()

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2435)
    out_layer = gdb.CreateLayer(layer_name, srs=srs, geom_type=temp_layer.GetGeomType())

    strField = ogr.FieldDefn("中文", ogr.OFTInteger)
    out_layer.CreateField(strField, False)  # true表示会根据字段长度限制进行截短

    # for i in range(out_layerDefn.GetFieldCount()):
    #     fieldDefn = out_layerDefn.GetFieldDefn(i)
    #     fieldName = fieldDefn.GetName()
    #     strField = ogr.FieldDefn("中文", ogr.OFTInteger)
    #     out_layer.CreateField(fieldDefn, True)  # true表示会根据字段长度限制进行截短

    i = 0
    record_num = 1
    feaColl = []

    log.info('开始抓取数据...')
    while record_num > 0:

        trytime = 0
        while True:
            query_clause = "OBJECTID_1 > " + str(i * num_return) + " and OBJECTID_1 <= " + str((i + 1) * num_return)
            geoObjs = get_json_by_query(url, query_clause)
            if geoObjs is not None:
                record_num = geoObjs.getFeatureCount()
                break
            else:
                trytime += 1
                if trytime > 5:
                    log.error('error in ' + query_clause)
                    break

        i += 1
        log.debug(i)
    log.info('完成抓取.')

    res = {
        'type': geoObjs['type'],
        'crs': geoObjs['crs'],
        'features': feaColl
    }

    log.info('开始输出到文件...')
    with open(output_path, 'w') as output:
        geojson.dump(res, output)
        # with fiona.open('res/1.shp', 'w', schema=schema2, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
        #         d.write(geoObjs['features'][0])
    log.info("over!")


#  Post参数到服务器获取geojson对象
def get_json_by_query(url, query_clause):
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'suplicmap.pnr.sz',
                  'Pragma': 'no-cache'}

    # 定义post的参数
    body_value = {'where': query_clause,
                  'outFields': '*',
                  'outSR': '2435',
                  'f': 'json'}

    # 对请求参数进行编码
    data = urllib.parse.urlencode(body_value).encode(encoding='UTF8')
    # 请求不同页面的数据
    req = urllib.request.Request(url=url, data=data, headers=reqheaders)
    r = urllib.request.urlopen(req)
    respData = r.read().decode('utf-8')

    # return respData
    # geoObjs = geojson.loads(respData)
    esri_json = ogr.GetDriverByName('ESRIJSON')
    geoObjs = esri_json.Open(respData, 0)

    # layer = geoObjs.GetLayer()
    # for feature in layer:
    #     geom = feature.GetGeometryRef()
    #     print(geom.Centroid().ExportToWkt())

    return geoObjs


if __name__ == '__main__':
    main()
