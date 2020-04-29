# -*- coding: utf-8 -*-

from multiprocessing import Pool, cpu_count
from gevent import monkey
from gevent.pool import Pool as gpool
from gevent.queue import Queue
import json
import time, datetime
from log4p import Log
import urllib.request, urllib.parse
import os
from osgeo import ogr
import osgeo.osr as osr
import click
import traceback

try_num = 10
num_return = 1000 # 返回条数
# max_return = 1000000
log = Log(__file__)
# epsg = 2435
OID_NAME = "OBJECTID"  # FID字段名称

@click.command()
@click.option('--url', '-u',
              help='Input url. For example, http://suplicmap.pnr.sz/dynaszmap_3/rest/services/SZMAP_DLJT_GKDL/MapServer/10/',
              required=True)
@click.option(
    '--layer-name', '-n',
    help='Output layer name, which is shown in geodatabase. For example, 道路面',
    required=True)
# @click.option(
#     '--sr', '-s',
#     help='srs EPSG ID. For example, 2435',
#     type=int,
#     default=2435,
#     required=False)
@click.option(
    '--loop-pos', '-l',
    help='Start loop position. For example, 0',
    type=int,
    default=0,
    required=False)
@click.option(
    '--output-path', '-o',
    help='Output file geodatabase, need the full path. For example, res/data.gdb',
    required=True)
def main(url, layer_name, sr, loop_pos, output_path):
    """crawler program for vector data in http://suplicmap.pnr.sz."""
    start = time.time()

    # epsg = sr
    if url[-1] == r"/":
        query_url = url + "query"
        url_json = url[:-1] + "?f=pjson"
    else:
        query_url = url + "/query"
        url_json = url + "?f=pjson"

    log.info("开始创建文件数据库...")

    outdriver=ogr.GetDriverByName('FileGDB')
    if os.path.exists(output_path):
        gdb = outdriver.Open(output_path, 1)
        log.info("文件数据库已存在，在已有数据库基础上创建图层.")
    else:
        gdb = outdriver.CreateDataSource(output_path)

    # 向服务器发送一条请求，获取数据字段信息
    respData = get_json(url_json)
    if respData is None:
        log.error('获取数据字段信息失败,无法创建数据库.')
        return

    geoObjs = json.loads(respData)
    epsg = geoObjs['sourceSpatialReference']['wkid']
    dateLst = parseDateField(geoObjs)  # 获取日期字段
    OID = parseOIDField(geoObjs)
    if OID is None:
        log.error('获取OID字段信息失败,无法创建数据库.')
        return

    GeoType = parseGeoTypeField(geoObjs)
    if GeoType is None:
        log.error('获取Geometry字段信息失败,无法创建数据库.')
        return

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)

    # out_layer = gdb.CreateLayer(layer_name, srs=srs, geom_type=temp_layer.GetGeomType(),options=["LAYER_ALIAS=电动"])
    out_layer = gdb.CreateLayer(layer_name, srs=srs, geom_type=GeoType)
    # LayerDefn = out_layer.GetLayerDefn()
    fields = geoObjs['fields']

    i = 0
    for field in fields:
        # fieldDefn = out_layerDefn.GetFieldDefn(i)
        if field['type'] == "esriFieldTypeOID":
            OID_NAME = field['name']
            continue
        if field['type'] == "esriFieldTypeGeometry":
            continue

        # if field['name'] == "OBJECTID":  # 和默认OBJECTID重名，会加下划线
        #     iOBJECTID_NUM = i

        OFTtype = parseTypeField(field['type'])
        new_field = ogr.FieldDefn(field['name'], OFTtype)

        if OFTtype == ogr.OFTString:
            new_field.SetWidth(field['length'])
        elif OFTtype == ogr.OFTReal:
            new_field.SetWidth(18)
            new_field.SetPrecision(10)

        # LayerDefn.AddFieldDefn(new_field)
        out_layer.CreateField(new_field, True)  # true表示会根据字段长度限制进行截短
        i += 1

    defn = out_layer.GetLayerDefn()
    for i in range(defn.GetFieldCount()):
        fieldName = defn.GetFieldDefn(i).GetName()
        fieldTypeCode = defn.GetFieldDefn(i).GetType()
        fieldType = defn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
        fieldWidth = defn.GetFieldDefn(i).GetWidth()
        GetPrecision = defn.GetFieldDefn(i).GetPrecision()

        log.debug(fieldName + " - " + fieldType + " " + str(fieldWidth) + " " + str(GetPrecision))

    log.info("文件数据库创建成功.")
    log.info("保存位置为" + os.path.abspath(output_path) + ", 图层名称为" + out_layer.GetName())

    record_num = 1
    iRow = 0
    log.info('开始抓取数据...')
    while record_num > 0:

        trytime = 0
        while True:
            query_clause = OID_NAME + " > " + str(loop_pos * num_return) + " and " + OID_NAME + " <= " + str((loop_pos + 1) * num_return)
            respData = get_json_by_query(query_url, query_clause, epsg)
            esri_json = ogr.GetDriverByName('ESRIJSON')
            geoObjs = esri_json.Open(respData, 0)
            if geoObjs is not None:
                json_Layer = geoObjs.GetLayer()
                record_num = json_Layer.GetFeatureCount()

                defn = json_Layer.GetLayerDefn()
                for feature in json_Layer:  # 将json要素拷贝到gdb中
                    try:
                        FID = feature.GetField(OID_NAME)
                        ofeature = ogr.Feature(out_layer.GetLayerDefn())
                        ofeature.SetGeometry(feature.GetGeometryRef())
                        ofeature.SetFID(FID)
                        if feature.GetField("OBJECTID") is not None:
                            ofeature.SetField('OBJECTID_', feature.GetField("OBJECTID"))

                        for i in range(defn.GetFieldCount()):
                            fieldName = defn.GetFieldDefn(i).GetName()
                            if fieldName == "OBJECTID" or fieldName == OID_NAME:
                                continue

                            ofeature.SetField(fieldName, feature.GetField(i))

                        for dateField in dateLst:

                                timeArray = time.localtime(int(feature.GetField(dateField))/1000)  # 1970秒数
                                otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
                                ofeature.SetField(dateField, otherStyleTime)


                        out_layer.CreateFeature(ofeature)
                        ofeature.Destroy()
                    except:
                        log.error("错误发生在FID=" + str(FID) + "\n" + traceback.format_exc())
                        continue

                    iRow += 1
                    log.debug(iRow)
                break
            else:
                trytime += 1
                if trytime > try_num:
                    log.error('数据抓取失败. error in ' + query_clause)
                    break
        loop_pos += 1

    outdriver = None
    del gdb
    out_layer = None
    end = time.time()
    log.info('完成抓取.耗时：' + str(end - start))


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
            res = json.loads(respData)
            if 'error' not in res.keys():
                return respData
        except:
            log.error('HTTP请求失败！正在准备重发...')
            trytime += 1

        time.sleep(2)
        continue


#  Post参数到服务器获取geojson对象
def get_json_by_query(url, query_clause, epsg):
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'suplicmap.pnr.sz',
                  'Pragma': 'no-cache'}

    # 定义post的参数
    body_value = {'where': query_clause,
                  'outFields': '*',
                  'outSR': str(epsg),
                  'f': 'json'}

    # 对请求参数进行编码
    data = urllib.parse.urlencode(body_value).encode(encoding='UTF8')
    # 请求不同页面的数据
    trytime = 0
    while trytime < try_num:
        try:
            req = urllib.request.Request(url=url, data=data, headers=reqheaders)
            r = urllib.request.urlopen(req)
            respData = r.read().decode('utf-8')
            # geoObj = json.loads(respData)
            return respData
        except:
            log.error('HTTP请求失败！正在准备重发...')
            trytime += 1

        time.sleep(2)
        continue
    return None


def parseDateField(fields):
    fields = fields['fields']
    order = 0
    DateFields = []
    for field in fields:
        if field['type'] == "esriFieldTypeDate":
            DateFields.append(field['name'])
        order += 1
    return DateFields

def parseOIDField(fields):
    fields = fields['fields']
    order = 0
    for field in fields:
        if field['type'] == "esriFieldTypeOID":
            return [order, field['name']]
        order += 1
    return None

def parseGeoTypeField(fields):
    GeoType = fields['geometryType']

    if GeoType == "esriGeometryPoint":
        return ogr.wkbPoint
    elif GeoType == "esriGeometryLine":
        return ogr.wkbLineString
    elif GeoType == "esriGeometryPolyline":
        return ogr.wkbMultiLineString
    elif GeoType == "esriGeometryPolygon" or "esriGeometryMultiPatch":
        return ogr.wkbMultiPolygon
    else:
        return None


def parseTypeField(FieldType):
    if FieldType == "esriFieldTypeSmallInteger":
        return ogr.OFTInteger
    elif FieldType == "esriFieldTypeInteger":
        return ogr.OFTInteger
    elif FieldType == "esriFieldTypeSingle":
        return ogr.OFTReal
    elif FieldType == "esriFieldTypeDouble":
        return ogr.OFTReal
    elif FieldType == "esriFieldTypeGUID" or FieldType == "esriFieldTypeGlobalID" or \
            FieldType == "esriFieldTypeXML" or FieldType == "esriFieldTypeString":
        return ogr.OFTString
    elif FieldType == "esriFieldTypeDate":
        return ogr.OFTDateTime
    elif FieldType == "esriFieldTypeBlob":
        return ogr.OFTBinary
    else:
        return None

if __name__ == '__main__':
    main()
