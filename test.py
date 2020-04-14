import os
from osgeo import ogr
import osgeo.osr as osr
import fiona
from collections import OrderedDict
from fiona.crs import from_epsg
import re
import asyncio



#open connections
indriver = ogr.GetDriverByName('ESRI Shapefile')
dataSource = indriver.Open('data/zone.shp', 0)

outdriver=ogr.GetDriverByName('FileGDB')
# gdb = outdriver.Open("res/out.gdb", 1)
out_path = "res/out.gdb"
if os.path.exists(out_path):
    gdb = outdriver.Open("res/out.gdb", 1)
else:
    gdb = outdriver.CreateDataSource("res/out.gdb")

# test_out = outdriver.Open('D:/test/New folder/region.gdb', 0)

#get input and output layers
inp_main=dataSource.GetLayer()
srs = osr.SpatialReference()
srs.ImportFromEPSG(2435)
out_main = gdb.CreateLayer('fff', srs=srs, geom_type=ogr.wkbPolygon)
# out_main=gdb.GetLayer('fff')

gdb.StartTransaction(force=True)
#cycle through the features in the input layer
for feature in inp_main:
    outfeature = ogr.Feature(out_main.GetLayerDefn())
    outfeature.SetGeometry(feature.GetGeometryRef())
    # outfeature.SetField('ID',feature.GetField('id'))
    out_main.CreateFeature(outfeature)
    outfeature.Destroy()

gdb.CommitTransaction()

#close connection
dataSource=None
gdb=None



# with fiona.Env():
with fiona.open('D:/test/New folder/region.gdb', driver='FileGDB') as source:
    print(source.meta)

print(fiona.listlayers('D:/test/New folder/region.gdb'))

# str_test = 'abcdefg中华HABC123456民族'
str_test = '唯一标识码'
hanzi_regex = re.compile(r'[\u4E00-\u9FA5]')
hanzi_list = hanzi_regex.findall(str_test)
print('包含的汉字:',len(hanzi_list))

schema = {
    'geometry': 'Polygon',
    'properties': OrderedDict([
        ('id', 'int'),
        ('开始', 'int'),
        ('结束', 'int'),
        ('yyyyyyxxxxbb', 'float')])
}

schema2 = {
    'geometry': 'Polygon',
    'properties': OrderedDict([
        ('OBJECTID_1', 'int'),
        ('OBJECTID', 'int'),
        ('NAME', 'str'),
        ('CLASS', 'int'),
        ('FROM_NAME', 'str'),
        ('TO_NAME', 'str'),
        ('WIDTH', 'str'),
        ('UPDATE_', 'int'),
        ('MEMO_', 'str'),
        ('Shape_Leng', 'float'),
        ('备注', 'str'),
        ('数据来源', 'str'),
        ('唯一标识码', 'str'),
        ('更新人', 'str'),
        ('涉', 'int'),
        ('道路编号', 'str'),
        ('Shape_Le_1', 'float'),
        ('国标代码', 'str'),
        ('最大x坐标', 'float'),
        ('最小x坐标', 'int'),
        ('宽度', 'int'),
        ('Shape_Length', 'float'),
        ('Shape_Area', 'float')])
}

schema4 = {
    'geometry': 'Polygon',
    'properties': OrderedDict([
        ('OBJECTID_1', 'int'),
        ('OBJECTID', 'int'),
        ('NAME', 'str'),
        ('CLASS', 'int'),
        ('FROM_NAME', 'str'),
        ('TO_NAME', 'str'),
        ('WIDTH', 'str'),
        ('UPDATE_', 'int'),
        ('MEMO_', 'str'),
        ('Shape_Leng', 'float'),
        ('备注', 'str'),
        ('数据来源', 'str')])
}

with fiona.open('data/2.geojson') as c:
    rec1 = next(iter(c))
    rec2 = next(iter(c))
    schema3 = c.schema
    schema3['geometry'] = 'Polygon'

# with fiona.Env():
#     with fiona.open('data/zone.shp') as source:
#         meta = source.meta
#         meta['driver'] = 'FileGDB'
#         meta['layer'] = 'layername'
#
#         with fiona.open('res/out.gdb', 'w', **meta) as sink:
#             for f in source:
#                 sink.write(f)

# with fiona.open('res/test2.gdb', 'w', schema=schema, layer='foo', driver='FileGDB', crs=from_epsg(2435), encoding='utf-8',  editable=True) as end:
#     end.write({
#         'geometry': rec1['geometry'],
#         'properties': OrderedDict([
#             ('id', 0),
#             ('开始', 1),
#             ('结束', 2),
#             ('yyyyyyxxxxbb', 0.6)])
#     })

# with fiona.open('res/1.shp', 'w', schema=schema4, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
#     d.write({
#         'geometry': rec['geometry'],
#         'properties': rec['properties']})

# with fiona.open('res/1.shp', 'w', schema=schema, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
#     d.write({
#         'geometry': rec['geometry'],
#         'properties': OrderedDict([
#             ('id', 0),
#             ('开始', 1),
#             ('结束', 2),
#             ('yyyyyyxxxxbb', 0.6)])
#     })


    # source.write({
    #     'geometry': rec['geometry'],
    #     'properties': OrderedDict([
    #         ('id', 0),
    #         ('开始', 1),
    #         ('结束', 2),
    #         ('yyyyyyxxxxbb', 0.6)])
    # })

print('over!')
