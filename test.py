import os
from osgeo import ogr, gdal
import osgeo.osr as osr
import fiona
from collections import OrderedDict
from fiona.crs import from_epsg
import re
import asyncio
# from log_test import WrappedLogger
# import logging
from log4p import Log
import math
import PIL.Image as Image
import colorlog  # 控制台日志输入颜色


log = Log(__file__)
minX = 80574.81260346594
minY = 1012.1329692534127
maxX = 176816.21260346594
maxY = 56528.33296925342
originX = -5123300
originY = 10002300
resolution = 5.291677250021167
scale = 20000
tilesize = 256

# min_row = 73418
# max_row = 73828
# min_col = 38414
# max_col = 39124
# min_row = 1468
# max_row = 1476
# min_col = 768
# max_col = 782
min_row = 7341
max_row = 7382
min_col = 3841
max_col = 3912

gcp_x0 = math.floor(((minX - originX) - min_col * (resolution * tilesize)) / resolution)
gcp_y0 = math.floor(((originY - maxY) - min_row * (resolution * tilesize)) / resolution)
gcp_x1 = math.floor(((maxX - originX) - max_col * (resolution * tilesize)) / resolution)
gcp_y1 = math.floor(((originY - minY) - max_row * (resolution * tilesize)) / resolution)


tilewidth = max_col - min_col
tileheight = max_row - min_row

# dr = gdal.GetDriverByName("GTiff")
# out_ds = dr.Create("output.tif", tilewidth * tilesize, tileheight * tilesize, 3, gdal.GDT_Byte, options=["INTERLEAVE=PIXEL"])
# ds = gdal.Open('E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/3/1469_771.png')
# redBand = ds.GetRasterBand(1)
# greenBand = ds.GetRasterBand(2)
# blueBand = ds.GetRasterBand(3)
# r = redBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, redBand.DataType)
# g = greenBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, greenBand.DataType)
# b = blueBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, blueBand.DataType)
#
# out_r = out_ds.GetRasterBand(1)
# out_g = out_ds.GetRasterBand(2)
# out_b = out_ds.GetRasterBand(3)
# out_r.WriteRaster((1469 - min_row) * tilesize, (771 - min_col) * tilesize, tilesize, tilesize, r, tilesize, tilesize)
# out_g.WriteRaster((1469 - min_row) * tilesize, (771 - min_col) * tilesize, tilesize, tilesize, g, tilesize, tilesize)
# out_b.WriteRaster((1469 - min_row) * tilesize, (771 - min_col) * tilesize, tilesize, tilesize, b, tilesize, tilesize)
#
# out_ds = None
# ds = None
# dr = None
#
# print("over")

merge_file = "output5.tif"
if os.path.exists(merge_file):
    os.remove(merge_file)

dr = gdal.GetDriverByName("GTiff")
out_ds = dr.Create(merge_file, tilewidth * tilesize, tileheight * tilesize, 3, gdal.GDT_Int16, options=["BIGTIFF=YES", "COMPRESS=LZW", "TILED=YES", "INTERLEAVE=PIXEL"])

out_r = out_ds.GetRasterBand(1)
out_g = out_ds.GetRasterBand(2)
out_b = out_ds.GetRasterBand(3)

icount = 0
for root, subDir, files in os.walk("E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/5"):  # e:/8_res E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/5
    for filename in files:
        ds = gdal.Open(os.path.join(root, filename))
        redBand = ds.GetRasterBand(1)
        greenBand = ds.GetRasterBand(2)
        blueBand = ds.GetRasterBand(3)
        r = redBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)
        g = greenBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)
        b = blueBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)
        # r = redBand.ReadAsArray().tostring()
        # g = greenBand.ReadAsArray().tostring()
        # b = blueBand.ReadAsArray().tostring()

        name = os.path.splitext(filename)[0]
        y = int(name.split('_')[0])
        x = int(name.split('_')[1])
        out_r.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, r, tilesize, tilesize)
        out_g.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, g, tilesize, tilesize)
        out_b.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, b, tilesize, tilesize)

        # out_r.FlushCache()
        # out_g.FlushCache()
        # out_b.FlushCache()

        icount += 1
        print(icount)


print("OK")
out_ds = None
dr = None
# min_row = 7341
# max_row = 7382
# min_col = 3841
# max_col = 3912

# tilewidth = max_col - min_col
# tileheight = max_row - min_row
#
# ditu_bigimg = Image.new('RGBA', (tilesize * tilewidth, tilesize * tileheight))
#
# for root, subDir, files in os.walk("E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/3"):  # E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/3
#     for filename in files:
#         name = os.path.splitext(filename)[0]
#         y = int(name.split('_')[0]) - min_row
#         x = int(name.split('_')[1]) - min_col
#
#         ditu_bigimg.paste(Image.open(os.path.join(root,filename)), (x * tilesize, y * tilesize))
#
# ditu_bigimg.save('res/level5.png')
# print('over')

# for i in range(min_row, max_row):
#     for j in range(min_col, max_col):
# TRACE = 5
# logging.addLevelName(TRACE, 'TRACE')
# formatter = colorlog.ColoredFormatter(
#     '%(log_color)s[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s',
#     log_colors=log_colors_config)  # 日志输出格式
# handler = colorlog.StreamHandler()
# handler.setFormatter(formatter)
# logger = logging.getLogger()
# logger.addHandler(handler)
# logger.setLevel(logging.DEBUG)
# logger.info('a message using a custom level')
#
# # logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(filename)s(%(lineno)d): "
# #                                                 "%(message)s")
#
# # logging.setLoggerClass(WrappedLogger)
# # logger = logging.getLogger('MyLogger')  # init MyLogger
# # logging.setLoggerClass(logging.Logger) # reset logger class to default
# my_logger = logging.getLogger('my_log')
# my_logger.addHandler(ch)

# logger = log.getLogger('MyLogger')
log.info('info')
log.debug('debug')

# logger = logging.getLogger('test')
# handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter('%(funcName)s: %(message)s'))
# handler.setLevel(11)
# logger.addHandler(handler)
# logger.debug('Will not print')


# #open connections
# indriver = ogr.GetDriverByName('ESRI Shapefile')
# dataSource = indriver.Open('data/zone.shp', 0)
#
# outdriver=ogr.GetDriverByName('FileGDB')
# # gdb = outdriver.Open("res/out.gdb", 1)
# out_path = "res/out.gdb"
# if os.path.exists(out_path):
#     gdb = outdriver.Open("res/out.gdb", 1)
# else:
#     gdb = outdriver.CreateDataSource("res/out.gdb")
#
# # test_out = outdriver.Open('D:/test/New folder/region.gdb', 0)
#
# #get input and output layers
# inp_main=dataSource.GetLayer()
# srs = osr.SpatialReference()
# srs.ImportFromEPSG(2435)
# out_main = gdb.CreateLayer('fff', srs=srs, geom_type=ogr.wkbPolygon)
# # out_main=gdb.GetLayer('fff')
#
# gdb.StartTransaction(force=True)
# #cycle through the features in the input layer
# for feature in inp_main:
#     outfeature = ogr.Feature(out_main.GetLayerDefn())
#     outfeature.SetGeometry(feature.GetGeometryRef())
#     # outfeature.SetField('ID',feature.GetField('id'))
#     out_main.CreateFeature(outfeature)
#     outfeature.Destroy()
#
# gdb.CommitTransaction()

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



print('over!')
