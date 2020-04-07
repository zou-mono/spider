import fiona
from collections import OrderedDict
from fiona.crs import from_epsg
import re

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
    rec = next(iter(c))
    schema3 = c.schema
    schema3['geometry'] = 'Polygon'

# with fiona.open('res/1.shp', 'w', schema=schema4, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
#     d.write({
#         'geometry': rec['geometry'],
#         'properties': rec['properties']})

with fiona.open('res/1.shp', 'w', schema=schema, driver='ESRI Shapefile', crs=from_epsg(2435), encoding='utf-8') as d:
    d.write({
        'geometry': rec['geometry'],
        'properties': OrderedDict([
            ('id', 0),
            ('开始', 1),
            ('结束', 2),
            ('yyyyyyxxxxbb', 0.6)])
    })

print('over!')
