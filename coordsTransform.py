# -*- coding: utf8 -*-

import csv
import codecs
import coordTransform_utils
import sys,os
from decimal import *
getcontext().rounding = ROUND_HALF_UP

reload(sys)
sys.setdefaultencoding('utf-8')

res = coordTransform_utils.bd09_to_wgs84(114.03842,22.754845)
print(str(round(res[0],6)))
print(str(round(res[1],6)))

res = coordTransform_utils.bd09_to_wgs84(114.142479,22.861718)
print(str(round(res[0],6)))
print(str(round(res[1],6)))

filename='kakou_location_all'
path=unicode('data/'+filename+'.csv',"utf-8")
# filenames = os.listdir(path)
input = csv.reader(codecs.open(path, 'r', 'gbk'))
output = csv.writer(codecs.open('data/'+filename+'_c.csv', 'w', 'gbk'))

icount = 0
for row in input:
    result = coordTransform_utils.gcj02_to_wgs84(float(row[1]), float(row[2]))
    # result = coordTransform_utils.gcj02_to_wgs84(float(row[1]), float(row[2]))
    # output.writerow([row[0],row[1],row[2],row[3],row[4],round(result[0],6), round(result[1],6)])
    output.writerow([row[0], round(result[0],6), round(result[1],6)])
    icount += 1
    print(icount)

