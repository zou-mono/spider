# -*- coding: utf8 -*-

import csv
import codecs
import coordTransform_utils
import sys,os
from decimal import *
getcontext().rounding = ROUND_HALF_UP

reload(sys)
sys.setdefaultencoding('utf-8')

filename='火车站出发地址'
path=unicode('data/'+filename+'.csv',"utf-8")
# filenames = os.listdir(path)
input = csv.reader(codecs.open(path, 'r', 'gbk'))
output = csv.writer(codecs.open('data/'+filename+'_c.csv', 'w', 'gbk'))

icount = 0
for row in input:
    result = coordTransform_utils.gcj02_to_wgs84(float(row[3]), float(row[2]))
    # output.writerow([row[0],row[1],row[2],row[3],row[4],round(result[0],6), round(result[1],6)])
    output.writerow([row[0],row[1],round(result[0],6), round(result[1],6)])
    icount += 1
    print(icount)