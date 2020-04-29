# -*- coding: utf8 -*-

import csv
import codecs
import coordTransform_utils
import sys,os
from decimal import *
getcontext().rounding = ROUND_HALF_UP

# sys.setdefaultencoding('utf-8')

res = coordTransform_utils.bd09_to_wgs84(114.03842,22.754845)
print(str(round(res[0],6)))
print(str(round(res[1],6)))

res = coordTransform_utils.bd09_to_wgs84(114.142479,22.861718)
print(str(round(res[0],6)))
print(str(round(res[1],6)))

filename='station.csv'
path='data/'+filename
# filenames = os.listdir(path)
# input = csv.reader(codecs.open('data/'+filename, 'w', 'utf-8'))
output = csv.writer(codecs.open('data/'+filename+'_c.csv', 'w', 'gbk'))

with open('data/station.csv', 'r', encoding='utf-8') as f:
    data = csv.reader(f, delimiter=',', quotechar='"')
    # data = f.read()
    # f_csv = data.encode('utf-8').decode('utf-8-sig')
    icount = 0
    for row in data:
        if icount == 0:
            output.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]])
            icount += 1
            continue

        result = coordTransform_utils.gcj02_to_wgs84(float(row[4]), float(row[3]))
        # result = coordTransform_utils.gcj02_to_wgs84(float(row[1]), float(row[2]))
        output.writerow([row[0], row[1], row[2], round(result[1],6), round(result[0],6), row[5], row[6], row[7]])
        icount += 1
        print(icount)



