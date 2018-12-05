# -*- coding: utf8 -*-

import csv
import codecs
import requests
import json
import coordTransform_utils
import sys
import xlwt, xlrd
from decimal import *

getcontext().rounding = ROUND_HALF_UP

reload(sys)
sys.setdefaultencoding('utf-8')


def init_get(filename):
    inpath = unicode('data/' + filename + '.csv', "utf-8")
    input = csv.reader(codecs.open(inpath, 'r', 'gbk'))
    outpath = unicode('res/' + filename + '_c.csv', "utf-8")
    output = csv.writer(codecs.open(outpath, 'w', 'gbk'))

    icount = 0
    for row in input:
        tmp = row[1].decode('utf-8')
        region = tmp[0:tmp.find('市')]
        address = tmp[tmp.find('市') + 1:len(tmp)]

        if region == -1 or address == -1:
            output.writerow(['0', row[0], row[1], '', '', ''])
            continue

        url1 = 'http://api.map.baidu.com/place/v2/suggestion?query={0}&region={1}&city_limit=true&output=json&ak=lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'.format(
            address, region)
        response = json.loads(requests.get(url1).content, encoding='utf-8')
        if len(response['result']):
            for resrow in response['result']:
                if resrow.has_key('location'):
                    # location = str(resrow['location']['lat']) + ',' + str(resrow['location']['lng'])
                    # url2 = 'http://api.map.baidu.com/geocoder/v2/?location={0}&output=json&pois=1&ak=lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'.format(
                    #     location)

                    result = coordTransform_utils.bd09_to_wgs84(resrow['location']['lng'], resrow['location']['lat'])

                    format_address = resrow['province'] + resrow['city'] + resrow['district'] + resrow['name']
                    output.writerow(['1', row[0], row[1], str(result[0]), str(result[1]), format_address])
                    break
        else:
            output.writerow(['0', row[0], row[1], '', '', ''])

        icount += 1
        print(icount)
    print("over")

def second_get(filename):
    inpath = unicode('data/' + filename + '.xlsx', "utf-8")
    outpath = unicode('data/' + filename + '_c.xls', "utf-8")

    inbook = xlrd.open_workbook(inpath)
    outbook = xlwt.Workbook(encoding='utf-8', style_compression=0)

    for i in range(0,len(inbook.sheet_names())):
        sheet = inbook.sheet_by_index(i)
        outsheet =  outbook.add_sheet(inbook.sheet_names()[i])

        icount = 0
        for nrow in range(0,sheet.nrows):
            id = sheet.cell_value(nrow,0)
            tmp = sheet.cell_value(nrow,1).decode('utf-8')
            region = tmp[0:tmp.find('市')]
            address = tmp[tmp.find('市') + 1:len(tmp)]

            url = 'http://api.map.baidu.com/place/v2/suggestion?query={0}&region={1}&city_limit=true&output=json&ak=lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'.format(
                address, region)
            response = json.loads(requests.get(url).content, encoding='utf-8')
            if len(response['result']):
                for resrow in response['result']:
                    if resrow.has_key('location'):
                        result = coordTransform_utils.bd09_to_wgs84(resrow['location']['lng'], resrow['location']['lat'])

                        format_address = resrow['province'] + resrow['city'] + resrow['district'] + resrow['name']

                        outsheet.write(nrow,0,id)
                        outsheet.write(nrow,1,address)
                        outsheet.write(nrow,2,str(result[0]))
                        outsheet.write(nrow,3,str(result[1]))
                        outsheet.write(nrow,4,format_address)
                        break
            else:
                outsheet.write(nrow,0,id)
                outsheet.write(nrow,1,address)
                outsheet.write(nrow,2,'')
                outsheet.write(nrow,3,'')
                outsheet.write(nrow,4,'')

            icount += 1
            print(icount)
    outbook.save(outpath)
    print("over")

def main():
    # init_get("火车站出发地址")
    # init_get("火车站到达地址")
    # init_get("机场出发地址")
    # init_get("机场到达地址")
    # init_get("旅客入境")
    # init_get("旅客出境")
    # init_get("码头出境地址")
    # init_get("码头入境地址")
    second_get("口岸车辆和汽车站核查地址3")


if __name__ == '__main__':
    main()
