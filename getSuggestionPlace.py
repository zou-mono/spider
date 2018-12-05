# -*- coding: utf8 -*-

import sys
# import urllib2
import requests
import json
import csv
import codecs
import coordTransform_utils

reload(sys)
sys.setdefaultencoding('utf-8')

input = csv.reader(codecs.open('data/NoLocation.csv', 'r', 'utf-8'))
output = csv.writer(codecs.open('data/searchLocation.csv', 'w', 'gbk'))
icount = 0
for row in input:
    address = row[1].decode()
    # if address=='市政':
    #     print '市政'
    if address[-1] == '东' or address[-1] == '西' or address[-1] == '南' or address[-1] == '北':
        address = address[0:len(address) - 1]

    url = 'http://api.map.baidu.com/place/v2/suggestion?query={0}&region=深圳市&city_limit=true&output=json&ak=lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'.format(
        address)
    response = json.loads(requests.get(url).content, encoding='utf-8')
    if len(response['result']):
        for resrow in response['result']:
            if resrow.has_key('location'):
                result = coordTransform_utils.bd09_to_wgs84(resrow['location']['lng'], resrow['location']['lat'])
                if 113.7132 < result[0] < 114.6309 and 22.3932 < result[1] < 22.8816:
                    output.writerow([row[0], row[1], str(result[0]), str(result[1])])
                else:
                    output.writerow([row[0], row[1], '', ''])
                break
        else:
            output.writerow([row[0], row[1], '', ''])
    else:
        output.writerow([row[0], row[1], '', ''])
    icount += 1
    print icount
