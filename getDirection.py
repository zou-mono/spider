# -*- coding: utf8 -*-

import csv
import codecs
import requests
import json, time
import coordTransform_utils
import sys
import xlwt, xlrd
from decimal import *

getcontext().rounding = ROUND_HALF_UP

reload(sys)
sys.setdefaultencoding('utf-8')


def cal_steps(route):
    preType = -1
    totalWalkDistance = 0  # 步行距离
    totalWalkTime = 0  # 步行时间
    transferNum = 0  # 换乘次数
    iStepNum = 0
    for step in route['steps']:
        curType = step[0]['vehicle_info']['type']
        if curType == 5:
            totalWalkDistance = totalWalkDistance + step[0]['distance']
            totalWalkTime = totalWalkTime + step[0]['duration']
        if curType == 3 and preType == 3:
            transferNum = transferNum + 1

        if 0 < iStepNum < len(route['steps']) - 1 and curType != 5:
            preType = curType

        iStepNum = iStepNum + 1

    return totalWalkDistance, totalWalkTime, transferNum


def try_scrapy(url):
    trytime = 0
    while True:
        try:
            response = json.loads(requests.get(url).content, encoding='utf-8')
            if response['status'] == 0:
                if len(response['result']):
                    if len(response['result']['routes']):
                        return response
            return 'error'
        except:
            trytime = trytime + 1
            if trytime >= 5:
                info = sys.exc_info()
                print "抓取失败. 错误原因: ", info[0], ":", info[1]
                break
            else:
                time.sleep(5)
                continue
        break


def init_get(filename):
    ak_key = 'lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'

    inpath = unicode('data/' + filename + '.xlsx', "utf-8")
    outpath = unicode('data/' + filename + '_c.xls', "utf-8")

    inbook = xlrd.open_workbook(inpath)
    outbook = xlwt.Workbook(encoding='utf-8', style_compression=0)

    try:
        for i in range(0, len(inbook.sheet_names())):
            sheet = inbook.sheet_by_index(i)
            outsheet = outbook.add_sheet(inbook.sheet_names()[i])

            outsheet.write(0, 0, 'tripID')
            outsheet.write(0, 1, 'alter')
            outsheet.write(0, 2, 'O_lon')  # 起点X
            outsheet.write(0, 3, 'O_lat')  # 起点Y
            outsheet.write(0, 4, 'D_lon')  # 终点X
            outsheet.write(0, 5, 'D_lat')  # 终点Y
            outsheet.write(0, 6, 'distance')  # 出行距离
            outsheet.write(0, 7, 'duration')  # 出行时间
            outsheet.write(0, 8, 'price')  # 出行成本
            outsheet.write(0, 9, 'walkDistance')  # 步行距离
            outsheet.write(0, 10, 'walkTime')  # 步行时间
            outsheet.write(0, 11, 'transferNumber')  # 换乘次数

            icount = 1
            for nrow in range(1, sheet.nrows):
                origin = str(sheet.cell_value(nrow, 2)) + ',' + str(sheet.cell_value(nrow, 1))
                destination = str(sheet.cell_value(nrow, 4)) + ',' + str(sheet.cell_value(nrow, 3))
                # tmp = sheet.cell_value(nrow,1).decode('utf-8')

                if nrow > 7400:
                    ak_key = 'MVeqoIZ3i3nygLwUKc3lvW9NvVKsDP9v'

                # 公交推荐
                url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                    origin, destination, ak_key)
                # response = json.loads(requests.get(url).content, encoding='utf-8')
                response = try_scrapy(url)

                if response != 'error':
                    route = response['result']['routes'][0]
                    outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                    outsheet.write(icount, 1, '推荐公交')
                    outsheet.write(icount, 2, sheet.cell_value(nrow, 1))  # 起点X
                    outsheet.write(icount, 3, sheet.cell_value(nrow, 2))  # 起点Y
                    outsheet.write(icount, 4, sheet.cell_value(nrow, 3))  # 终点X
                    outsheet.write(icount, 5, sheet.cell_value(nrow, 4))  # 终点Y
                    outsheet.write(icount, 6, route['distance'])  # 出行距离
                    outsheet.write(icount, 7, route['duration'])  # 出行时间
                    outsheet.write(icount, 8, route['price'])  # 出行成本
                    totalWalkDistance, totalWalkTime, transferNum = cal_steps(route)
                    outsheet.write(icount, 9, totalWalkDistance)  # 步行距离
                    outsheet.write(icount, 10, totalWalkTime)  # 步行时间
                    outsheet.write(icount, 11, transferNum)  # 换乘次数
                    outsheet.write(icount, 12, sheet.cell_value(nrow, 5))  # 名称

                    icount += 1

                # # 巴士优先
                # url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&tactics_incity=3&ak={2}'.format(
                #     origin, destination, ak_key)
                # # response = json.loads(requests.get(url).content, encoding='utf-8')
                # response = try_scrapy(url)
                # if response != 'error':
                #     route = response['result']['routes'][0]
                #     outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                #     outsheet.write(icount, 1, '巴士优先')
                #     outsheet.write(icount, 2, sheet.cell_value(nrow, 1))  # 起点X
                #     outsheet.write(icount, 3, sheet.cell_value(nrow, 2))  # 起点Y
                #     outsheet.write(icount, 4, sheet.cell_value(nrow, 3))  # 终点X
                #     outsheet.write(icount, 5, sheet.cell_value(nrow, 4))  # 终点Y
                #     outsheet.write(icount, 6, route['distance'])  # 出行距离
                #     outsheet.write(icount, 7, route['duration'])  # 出行时间
                #     outsheet.write(icount, 8, route['price'])  # 出行成本
                #     totalWalkDistance, totalWalkTime, transferNum = cal_steps(route)
                #     outsheet.write(icount, 9, totalWalkDistance)  # 步行距离
                #     outsheet.write(icount, 10, totalWalkTime)  # 步行时间
                #     outsheet.write(icount, 11, transferNum)  # 换乘次数
                #     outsheet.write(icount, 12, sheet.cell_value(nrow, 5))  # 名称
                #
                #     icount += 1
                #
                # # 骑车
                # url = 'http://api.map.baidu.com/direction/v2/riding?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                #     origin, destination, ak_key)
                # # response = json.loads(requests.get(url).content, encoding='utf-8')
                # response = try_scrapy(url)
                #
                # if response != 'error':
                #     route = response['result']['routes'][0]
                #     outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                #     outsheet.write(icount, 1, '骑车')
                #     outsheet.write(icount, 2, sheet.cell_value(nrow, 1))  # 起点X
                #     outsheet.write(icount, 3, sheet.cell_value(nrow, 2))  # 起点Y
                #     outsheet.write(icount, 4, sheet.cell_value(nrow, 3))  # 终点X
                #     outsheet.write(icount, 5, sheet.cell_value(nrow, 4))  # 终点Y
                #     outsheet.write(icount, 6, route['distance'])  # 出行距离
                #     outsheet.write(icount, 7, route['duration'])  # 出行时间
                #     outsheet.write(icount, 8, 0)  # 出行成本
                #     outsheet.write(icount, 9, 0)  # 步行距离
                #     outsheet.write(icount, 10, 0)  # 步行时间
                #     outsheet.write(icount, 11, 0)  # 换乘次数
                #     outsheet.write(icount, 12, sheet.cell_value(nrow, 5))  # 名称
                #
                #     icount += 1

                # 小汽车
                url = 'http://api.map.baidu.com/direction/v2/driving?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                    origin, destination, ak_key)
                response = try_scrapy(url)
                if response != 'error':
                    route = response['result']['routes'][0]
                    outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                    outsheet.write(icount, 1, '小汽车')
                    outsheet.write(icount, 2, sheet.cell_value(nrow, 1))  # 起点X
                    outsheet.write(icount, 3, sheet.cell_value(nrow, 2))  # 起点Y
                    outsheet.write(icount, 4, sheet.cell_value(nrow, 3))  # 终点X
                    outsheet.write(icount, 5, sheet.cell_value(nrow, 4))  # 终点Y
                    outsheet.write(icount, 6, route['distance'])  # 出行距离
                    outsheet.write(icount, 7, route['duration'])  # 出行时间
                    outsheet.write(icount, 8, 0.56 * route['distance'] / 1000)  # 出行成本
                    outsheet.write(icount, 9, 0)  # 步行距离
                    outsheet.write(icount, 10, 0)  # 步行时间
                    outsheet.write(icount, 11, 0)  # 换乘次数
                    outsheet.write(icount, 12, sheet.cell_value(nrow, 5))  # 名称

                    icount += 1

                # # 出租车
                # if response != 'error':
                #     route = response['result']['routes'][0]
                #     outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                #     outsheet.write(icount, 1, '出租车')
                #     outsheet.write(icount, 2, sheet.cell_value(nrow, 1))  # 起点X
                #     outsheet.write(icount, 3, sheet.cell_value(nrow, 2))  # 起点Y
                #     outsheet.write(icount, 4, sheet.cell_value(nrow, 3))  # 终点X
                #     outsheet.write(icount, 5, sheet.cell_value(nrow, 4))  # 终点Y
                #     outsheet.write(icount, 6, route['distance'])  # 出行距离
                #     outsheet.write(icount, 7, route['duration'])  # 出行时间
                #     outsheet.write(icount, 8, route['taxi_fee'])  # 出行成本
                #     outsheet.write(icount, 9, 0)  # 步行距离
                #     outsheet.write(icount, 10, 0)  # 步行时间
                #     outsheet.write(icount, 11, 0)  # 换乘次数
                #     outsheet.write(icount, 12, sheet.cell_value(nrow, 5))  # 名称
                #
                #     icount += 1

                outbook.save(outpath)
                print sheet.cell_value(nrow, 0)
    finally:
        outbook.save(outpath)
        print("over")

def init_get2(filename):
    ak_key = 'lbvmtR1hSGoDEtDqZxyKQuefabqoVoOB'

    input = csv.reader(codecs.open('data/' + filename + '.csv', 'r', 'utf-8'))
    output = csv.writer(codecs.open('data/' + filename + '_c.csv', 'w', 'gbk'))

    # infile = open('data/' + filename + '.csv')

    try:
        output.writerow(['tripID','alter','O_lon','O_lat','D_lon','D_lat','distance','duration','price','walkDistance','walkTime','transferNumber'])

        icount = 1
        iline = 0
        for nrow in input:
            if iline < 22195:
                iline += 1
                continue

            origin = str(nrow[2]) + ',' + str(nrow[1])
            destination = str(nrow[4]) + ',' + str(nrow[3])

            if iline > 7400:
                ak_key = 'MVeqoIZ3i3nygLwUKc3lvW9NvVKsDP9v'

            # 公交推荐
            url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                origin, destination, ak_key)
            # response = json.loads(requests.get(url).content, encoding='utf-8')
            response = try_scrapy(url)

            if response != 'error':
                route = response['result']['routes'][0]
                totalWalkDistance, totalWalkTime, transferNum = cal_steps(route)
                for rot in response['result']['routes']:
                    if rot['price'] > 0:
                        route = rot
                        break
                output.writerow([nrow[0],
                                 '推荐公交',
                                 nrow[1],
                                 nrow[2],
                                 nrow[3],
                                 nrow[4],
                                 route['distance'],
                                 route['duration'],
                                 route['price'],
                                 totalWalkDistance,
                                 totalWalkTime,
                                 transferNum])
                icount += 1

            # 巴士优先
            url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&tactics_incity=3&ak={2}'.format(
                origin, destination, ak_key)
            # response = json.loads(requests.get(url).content, encoding='utf-8')
            response = try_scrapy(url)
            if response != 'error':
                route = response['result']['routes'][0]
                for rot in response['result']['routes']:
                    if rot['price'] > 0:
                        route = rot
                        break
                totalWalkDistance, totalWalkTime, transferNum = cal_steps(route)
                output.writerow([nrow[0],
                                 '巴士优先',
                                 nrow[1],
                                 nrow[2],
                                 nrow[3],
                                 nrow[4],
                                 route['distance'],
                                 route['duration'],
                                 route['price'],
                                 totalWalkDistance,
                                 totalWalkTime,
                                 transferNum])
                icount += 1
            #
            # 骑车
            url = 'http://api.map.baidu.com/direction/v2/riding?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                origin, destination, ak_key)
            # response = json.loads(requests.get(url).content, encoding='utf-8')
            response = try_scrapy(url)

            if response != 'error':
                route = response['result']['routes'][0]
                output.writerow([nrow[0],
                                 '骑车',
                                 nrow[1],
                                 nrow[2],
                                 nrow[3],
                                 nrow[4],
                                 route['distance'],
                                 route['duration'],
                                 0,
                                 0,
                                 0,
                                 0])
                icount += 1

            # 小汽车
            url = 'http://api.map.baidu.com/direction/v2/driving?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                origin, destination, ak_key)
            response = try_scrapy(url)
            if response != 'error':
                route = response['result']['routes'][0]
                output.writerow([nrow[0],
                                 '小汽车',
                                 nrow[1],
                                 nrow[2],
                                 nrow[3],
                                 nrow[4],
                                 route['distance'],
                                 route['duration'],
                                 0.56 * route['distance'] / 1000,
                                 0,
                                 0,
                                 0])
                icount += 1

            # 出租车
            if response != 'error':
                route = response['result']['routes'][0]
                output.writerow([nrow[0],
                                 '出租车',
                                 nrow[1],
                                 nrow[2],
                                 nrow[3],
                                 nrow[4],
                                 route['distance'],
                                 route['duration'],
                                 route['taxi_fee'],
                                 0,
                                 0,
                                 0])
                icount += 1

            iline += 1
            print nrow[0]
    finally:
        print("over")

def second_get(filename):
    ak_key = 'MVeqoIZ3i3nygLwUKc3lvW9NvVKsDP9v'

    inpath = unicode('data/' + filename + '.xls', "utf-8")
    outpath = unicode('data/' + filename + '_c.xls', "utf-8")

    inbook = xlrd.open_workbook(inpath)
    outbook = xlwt.Workbook(encoding='utf-8', style_compression=0)

    try:
        for i in range(0, len(inbook.sheet_names())):
            sheet = inbook.sheet_by_index(i)
            outsheet = outbook.add_sheet(inbook.sheet_names()[i])

            outsheet.write(0, 0, 'tripID')
            outsheet.write(0, 1, 'alter')
            outsheet.write(0, 2, 'O_lon')  # 起点X
            outsheet.write(0, 3, 'O_lat')  # 起点Y
            outsheet.write(0, 4, 'D_lon')  # 终点X
            outsheet.write(0, 5, 'D_lat')  # 终点Y
            outsheet.write(0, 6, 'distance')  # 出行距离
            outsheet.write(0, 7, 'duration')  # 出行时间
            outsheet.write(0, 8, 'price')  # 出行成本
            outsheet.write(0, 9, 'walkDistance')  # 步行距离
            outsheet.write(0, 10, 'walkTime')  # 步行时间
            outsheet.write(0, 11, 'transferNumber')  # 换乘次数

            icount = 1
            for nrow in range(1, sheet.nrows):
                if sheet.cell_value(nrow, 8) != -1:
                    outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                    outsheet.write(icount, 1, sheet.cell_value(nrow, 1))
                    outsheet.write(icount, 2, sheet.cell_value(nrow, 2))  # 起点X
                    outsheet.write(icount, 3, sheet.cell_value(nrow, 3))  # 起点Y
                    outsheet.write(icount, 4, sheet.cell_value(nrow, 4))  # 终点X
                    outsheet.write(icount, 5, sheet.cell_value(nrow, 5))  # 终点Y
                    outsheet.write(icount, 6, sheet.cell_value(nrow, 6))  # 出行距离
                    outsheet.write(icount, 7, sheet.cell_value(nrow, 7))  # 出行时间
                    outsheet.write(icount, 8, sheet.cell_value(nrow, 8))  # 出行成本
                    outsheet.write(icount, 9, sheet.cell_value(nrow, 9))  # 步行距离
                    outsheet.write(icount, 10, sheet.cell_value(nrow, 10))  # 步行时间
                    outsheet.write(icount, 11, sheet.cell_value(nrow, 11))  # 换乘次数
                    icount += 1
                    outbook.save(outpath)
                    continue

                origin = str(sheet.cell_value(nrow, 3)) + ',' + str(sheet.cell_value(nrow, 2))
                destination = str(sheet.cell_value(nrow, 5)) + ',' + str(sheet.cell_value(nrow, 4))

                if sheet.cell_value(nrow, 1) == '推荐公交':
                    # 公交推荐
                    url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&ak={2}'.format(
                        origin, destination, ak_key)
                elif sheet.cell_value(nrow, 1) == '巴士优先':
                    # 巴士优先
                    url = 'http://api.map.baidu.com/direction/v2/transit?origin={0}&destination={1}&coord_type=wgs84&tactics_incity=3&ak={2}'.format(
                        origin, destination, ak_key)

                response = try_scrapy(url)
                if response != 'error':
                    route = response['result']['routes'][0]
                    for rot in response['result']['routes']:
                        if rot['price'] > 0:
                            route = rot
                            break

                    outsheet.write(icount, 0, sheet.cell_value(nrow, 0))
                    outsheet.write(icount, 1, sheet.cell_value(nrow, 1))
                    outsheet.write(icount, 2, sheet.cell_value(nrow, 2))  # 起点X
                    outsheet.write(icount, 3, sheet.cell_value(nrow, 3))  # 起点Y
                    outsheet.write(icount, 4, sheet.cell_value(nrow, 4))  # 终点X
                    outsheet.write(icount, 5, sheet.cell_value(nrow, 5))  # 终点Y
                    outsheet.write(icount, 6, route['distance'])  # 出行距离
                    outsheet.write(icount, 7, route['duration'])  # 出行时间
                    outsheet.write(icount, 8, route['price'])  # 出行成本
                    totalWalkDistance, totalWalkTime, transferNum = cal_steps(route)
                    outsheet.write(icount, 9, totalWalkDistance)  # 步行距离
                    outsheet.write(icount, 10, totalWalkTime)  # 步行时间
                    outsheet.write(icount, 11, transferNum)  # 换乘次数
                    icount += 1

                outbook.save(outpath)
                print sheet.cell_value(nrow, 0)
    finally:
        outbook.save(outpath)
        print("over")


def main():
    # init_get("OD地址")
    # second_get('OD地址_修正')
    # init_get('zoneOD')
    init_get2('Trip_OD_0929')

if __name__ == '__main__':
    main()
