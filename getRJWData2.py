# -*- coding: utf8 -*-

import urllib, urllib2
import sys, os
from optparse import OptionParser
from datetime import *
import time, threadpool, threading
import csv, codecs
import hashlib
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

def Crawl(vdatetime):
    header = ['监测点位名称', '空气质量指数(AQI)', '首要污染物', '空气质量指数级别', '空气质量指数类别_类别', '空气质量指数类别_颜色及健康提示',
              '颗粒物(粒径小于等于2.5μm)一小时平均_浓度(μg/m3)', '颗粒物(粒径小于等于2.5μm)24小时滑动平均_浓度(μg/m3)', '颗粒物(粒径小于等于2.5μm)24小时滑动平均_分指数',
              '二氧化硫(SO2)一小时平均_浓度(μg/m3)', '二氧化硫(SO2)一小时平均_分指数',
              '二氧化氮(NO2)一小时平均_浓度(μg/m3)', '二氧化氮(NO2)一小时平均_分指数',
              '颗粒物(粒径小于等于10um)一小时平均_浓度(μg/m3)',
              '颗粒物(粒径小于等于10um)24小时滑动平均_浓度(μg/m3)', '颗粒物(粒径小于等于10um)24小时滑动平均_ 分指数',
              '一氧化碳(CO)一小时平均_浓度(mg/m3)', '一氧化碳(CO)一小时平均_ 分指数',
              '臭氧(O3)一小时平均_浓度(μg/m3)', '臭氧(O3)一小时平均_分指数',
              '臭氧(O3)8小时滑动平均_浓度(μg/m3)', '臭氧(O3)8小时滑动平均_分指数']
    date = vdatetime.strftime("%Y-%m-%d")
    hour = vdatetime.strftime("%H")
    year = vdatetime.strftime("%Y")
    month = vdatetime.strftime("%m")
    day = vdatetime.strftime("%d")
    sdatetime = vdatetime.strftime("%Y-%m-%d %H:%M:%S")

    outDic = './DetectorData/' + year + '/' + month + '/' + day + '/'
    #outDic = './DetectorData/' + year + '/'
    outFile = date + '_' + hour + '.csv'

    mutex = threading.Lock()
    mutex.acquire(100)
    try:
        if not os.path.exists(outDic):
            os.makedirs(outDic)
    except Exception as e:
        print e.message

    mutex.release()

    output = csv.writer(codecs.open(outDic + outFile, 'w', 'gbk'))
    output.writerow(header)
    url = 'http://203.91.46.40/pages/szepb/kqzl/TGzfwHjKqzlzs.jsp'

    # 定义请求头
    reqheaders = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                  'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
                  'Cache-Control': 'no-cache',
                  'Connection': 'keep-alive',
                  'Accept-Encoding': 'gzip, deflate',
                  'Content-Length': '102',
                  'Content-Type': 'application/x-www-form-urlencoded',
                  'Cookie': 'JSESSIONID=9D64F6F14A2DCB107BCB9F08AF94D4FD',
                  'Host': '203.91.46.40',
                  'Origin': 'http://203.91.46.40',
                  'Pragma': 'no-cache',
                  'Referer': 'http://203.91.46.40/pages/szepb/kqzl/TGzfwHjKqzlzs.jsp',
                  'Upgrade-Insecure-Requests': '1',
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

    m2 = hashlib.md5()
    m2.update(sdatetime)
    #print m2.hexdigest()
# 定义post的参数
    body_value = {'hash': m2.hexdigest(),
                  'FROM_SELF': 'true',
                  'q_JCRQ': date,
                  'q_SDATE': hour,
                  'q_JCDW': '',
                  'q_JCDW_text': ''}

    # 对请求参数进行编码
    data = urllib.urlencode(body_value)

    trytime = 0
    while True:
        try:
            request = urllib2.Request(url=url, data=data, headers=reqheaders)
            r = urllib2.urlopen(request)
            resStr = r.read()

            soup = BeautifulSoup(resStr, 'html.parser')
            tables = soup.find_all('table')

            # print len(tables)
            if len(tables) > 1:
                tab = tables[1]
                tbody = tab.find_all("tbody")
                if len(tbody[0].find_all('tr')) == 0:
                    print(date + '_' + time + "数据为空！")
                    return
            else:
                print(date + '_' + time + "数据为空！")
                return

            for tr in tbody[0].find_all('tr'):
                reslist = []
                colCount = 0
                for td in tr.findAll('td'):
                    if colCount == 5:  # 第五列是图片，跳过
                        reslist.append("")
                    else:
                        reslist.append(td.getText().encode('gbk', 'ignore').decode('gbk'))
                    colCount = colCount + 1
                    # print td.getText(),
                output.writerow(reslist)
            print(date + '_' + hour + "数据成功抓取！")
        except:
            trytime = trytime + 1
            if trytime >= 5:
                info = sys.exc_info()
                date = vdatetime.strftime("%Y-%m-%d %H:%M:%S")
                print date, "抓取失败. 错误原因: ", info[0], ":", info[1]
                break
            else:
                time.sleep(5)
                continue
        break

def main(argv):
    argv = ["-s", "2017-06-25 00:00:00", "-e", "2017-06-26 10:00:00"]
    usage = "getRJWData[ -s <start>][-e <end>] arg1[,arg2..]"

    parser = OptionParser(usage)
    parser.add_option("-s", "--start", action="store", type="string",
                      dest="start",
                      default=False,
                      help="开始时间")
    parser.add_option("-e", "--end", action="store", type="string",
                      dest="end",
                      default=False,
                      help="结束时间")

    (options, args) = parser.parse_args(argv)
    start = options.start
    end = options.end

    if start == False or end == False:
        print "错误：缺失参数！"
        sys.exit(2)

    starttime = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    endtime = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    deltatime = (endtime - starttime).total_seconds()

    param_list=[]
    pool = threadpool.ThreadPool(24)
    for t in range(0, int(deltatime / 3600) + 1):
        vdatetime = starttime + timedelta(hours=t)
        param_list.append(vdatetime)

    requests = threadpool.makeRequests(Crawl, param_list)
    [pool.putRequest(req) for req in requests]
    pool.wait()

if __name__ == '__main__':
    main(sys.argv)
