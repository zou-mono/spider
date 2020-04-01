# -*- coding: utf8 -*-

import urllib.request, urllib.parse
import re
from bs4 import BeautifulSoup
import math
import csv, codecs
import jsonlines

url = 'http://zjj.sz.gov.cn/ris/bol/szfdc/index.aspx'
domain_url = 'http://zjj.sz.gov.cn/ris/bol/szfdc'
land_type = set([])
res = []
ddlPageCount = 20 # 每页的条数参数
jsonl_path = "./res.jsonl"

def main():

    # 获取post基本参数信息
    VIEWSTATE, EVENTVALIDATION, pageCount = get_base_para()

    # 遍历所有数据，把土地用途全部提取出来
    # traversal_landuse_type(VIEWSTATE, EVENTVALIDATION, pageCount)

    traversal_landinfo(VIEWSTATE, EVENTVALIDATION, pageCount)

    with jsonlines.open(jsonl_path, mode='w', flush=True) as out_json:
        for l in res:
            out_json.write(l)

    print("over!")


def json2csv():
    outfile = codecs.open("result.csv", 'w', 'gbk')

    headers = []
    with jsonlines.open(jsonl_path, mode='r') as in_json:
        for l in in_json:
            for entry in list(l.keys()):
                headers.append(entry)
    headerSet = list(set(headers))
    headerSet.sort(key=headers.index)
    print(headerSet)
    # reslist.sort(key=header.index)

    output = csv.DictWriter(outfile, fieldnames=headerSet)
    output.writeheader()

    res = []
    resEntry = {}
    with jsonlines.open(jsonl_path, mode='r') as in_json:
        for l in in_json:
            for header in headerSet:
                if header in list(l.keys()):
                    resEntry[header] = l[header]
                else:
                    resEntry[header] = ''

            output.writerow(resEntry)
            outfile.flush()


def get_base_para():
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'zjj.sz.gov.cn',
                  'Pragma': 'no-cache'}

    req = urllib.request.Request(url=url, headers=reqheaders)
    r = urllib.request.urlopen(req)
    respHtml = r.read().decode('utf-8')
    soup = BeautifulSoup(respHtml, 'html.parser')
    # respHtml = respHtml.decode('utf-8')
    VIEWSTATE = get_VIEWSTATE(respHtml)
    EVENTVALIDATION = get_EVENTVALIDATION(respHtml)
    print(VIEWSTATE)
    print(EVENTVALIDATION)

    # 获取记录总条数
    content = soup.find('div', id='SelectPageSizeDiv')
    recordCount = get_recordCount(content.span.getText())
    # 获取页码数
    pageCount = math.ceil(int(recordCount) / ddlPageCount)
    return VIEWSTATE, EVENTVALIDATION, pageCount


def get_page_content(VIEWSTATE, EVENTVALIDATION, icurPage):
    # 定义请求头
    reqheaders = {'Content-Type': 'application/x-www-form-urlencoded',
                  'Host': 'zjj.sz.gov.cn',
                  'Pragma': 'no-cache'}

    # 定义post的参数
    body_value = {'scriptManager2': 'updatepanel2|AspNetPager1',
                  '__EVENTTARGET': 'AspNetPager1',
                  '__VIEWSTATE': VIEWSTATE,
                  '__EVENTVALIDATION': EVENTVALIDATION,
                  'ddlPageCount': ddlPageCount,
                  '__VIEWSTATEENCRYPTED': '',
                  '__EVENTARGUMENT': icurPage}

    # 对请求参数进行编码
    data = urllib.parse.urlencode(body_value).encode(encoding='UTF8')
    # 请求不同页面的数据
    req = urllib.request.Request(url=url, data=data, headers=reqheaders)
    r = urllib.request.urlopen(req)
    respHtml = r.read().decode('utf-8')
    soup = BeautifulSoup(respHtml, 'html.parser')

    return soup


def get_VIEWSTATE(r):
    pattern1 = r'VIEWSTATE\".*value=\".*\"'
    match = re.search(pattern1, r).group(0)
    pattern2 = r'VIEWSTATE\" id=\"__VIEWSTATE\" value=\"'
    match1 = re.split(pattern2,match)
    return match1[1][:-1] #返回_VIEWSTATE


def get_EVENTVALIDATION(r):
    pattern1 = r'EVENTVALIDATION\".*value=\".*\"'
    match = re.search(pattern1, r).group(0)
    pattern2 = r'EVENTVALIDATION\" id=\"__EVENTVALIDATION\" value=\"'
    match1 = re.split(pattern2,match)
    return match1[1][:-1] #返回_EVENTVALIDATION


def get_recordCount(r):
    pattern1 = r'共(\d+)条'
    match = re.search(pattern1, r).group(1).strip()
    return match


def get_certdetailHtml(r):
    certdetail_url = re.sub(r'\.', domain_url, r, 1)

    # 定义请求头
    reqheaders = {'Host': 'zjj.sz.gov.cn',
                  'Referer': 'http://zjj.sz.gov.cn/ris/bol/szfdc/index.aspx',
                  'Connection': 'keep-alive'}
    req = urllib.request.Request(url=certdetail_url, headers=reqheaders)
    r = urllib.request.urlopen(req)
    respHtml = r.read().decode('utf-8')
    soup = BeautifulSoup(respHtml, 'html.parser')
    content = soup.find('table')

    return content


def traversal_landuse_type(VIEWSTATE, EVENTVALIDATION, pageCount):
    irow = 0
    for icurPage in range(1, pageCount + 1):
        soup = get_page_content(VIEWSTATE, EVENTVALIDATION, icurPage)

        # 获得总表信息
        content = soup.find('div', class_='record fix')
        content = content.find('table')
        trs = content.find_all('tr')

        for i in range(1, len(trs) - 1):
            tr = trs[i]
            tds = tr.find_all("td")

            content = get_certdetailHtml(tds[1].a.attrs['href'])
            trs2 = content.find('tbody').find_all('tr')
            for j in range(7, 15):
                td2 = trs2[j].find_all('td')[1]
                land_type.add(td2.getText().strip())

            irow += 1
            print(irow)

    print(land_type)


def get_num(r):
    pattern1 = r'(\d+(\.\d+)?)'
    match = re.search(pattern1, r)
    if match is None:
        return 0
    else:
        return match.group(0)


def traversal_landinfo(VIEWSTATE, EVENTVALIDATION, pageCount):
    irow = 0
    for icurPage in range(1, pageCount + 1):
        soup = get_page_content(VIEWSTATE, EVENTVALIDATION, icurPage)

        # 获得总表信息
        content = soup.find('div', class_='record fix')
        content = content.find('table')
        trs = content.find_all('tr')

        for i in range(1, len(trs) - 1):
            tr = trs[i]
            tds = tr.find_all("td")

            content = get_certdetailHtml(tds[1].a.attrs['href'])
            trs2 = content.find('tbody').find_all('tr')

            tempA = {
                trs2[1].find_all('td')[0].getText().strip(): trs2[1].find_all('td')[1].getText().strip(),  # 许可证号
                trs2[1].find_all('td')[2].getText().strip(): trs2[1].find_all('td')[3].getText().strip(),  # 项目名称
                trs2[2].find_all('td')[0].getText().strip(): trs2[2].find_all('td')[1].getText().strip(),  # 发展商
                trs2[2].find_all('td')[2].getText().strip(): trs2[2].find_all('td')[3].getText().strip(),  # 所在位置
                trs2[3].find_all('td')[0].getText().strip(): trs2[3].find_all('td')[1].getText().strip(),  # 栋数
                trs2[3].find_all('td')[2].getText().strip(): trs2[3].find_all('td')[3].getText().strip(),  # 地块编号
                trs2[4].find_all('td')[0].getText().strip(): trs2[4].find_all('td')[1].getText().strip(),  # 房产证编号
                trs2[4].find_all('td')[2].getText().strip(): get_num(trs2[4].find_all('td')[3].getText().strip()),  # 批准面积
                trs2[5].find_all('td')[0].getText().strip(): trs2[5].find_all('td')[1].getText().strip(),  # 土地出让合同
                trs2[5].find_all('td')[2].getText().strip(): trs2[5].find_all('td')[3].getText().strip(),  # 批准日期
                trs2[6].find_all('td')[0].getText().strip().replace('\n', '').replace('\r', '').replace(' ', ''): trs2[6].find_all('td')[1].getText().strip()  # 发证日期（开始销售日期）
            }

            tempB = {}
            for j in range(7, 15):
                if trs2[j].find_all('td')[1].getText().strip() == "--":
                    continue
                tempB.update(tempA, **{
                    trs2[j].find_all('td')[1].getText().strip() + trs2[j].find_all('td')[2].getText().strip(): get_num(trs2[j].find_all('td')[3].getText().strip()),  # 面积
                    trs2[j].find_all('td')[1].getText().strip() + trs2[j].find_all('td')[4].getText().strip(): trs2[j].find_all('td')[5].getText().strip()   # 套数
                })

            res.append(tempB)

            irow += 1
            print(irow)


if __name__ == '__main__':
    # main()  # 抓取为json格式，并存储为文件
    json2csv()  # 将json格式文件转换为csv格式文件
