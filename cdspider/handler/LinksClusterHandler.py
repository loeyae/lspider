#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-17 19:56:32
"""
import re
import sys
import hashlib
import math
import copy
from . import BaseHandler
from cdspider.parser.lib import LinksExtractor
from urllib.parse import urlparse

class LinksClusterHandler(BaseHandler):
    """
    general handler
    """

    def route(self, mode, rate, save):
        yield None

    def init_process(self, save):
        if "crawler" in self.task and self.task["crawler"] == "selenium":
            self.process =  {
                "request": {
                    'crawler': self.task['crawler'],
                    'method': "open",
                    'proxy': "auto"
                },
                "parse": None,
                "page": None,
            }
        else:
            self.process =  {
                "request": self.DEFAULT_PROCESS,
                "parse": None,
                "page": None,
            }

    def newtask(self, message):
        """
        接收任务并重新分组
        """
        urlsdb = self.db['UrlsDB']
        where = {'tid': message['tid'], 'tier' : message['tier']}
        hits = 50
        count = urlsdb.get_count(where = where)
        if count > 0:
            arrTmp = []
            for i in range(math.ceil(count / hits)):
                offset = 0 if i == 0 else i * hits
                ret = urlsdb.get_list(where = where, select={'uuid': True, 'url': True, 'title': True}, offset = offset, hits = hits)
                for i in list(ret):
                    arrTmp.append(i)

            arrUuid  = {}
            arrUrl   = []
            outarr   = {}
            sortArr  = []
            for item in arrTmp:
                arrUrl.append(item['url'])

            for url in arrUrl:
                queryArr = []
                # 解析 path query
                urlInfo  = urlparse(url)
                urlpath  = urlInfo.path
                urlquery = urlInfo.query
                # 分割并计算值
                urlpath_ = re.split(r'/|\.',urlpath)
                paths    = [tok.lower() for tok in urlpath_ if len(tok) > 0]
                urlquery_= re.split(r'&',urlquery)
                querys   = [tok.lower() for tok in urlquery_ if len(tok) > 0]
                for q in querys:
                    queryArr.append(q.split('='))

                pnum = len(paths) if len(paths) <= 9 else 9
                qnum = len(queryArr) if len(queryArr) <= 9 else 9
                # 以url为key num为值
                outarr[url] = str(pnum) + str(qnum)
            # 按值分堆
            arrtmp = sorted(outarr.items(), key=lambda d:d[1], reverse = True)
            # 开始对堆进行排序
            n = arrtmp[0][1]
            arr = {}
            for i in range(len(arrtmp)):
                if n == arrtmp[i][1]:
                    arr[arrtmp[i][0]] = arrtmp[i][1]
                else:
                    # 找到一堆，排序，暂存
                    sortTmp = sorted(arr.items(), key=lambda d:d[0], reverse = True)
                    sortArr.append(sortTmp)
                    # 重新再来
                    arr = {}
                    n = arrtmp[i][1]
                    arr[arrtmp[i][0]] = arrtmp[i][1]


            # 循环退出，arr还有数据，排序，暂存
            sortTmp = sorted(arr.items(), key=lambda d:d[0], reverse = True)
            sortArr.append(sortTmp)
            for item in arrTmp:
                arrUuid[item['url']] = item['uuid']

            # 更新数据
            if sortArr:
                for item in sortArr:
                    for it in item:
                        try:
                            urlsdb.update(id = arrUuid[it[0]], obj = {"cluster": it[1]})
                            print('update success!')
                        except Exception:
                            print('update error!')
        else:
            print('find no data!')

    def run_parse(self, rule):
        # 根据sid�站点域名
        sitedb = self.db['SitesDB']
        site = sitedb.get_site(self.task['sid'])
        extractor = LinksExtractor(url=site['url'])
        extractor.exctract(self.response['last_source'], errors = 'ignore')
        # if '://www.' in site['url']:
        #     re_type = 'domains'
        # else:
        #     re_type = 'subdomains'
        # domain 改成 subdomains试试
        re_type = 'subdomains'
        self.response['parsed'] = extractor.infos[re_type]


    def run_result(self, save):
        arrTmp   = self.response['parsed']
        # 处理爬虫触发不成功
        if not len(arrTmp):
            print('crawl error')
            exit()

        arrTitle = {}
        arrUrl   = []
        urlInfo  = []
        outarr   = {}
        sortArr  = []
        for item in arrTmp:
            arrUrl.append(item['url'])
        # print(arrUrl)
        for url in arrUrl:
            queryArr = []
            # 解析 path query
            urlInfo  = urlparse(url)
            urlpath  = urlInfo.path
            urlquery = urlInfo.query
            # 分割并计算值
            urlpath_ = re.split(r'/|\.',urlpath)
            paths    = [tok.lower() for tok in urlpath_ if len(tok) > 0]
            urlquery_= re.split(r'&',urlquery)
            querys   = [tok.lower() for tok in urlquery_ if len(tok) > 0]
            for q in querys:
                queryArr.append(q.split('='))

            pnum = len(paths) if len(paths)<=9 else 9
            qnum = len(queryArr) if len(queryArr)<=9 else 9
            # 以url为key num为值
            outarr[url] = str(pnum)+str(qnum)
        # 按值分堆
        arrtmp = sorted(outarr.items(), key=lambda d:d[1], reverse = True)

        # 开始对堆进行排序
        n = arrtmp[0][1]
        arr = {}
        for i in range(len(arrtmp)):
            if n == arrtmp[i][1]:
                arr[arrtmp[i][0]] = arrtmp[i][1]
            else:
                # 找到一堆，排序，暂存
                sortTmp = sorted(arr.items(), key=lambda d:d[0], reverse = True)
                sortArr.append(sortTmp)
                # 重新再来
                arr = {}
                n = arrtmp[i][1]
                arr[arrtmp[i][0]] = arrtmp[i][1]

        # 循环退出，arr还有数据，排序，暂存
        sortTmp = sorted(arr.items(), key=lambda d:d[0], reverse = True)
        sortArr.append(sortTmp)
        # 准备 title
        for item in arrTmp:
            arrTitle[item['url']] = item['title']
        # 准备写库
        if sortArr:
            urlsdb = self.db['UrlsDB']
            for item in sortArr:
                for it in item:
                    # 防止重复，去掉最后的/
                    #url = it[0].rstrip('/')
                    url = it[0]
                    try:
                        urlpath  = urlparse(url).path
                        urlquery = urlparse(url).query
                        if len(urlpath) <= 1 and len(urlquery) == 0:
                            if '.' + urlparse(self.task['url']).netloc in url:
                                baseUrl = 0
                            else:
                                baseUrl = 1
                        else:
                            baseUrl = 0
                        print(url, baseUrl)
                        urlsdb.insert({"url": url, "title": arrTitle[it[0]], "cluster": it[1], "pid": int(self.task['pid']), "sid": int(self.task['sid']), "tid": int(self.task['tid']), "tier": int(self.task['tier']), "baseUrl": baseUrl, 'ruleStatus': 0})
                        print('write success!')
                    except Exception:
                        print('url is exist!')
