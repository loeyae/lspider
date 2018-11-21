#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-17 19:56:32
"""
from . import BaseHandler
from cdspider.parser.lib import LinksExtractor
from urllib.parse import urlparse
import re
import hashlib

class LinksClusterHandler(BaseHandler):
    """
    general handler
    """

    def init_process(self):
        self.process =  {
            "request": self.DEFAULT_PROCESS,
            "parse": None,
            "page": None
        }

    def run_parse(self, rule):
        extractor = LinksExtractor(url=self.response['final_url'])
        extractor.exctract(self.response['last_source'])
        self.response['parsed'] = extractor.infos['subdomain']

    # def run_result(self, save):
    #     if self.response['parsed']:
    #         arrtmp = sorted(self.response['parsed'], key=lambda url:url['url'])
    #         for item in arrtmp:
    #             print(item)

    # def run_result(self, save):
    #     urlsdb = self.db['UrlsDB']
    #     for item in urlsdb.get_list('urls'):
    #         print(item)
        # pageRuledb = self.db['pageRule']
        # print(pageRuledb.find())

    def run_result(self, save):
        arrTmp   = self.response['parsed']
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
            urlsUniquedb = self.db['UrlsUniqueDB']
            for item in sortArr:
                for it in item:
                    urlmd5 = hashlib.md5(it[0].encode(encoding='UTF-8')).hexdigest()
                    try:
                        urlsUniquedb.insert({"urlmd5": urlmd5, "url": it[0]})
                        urlpath = urlparse(it[0]).path
                        if len(urlpath) <= 1:
                            baseUrl = 1
                        else:
                            baseUrl = 0
                        urlsdb.insert({"url": it[0], "title": arrTitle[it[0]], "cluster": it[1], "pid": self.task['pid'], "sid": self.task['sid'], "tid": self.task['tid'], "tier": self.task['tier'], "baseUrl": baseUrl})
                        print('write success!')
                    except Exception as e:
                        print('url is exist!')

        # urlsdb = self.db['UrlsDB']
        # if self.response['parsed']:
        #     arrtmp = sorted(self.response['parsed'], key=lambda url:url['url'])
        #     for item in arrtmp:
        #         print(item)
        #         urlsdb.insert({"url": item['url'], "title": item['title']})
