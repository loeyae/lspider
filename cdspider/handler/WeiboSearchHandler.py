#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-27 15:06:26
"""
import re
import copy
import time
import traceback
import urllib.request
from . import GeneralSearchHandler
from .traite import NewAttachmentTask
from urllib.parse import urljoin, urlparse, quote_plus
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser.lib import TimeParser

class WeiboSearchHandler(GeneralSearchHandler, NewAttachmentTask):
    """
    weibo search handler
    :property task 爬虫任务信息 {"mode": "weibo-search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "weibo-search", "keyword": 关键词规则, "authorListRule": 列表规则，参考列表规则}
    """

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.task['mediaType'] = MEDIA_TYPE_WEIBO
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']:
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            typeinfo = utils.typeinfo(self.response['final_url'])
            for each in self.response['parsed']:
                if not each['url']:
                    continue
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成唯一ID, 并判断是否已存在
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(each['url'], {}), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result = self.build_weibo_info(result=each, final_url=self.response['final_url'], **unid)
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                    if not self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        result_id = self.db['WeiboInfoDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                        self.build_sync_task(result_id, 'WeiboInfoDB')
                    self.result2attach(result['crawlinfo'], save, result_id, url=each['url'], **typeinfo)
                    self.db['WeiboInfoDB'].update(result_id, {"crawlinfo": result['crawlinfo']})
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def build_weibo_info(self, **kwargs):
        """
        构造评论数据
        """
        now = int(time.time())
        result = kwargs.pop('result')
        #格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        #爬虫信息记录
        result['pubtime'] = pubtime
        result['crawlinfo'] = {
            'listMode': self.task['mode'],
            'pid': self.task['pid'],                        # project id
            'sid': self.task['sid'],                        # site id
            'tid': self.task['tid'],                        # task id
            'uid': self.task['uid'],                        # url id
            'kid': self.task['kid'],                        # url id
            'listRle': self.process['uuid'],                # authorListRule id
            'list_url': self.task['url'],            # 列表url
        }
        result['mediaType'] = MEDIA_TYPE_WEIBO
        result['acid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result

    def url_prepare(self, url):
        return url

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WeiboSearchHandler, self).finish(save)
        crawlinfo = self.task.get('crawlinfo', {}) or {}
        self.crawl_info['crawl_end'] = int(time.time())
        crawlinfo[str(self.crawl_id)] = self.crawl_info
        crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
        if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
            del crawlinfo_sorted[0]
        s = self.task.get("save")
        if not s:
            s = {}
        s.update(save)
        self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": s})

    def build_sync_task(self, rid, db):
        """
        生成同步任务并入队
        """
        message = {'rid': rid, 'db': db}
        self.queue['article2kafka'].put_nowait(message)
