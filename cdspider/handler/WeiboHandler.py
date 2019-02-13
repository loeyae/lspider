#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-16 14:34:28
"""
import copy
import time
from . import BaseHandler
from .traite import NewAttachmentTask
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class WeiboHandler(BaseHandler, NewAttachmentTask):
    """
    weibo handler
    :property task 爬虫任务信息 {"mode": "comment", "uuid": SpiderTask.weibo uuid}
                   当测试该handler，数据应为 {"mode": "weibo", "url": url, "authorListRule": 评论规则，参考评论规则}
    """

    def get_scripts(self):
        """
        获取自定义脚本
        """
        try:
            if "uuid" in self.task and self.task['uuid']:
                task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.task['mode'])
                if not task:
                    raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
                self.task.update(task)
            rule = self.match_rule({}) or {}
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        self.process = self.match_rule(save)

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "authorListRule" in self.task:
            parse_rule = self.task.get("authorListRule")
            author = copy.deepcopy(self.task['author'])
            if not author:
                raise CDSpiderError("author not exists")
        else:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            author = self.db['WeiboAuthorDB'].get_detail(self.task['uid'])
            if not author:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("author: %s not exists" % self.task['uid'])
            if author['status'] != WeiboAuthorDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                raise CDSpiderHandlerError("author: %s not active" % self.task['uid'])
            parse_rule = self.db['AuthorListRuleDB'].get_detail_by_tid(author['tid'])
            if not parse_rule:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("author rule by tid: %s not exists" % author['tid'])
            if parse_rule['status'] != AuthorListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("author rule: %s not active" % rule['uuid'])
        if 'hard_code' in save:
            del save['hard_code']
        save['request'] = {
            "hard_code": [{
                "mode": "format",
                "name": "uid",
                "value": author['uid'],
            }],
        }
        save['paging'] = True
        self.task['mediaType'] = MEDIA_TYPE_WEIBO
        self.task['url'] = parse_rule['baseUrl']
        save['base_url'] = parse_rule['baseUrl']
        return parse_rule

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"uid": url uuid, "mode": handler mode}]
        """
        uid = message['uid']
        if not isinstance(uid, (list, tuple)):
            uid = [uid]
        for each in uid:
            spiderTasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"uid": each})
            if len(list(spiderTasks)) > 0:
                continue
            author = self.db['WeiboAuthorDB'].get_detail(each)
            if not author:
                raise CDSpiderDBDataNotFound("author: %s not found" % each)
            task = {
                'mode': message['mode'],     # handler mode
                'pid': author['pid'],        # project uuid
                'sid': author['sid'],        # site uuid
                'tid': author['tid'],        # task uuid
                'uid': each,                 # url uuid
                'kid': 0,                    # keyword id
                'url': "base_url",           # url
                'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            }
            self.debug("%s newtask: %s" % (self.__class__.__name__, str(task)))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.db['SpiderTaskDB'].insert(task)

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
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
            'stid': self.task['uuid'],                      # spider task id
            'kid': self.task['kid'],                        # url id
            'listRule': self.process['uuid'],               # authorListRule id
            'list_url': self.request_params['url'],         # 列表url
        }
        result['mediaType'] = MEDIA_TYPE_WEIBO
        result['acid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WeiboHandler, self).finish(save)
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
