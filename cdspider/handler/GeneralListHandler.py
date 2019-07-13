# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 20:45:56
"""
import copy
import time
from cdspider.handler import GeneralHandler
from cdspider.database.base import *
from cdspider.libs.constants import *


class GeneralListHandler(GeneralHandler):
    """
    general list handler
    task 爬虫任务信息 {"mode": "list", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "list", "url": url, "listRule": 列表规则，参考列表规则}
    支持注册的插件:
        list_handler.mode_handle  匹配详情页的mode
            data参数为 {"save": save,"url": url}
    """

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"uid": url uuid, "mode": handler mode}]
        """
        uid = message['uid']
        if not isinstance(uid, (list, tuple)):
            uid = [uid]
        rules = {}
        for each in uid:
            tasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"uid": each})
            if len(list(tasks)) > 0:
                continue
            urls = self.db['UrlsDB'].get_detail(each)
            if not urls:
                raise CDSpiderDBDataNotFound("url: %s not found" % each)
            ruleId = urls.pop('ruleId', 0)
            if str(ruleId) in rules:
                rule = rules[str(ruleId)]
            else:
                rule = self.db['ListRuleDB'].get_detail(ruleId)
                if not rule:
                    rule = {}
                rules[str(ruleId)] = rule

            task = {
                'mode': message['mode'],     # handler mode
                'pid': urls['pid'],          # project uuid
                'sid': urls['sid'],          # site uuid
                'tid': urls.get('tid', 0),   # task uuid
                'uid': each,                 # url uuid
                'kid': 0,                    # keyword id
                'frequency': str(rule.get('frequency', self.DEFAULT_FREQUENCY)),
                'status': urls.get("status", SpiderTaskDB.STATUS_INIT),
                'expire': int(time.time()) + int(rule.get('frequency', 0)) if int(rule.get('frequency', 0)) > 0 else 0,
                'url': urls['url'],          # url
            }
            self.debug("%s newtask: %s" % (self.__class__.__name__, str(task)))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.db['SpiderTaskDB'].insert(task)

    def match_rule(self, save):
        """
        获取匹配的规则
        :param save: 传递的上下文
        """
        if "rule" in self.task and self.task['rule']:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            rule = self.db['ListRuleDB'].get_detail(int(self.task['rule']))
            if not rule:
                raise CDSpiderDBDataNotFound("rule: %s not exists" % self.task['rule'])
        else:
            urls = self.db['UrlsDB'].get_detail(self.task['uid'])
            if not urls:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("urls: %s not exists" % self.task['uid'])
            if urls['status'] != UrlsDB.STATUS_ACTIVE or urls['ruleStatus'] != UrlsDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderHandlerError("url not active")
            if 'jsonUrl' in urls and urls['jsonUrl']:
                self.task['url'] = urls['jsonUrl']
            save['base_url'] = urls['url']
            self.task['urls'] = urls
            if 'ruleId' not in urls or not urls['ruleId']:
                raise CDSpiderHandlerError("url not has list rule")
            rule = self.db['ListRuleDB'].get_detail(urls['ruleId'])
            if not rule:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("rule: %s not exists" % urls['ruleId'])
            if rule and rule['status'] != ListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("list rule not active")

        return rule

    def build_item_task(self, rid, mode, save):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': mode,
            'rid': rid,
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER))
        }
        self.debug("%s new item task: %s" % (self.__class__.__name__, message))
        self.queue[QUEUE_NAME_SCHEDULER_TO_SPIDER].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(GeneralListHandler, self).finish(save)
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
        self.db['SpiderTaskDB'].update(
            self.task['uuid'], self.mode,
            {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": s})
