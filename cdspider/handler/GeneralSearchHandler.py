# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-22 10:28:27
"""
import re
import copy
import time
import urllib.request
from urllib.parse import urlparse, quote_plus
from cdspider.handler import GeneralHandler
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *


class GeneralSearchHandler(GeneralHandler):
    """
    general search handler
    task 爬虫任务信息 {"mode": "search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "search", "keyword": 关键词规则,
                   "authorListRule": 列表规则，参考列表规则}

    支持注册的插件:
        search_handler.mode_handle
            data参数为 {"save": save,"url": url}
    """

    def newtask(self, message):
        """
        新建爬虫任务
        :param message: [{"tid": task id, "kid": keyword uuid, "mode": handler mode}]
        """
        if 'tid' in message and message['tid']:
            tid = message['tid']
            if not isinstance(tid, (list, tuple)):
                tid = [tid]
            for each in tid:
                tasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"tid": each})
                if len(list(tasks)) > 0:
                    continue
                task = self.db['TaskDB'].get_detail(each)
                if not task:
                    raise CDSpiderDBDataNotFound("task: %s not found" % each)
                uuid = 0
                while True:
                    has_word = False
                    for item in self.db['KeywordsDB'].get_new_list(uuid, select=['uuid']):
                        t = {
                            'mode': message['mode'],     # handler mode
                            'pid': task['pid'],          # project uuid
                            'sid': task['sid'],          # site uuid
                            'tid': each,                 # task uuid
                            'uid': 0,                    # url uuid
                            'kid': item['uuid'],         # keyword id
                            'url': 'base_url',           # url
                            'frequency': str(task.get('frequency', self.DEFAULT_FREQUENCY)),
                            'status': SpiderTaskDB.STATUS_ACTIVE
                        }
                        self.debug("%s newtask: %s" % (self.__class__.__name__, str(t)))
                        if not self.testing_mode:
                            '''
                            testing_mode打开时，数据不入库
                            '''
                            self.db['SpiderTaskDB'].insert(t)
                        uuid = item['uuid']
                        has_word = True
                    if not has_word:
                        break
        else:
            kid = message['kid']
            if not isinstance(kid, (list, tuple)):
                kid = [kid]
            for each in kid:
                word = self.db['KeywordsDB'].get_detail(each)
                if not word:
                    raise CDSpiderDBDataNotFound("word: %s not found" % each)
                uuid = 0
                while True:
                    has_word = False
                    for item in self.db['TaskDB'].get_new_list(
                            uuid, where={"type": TASK_TYPE_SEARCH},
                            select=['uuid', 'pid', 'sid', 'mediaType', 'searchType']):
                        mode = HANDLER_MODE_DEFAULT_SEARCH
                        tasks = self.db['SpiderTaskDB'].get_list(mode, {"tid": item['uuid'], "kid": each})
                        if len(list(tasks)) > 0:
                            continue
                        t = {
                            'mode': mode,     # handler mode
                            'pid': item['pid'],          # project uuid
                            'sid': item['sid'],          # site uuid
                            'tid': item['uuid'],         # task uuid
                            'uid': 0,                    # url uuid
                            'kid': each,                 # keyword id
                            'frequency': str(item.get('frequency', self.DEFAULT_FREQUENCY)),
                            'url': 'base_url',           # url
                            'status': SpiderTaskDB.STATUS_ACTIVE
                        }
                        self.debug("%s newtask: %s" % (self.__class__.__name__, str(t)))
                        if not self.testing_mode:
                            '''
                            testing_mode打开时，数据不入库
                            '''
                            self.db['SpiderTaskDB'].insert(t)
                        uuid = item['uuid']
                        has_word = True
                    if not has_word:
                        break

    def match_rule(self, save):
        """
        获取匹配的规则
        :param save: 传递的上下文
        """
        if "listRule" in self.task:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            rule = copy.deepcopy(self.task['listRule'])
            keyword = copy.deepcopy(self.task['keyword'])
            if not keyword:
                raise CDSpiderError("keyword not exists")
        else:
            keyword = self.db['KeywordsDB'].get_detail(self.task['kid'])
            if not keyword:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("keyword: %s not exists" % self.task['kid'])
            if keyword['status'] != KeywordsDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderHandlerError("keyword: %s not active" % self.task['kid'])
            rule = self.db['ListRuleDB'].get_detail_by_tid(self.task['tid'])
            if not rule:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("task rule by tid: %s not exists" % self.task['tid'])
            if rule['status'] != ListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("author rule: %s not active" % rule['uuid'])
        kset = rule['request'].pop('keyword', {})
        if 'hard_code' in save:
            del save['hard_code']
        mode = kset.pop('mode', 'format')
        save['request'] = {
            "hard_code": [{
                "mode": mode,
                "name": kset.pop('key', 'keyword'),
                "value": quote_plus(keyword['name']) if mode != 'post' else keyword['name'],
            }],
        }
        now = int(time.time())
        params = {"lastmonth": now - 30 * 86400, "lastweek": now - 7 * 86400,
                  "yesterday": now - 86400, "lasthour": now - 36000, "now": now}
        self.task['url'] = utils.build_url_by_rule({"base": rule['baseUrl'], "mode": "format"}, params)
        save['base_url'] = self.task['url']
        save['paging'] = True
        return rule

    def url_prepare(self, url):
        """
        获取真正的url
        """
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = urllib.request.Request(url = url, headers = headers, method = 'GET')
        response = urllib.request.urlopen(req)
        furl = response.geturl()
        if urlparse(furl).netloc != urlparse(url).netloc:
            return furl
        else:
            content = response.read()
            urllist = re.findall(b'window\.location\.replace\("([^"]+)"\)', content)
            if urllist:
                return urllist[0].decode()
        return furl
