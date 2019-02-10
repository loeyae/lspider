#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 20:45:56
"""
import copy
import time
import traceback
from . import BaseHandler
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class GeneralListHandler(BaseHandler):
    """
    general list handler
    :property task 爬虫任务信息 {"mode": "list", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "list", "url": url, "listRule": 列表规则，参考列表规则}
    """

    BBS_TYPES = (MEDIA_TYPE_BBS, MEDIA_TYPE_ASK)

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"uid": url uuid, "mode": handler mode}]
        """
        uid = message['uid']
        if not isinstance(uid, (list, tuple)):
            uid = [uid]
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
                'uid': each,                  # url uuid
                'kid': 0,                    # keyword id
                'frequency': str(rule.get('rate', self.DEFAULT_RATE)),
                'url': urls['url'],          # url
            }
            self.debug("%s newtask: %s" % (self.__class__.__name__, str(task)))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.db['SpiderTaskDB'].insert(task)

    def get_scripts(self):
        """
        获取列表规则中的自定义脚本
        :return 自定义脚本
        """
        try:
            if "uuid" in self.task and self.task['uuid']:
                task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.task['mode'])
                if not task:
                    raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
                self.task.update(task)
            rule = self.match_rule({})
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        rule = self.match_rule(save)
        self.process =  rule

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        rule = {}
        if "listRule" in self.task and self.task['listRule']:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            rule = copy.deepcopy(self.task['listRule'])
        else:
            urls = self.db['UrlsDB'].get_detail(self.task['uid'])
            if not urls:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("urls: %s not exists" % self.task['uid'])
            if urls['status'] != UrlsDB.STATUS_ACTIVE or urls['ruleStatus'] != UrlsDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                raise CDSpiderHandlerError("url not active")
            save['base_url'] = urls['url']
            self.task['urls'] = urls
            if not 'ruleId' in urls or not urls['ruleId']:
                raise CDSpiderHandlerError("url not has list rule")
            rule = self.db['ListRuleDB'].get_detail(urls['ruleId'])
            if not rule:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("rule: %s not exists" % urls['ruleId'])
            if rule and rule['status'] != ListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("list rule not active")
        if rule and 'jsonUrl' in rule and rule['jsonUrl']:
            self.task['url'] = rule['jsonUrl']
            save['base_url'] = self.task['url']

        return rule

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        """
        构造文章数据的爬虫信息
        :param final_url 请求的url
        :input self.task 爬虫任务信息
        :input self.crawl_id 爬虫运行时刻
        """
        mediaType = self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER))
        return {
                'listMode': self.task['mode'],
                'mode': HANDLER_MODE_BBS_ITEM if mediaType in self.BBS_TYPES else HANDLER_MODE_DEFAULT_ITEM,
                "stid": self.task.get("uuid", 0),   # SpiderTask uuid
                "uid": self.task.get("uid", 0),     # url id
                "pid": self.task.get('pid', 0),     # project id
                "sid": self.task.get('sid', 0),     # site id
                "tid": self.task.get('tid', 0),     # task id
                "kid": self.task.get('kid', 0),     # keyword id
                "listRule": self.process.get('uuid', 0),   # 规则ID
                "list_url": final_url,              # 列表url
                "list_crawl_id": self.crawl_id,     # 列表抓取时间
        }

    def _build_result_info(self, **kwargs):
        """
        构造文章数据
        :param result 解析到的文章信息 {"title": 标题, "author": 作者, "pubtime": 发布时间, "content": 内容}
        :param final_url 请求的url
        :param typeinfo 域名信息 {'domain': 一级域名, 'subdomain': 子域名}
        :param crawlinfo 爬虫信息
        :param unid 文章唯一索引
        :param ctime 抓取时间
        :param status 状态
        :input self.crawl_id 爬取时刻
        """
        now = int(time.time())
        result = kwargs.get('result', {})
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            "status": kwargs.get('status', ArticlesDB.STATUS_INIT),
            'url': kwargs['final_url'],
            'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
            'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER)),
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'result': result,
            'crawlinfo': kwargs.get('crawlinfo'),
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
            }
        return r

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        ctime = self.crawl_id
        new_count = self.crawl_info['crawl_count']['new_count']
        #格式化url
        formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
        for item in formated:
            self.crawl_info['crawl_count']['total'] += 1
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
            else:
                #生成文章唯一索引并判断文章是否已经存在
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            if inserted:
                crawlinfo =  self._build_crawl_info(self.response['final_url'])
                typeinfo = utils.typeinfo(item['url'])
                result = self._build_result_info(final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                else:
                    result_id = self.db['ArticlesDB'].insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.build_item_task(result_id)
                self.crawl_info['crawl_count']['new_count'] += 1
            else:
                self.crawl_info['crawl_count']['repeat_count'] += 1
        if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
            self.crawl_info['crawl_count']['repeat_page'] += 1
            self.on_repetition(save)

    def url_prepare(self, url):
        return url

    def build_url_by_rule(self, data, base_url = None):
        """
        根据url规则格式化url
        :param data 解析到的数据
        :param base_url 基本url
        """
        if not base_url:
            base_url = self.task.get('url')
        urlrule = self.process.get("url")
        formated = []
        for item in data:
            if not 'url' in item or not item['url']:
                continue
            if item['url'].startswith('javascript') or item['url'] == '/':
                continue
            try:
                item['url'] = self.url_prepare(item['url'])
            except:
                self.error(traceback.format_exc())
                continue
            if urlrule and 'name' in urlrule and urlrule['name']:
                parsed = {urlrule['name']: item['url']}
                item['url'] = utils.build_url_by_rule(urlrule, parsed)
            else:
                item['url'] = urljoin(base_url, item['url'])
            formated.append(item)
        return formated


    def build_item_task(self, rid):
        """
        生成详情抓取任务并入队
        """
        mediaType = self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER))
        message = {
            'mode': HANDLER_MODE_BBS_ITEM if mediaType in self.BBS_TYPES else HANDLER_MODE_DEFAULT_ITEM,
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
        self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": s})
