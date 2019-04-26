# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-17 19:56:32
"""
import copy
import traceback
import time
from urllib.parse import urljoin
from cdspider.handler import BaseHandler
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser
from cdspider.libs import utils
from cdspider.database.base import *
from cdspider.libs.constants import *


class GeneralHandler(BaseHandler):
    """
    general handler
    """

    def route(self, handler_driver_name, frequency, save):
        """
        route
        :param handler_driver_name:
        :param frequency:
        :param save:
        :return:
        """
        yield None


    def get_scripts(self):
        """
        获取列表规则中的自定义脚本
        :return: 自定义脚本
        """
        try:
            if "uuid" in self.task and self.task['uuid']:
                task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.mode)
                if not task:
                    raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
                self.task.update(task)
            rule = self.match_rule({})
            return rule.get("scripts", None)
        except CDSpiderError:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        爬取流程格式：self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        :param save: 传递的上下文
        """
        rule = self.match_rule(save)
        self.process = rule

    def match_rule(self, save):
        """
        初始化抓取流程
        :return:
        """
        return {
            "request": self.DEFAULT_PROCESS,
            "parse": None,
            "page": None
        }


    def run_parse(self, rule, save={}):
        """
        根据解析规则解析源码，获取相应数据
        爬虫结果 self.response {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        解析结果 self.response {"parsed": 解析结果}
        :param rule 解析规则
        """
        parser = ListParser(source=self.response['content'], ruleset=copy.deepcopy(rule),
                            log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        self.debug("%s parsed: %s" % (self.__class__.__name__, parsed))
        if parsed:
            self.response['parsed'] = self.build_url_by_rule(parsed, self.response['final_url'])

    def run_result(self, save):
        """
        爬虫结果处理
        爬虫结果 self.response {"parsed": 解析结果, "final_url": 请求的url}
        :param save 保存的上下文信息
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['url']
        self.crawl_info['crawl_count']['page'] += 1
        ctime = self.crawl_id
        new_count = self.crawl_info['crawl_count']['new_count']

        for item in self.response['parsed']:
            self.crawl_info['crawl_count']['total'] += 1
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
            else:
                # 生成文章唯一索引并判断文章是否已经存在
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            if inserted:
                mode_list = self.extension("mode_handle", {"save": save, "url": result['url']})
                mode = HANDLER_MODE_DEFAULT_ITEM
                if mode_list:
                    for i in mode_list:
                        if i is not None:
                            mode = i
                            break

                crawlinfo = self.build_crawl_info(self.response['final_url'], mode)
                typeinfo = utils.typeinfo(item['url'])
                result = self.build_result_info(
                    final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                else:
                    result_id = self.db['ArticlesDB'].insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.build_item_task(result_id, mode)
                self.crawl_info['crawl_count']['new_count'] += 1
            else:
                self.crawl_info['crawl_count']['repeat_count'] += 1
        if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
            self.crawl_info['crawl_count']['repeat_page'] += 1
            self.on_repetition(save)

    def build_crawl_info(self, final_url, mode):
        """
        构造文章数据的爬虫信息
        :param final_url: 请求的url
        :param mode: mode
        """
        return {
            'listMode': self.mode,
            'mode': mode,
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

    def build_result_info(self, **kwargs):
        """
        构造文章数据
        :param result: 解析到的文章信息 {"title": 标题, "author": 作者, "pubtime": 发布时间, "content": 内容}
        :param final_url: 请求的url
        :param typeinfo: 域名信息 {'domain': 一级域名, 'subdomain': 子域名}
        :param crawlinfo: 爬虫信息
        :param unid: 文章唯一索引
        :param ctime: 抓取时间
        :param status: 状态
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
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER)),
            'author': result.pop('author', None),
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'result': result,
            'crawlinfo': kwargs.get('crawlinfo'),
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
        }
        return r

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
            if 'url' not in item or not item['url']:
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

    def build_item_task(self, rid, mode):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': mode,
            'rid': rid,
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER))
        }
        self.queue[QUEUE_NAME_SCHEDULER_TO_SPIDER].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(GeneralHandler, self).finish(save)
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

