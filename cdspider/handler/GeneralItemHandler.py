# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-28 21:45:37
"""
import time
import copy
from . import BaseHandler, HandlerUtils
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ItemParser
from cdspider.parser.lib import TimeParser


class GeneralItemHandler(BaseHandler):
    """
    general item handler
    task 爬虫任务信息 {"mode": "item", "rid": Article rid}
                      当测试该handler，数据应为 {"mode": "item", "url": url, "detailRule": 详情规则，参考详情规则}
    支持注册的插件:
        item_handler.result_handle
            data参数为 {"save": save, "domain": domain, "subdomain": subdomain}

    """

    def route(self, handler_driver_name, rate, save):
        """
        route
        :param handler_driver_name:
        :param rate:
        :param save:
        :return:
        """
        yield None

    def get_scripts(self):
        """
        获取自定义脚本
        :return:
        """
        try:
            rule = self.match_rule()
            return rule.get("scripts", None)
        except CDSpiderError:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程 初始化后的数据self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则}
        :param save: 传递的上下文
        :return:
        """
        if "detailRule" in self.task:
            typeinfo = utils.typeinfo(self.task['url'])
            if typeinfo['domain'] != self.task['detailRule']['domain']\
                    or (self.task['detailRule']['subdomain']
                        and typeinfo['subdomain'] != self.task['detailRule']['subdomain']):
                raise CDSpiderNotUrlMatched()
            if 'urlPattern' in self.task['detailRule'] and self.task['detailRule']['urlPattern']:
                '''
                如果规则中存在url匹配规则，则进行url匹配规则验证
                '''
                u = utils.preg(self.task['url'], self.task['detailRule']['urlPattern'])
                if not u:
                    raise CDSpiderNotUrlMatched()
        self.process = self.match_rule()
        if 'paging' in self.process and self.process['paging']:
            self.process['paging']['url'] = 'base_url'

    def match_rule(self):
        """
        匹配详情页规则
        :return:
        """
        # 优先获取task中详情规则
        parse_rule = self.task.get("detailRule", {})
        # 根据task中的rid获取文章信息
        rid = self.task.get('rid', None)
        if rid:
            article = self.db['ArticlesDB'].get_detail(rid, select=['mediaType', 'url', 'crawlinfo', 'title',
                                                                    'author', 'channel', 'pubtime'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % rid)
            self.task.setdefault('mediaType', article.get('mediaType', MEDIA_TYPE_OTHER))
            if 'ulr' not in self.task or not self.task['url']:
                self.task["url"] = article['url']
            self.task.setdefault('crawlinfo', article.get('crawlinfo', {}))
            self.task['article'] = article
        else:
            self.task.setdefault('crawlinfo', {})
        url = self.task.get("url")
        if not url:
            raise CDSpiderHandlerError("url not exists")
        if not parse_rule:
            '''
            task中不存在详情规则，根据域名匹配规则库中的规则
            '''
            subdomain, domain = utils.domain_info(url)
            if subdomain:
                '''
                优先获取子域名对应的规则
                '''
                parserule_list = self.db['ParseRuleDB'].get_list_by_subdomain(subdomain)
                for item in parserule_list:
                    if not parse_rule:
                        '''
                        将第一条规则选择为返回的默认值
                        '''
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        '''
                        如果规则中存在url匹配规则，则进行url匹配规则验证
                        '''
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
            else:
                '''
                获取域名对应的规则
                '''
                parserule_list = self.db['ParseRuleDB'].get_list_by_domain(domain)
                for item in parserule_list:
                    if not parse_rule:
                        '''
                        将第一条规则选择为返回的默认值
                        '''
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        '''
                        如果规则中存在url匹配规则，则进行url匹配规则验证
                        '''
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
        return parse_rule

    def run_parse(self, rule):
        """
        解析
        根据 self.response {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        获取self.response {"parsed": 解析结果}
        :param rule: 解析规则
        :return:
        """
        parser = ItemParser(source=self.response['content'], ruleset=copy.deepcopy(rule),
                            log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        """
        构造爬虫log信息
        :param final_url: 请求的url
        :return:
        """
        self.task['crawlinfo']['mode'] = self.mode
        if 'final_url' not in self.task['crawlinfo']:
            self.task['crawlinfo']['final_url'] = {str(self.page): final_url}
        else:
            self.task['crawlinfo']['final_url'][str(self.page)] = final_url
        if 'detailRule' not in self.task['crawlinfo']:
            self.task['crawlinfo']['detailRule'] = self.process.get('uuid', 0)
        self.task['crawlinfo']['page'] = self.page

    def _build_result_info(self, **kwargs):
        """
        构造文章数据
        :param result: 解析到的文章信息 {"title": 标题, "author": 作者, "pubtime": 发布时间, "content": 内容}
        :param final_url: 请求的url
        :param typeinfo: 域名信息 {'domain': 一级域名, 'subdomain': 子域名}
        :param crawlinfo: 爬虫信息
        :param unid: 文章唯一索引
        :param ctime: 抓取时间
        :param status: 状态
        :return:
        """
        now = int(time.time())
        result = kwargs.pop('result')
        item = self.task.get('article', {}) or {}
        # 格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            "status": kwargs.get('status', ArticlesDB.STATUS_ACTIVE),
            'url': kwargs['final_url'],
            'mediaType': self.process.get('mediaType', self.task.get('mediaType', MEDIA_TYPE_OTHER)),
            'title': item.get('title', None) or result.pop('title', None),              # 标题
            'author': result.pop('author', None) or item.get('author', None),      # 作者
            'content': result.pop('content', None),
            'pubtime': pubtime or item.get('pubtime', None),          # 发布时间
            'channel': result.pop('channel', None)  or item.get('channel', None),       # 频道信息
            'crawlinfo': kwargs.get('crawlinfo')
        }

        if all((r['title'], r['author'], r['content'], r['pubtime'])):
            '''
            判断文章是否解析完全
            '''
            r['status'] = ArticlesDB.STATUS_PARSED
        if "unid" in kwargs:
            r['acid'] = kwargs['unid']                                         # unique str
        if "ctime" in kwargs:
            r['ctime'] = kwargs['ctime']
        if "typeinfo" in kwargs:
            r['domain'] = kwargs.get("typeinfo", {}).get('domain', None)          # 站点域名
            r['subdomain'] = kwargs.get("typeinfo", {}).get('subdomain', None)    # 站点域名
        return r

    def run_result(self, save):
        """
        爬虫结果处理
        :param save: 传递的上下文
        :return:
        """
        self._build_crawl_info(final_url=self.response['final_url'])
        if self.response['parsed']:
            typeinfo = utils.typeinfo(self.response['final_url'])
            self.result2db(save, copy.deepcopy(typeinfo))
            self.extension("result_handle", {"save": save, **typeinfo})

    def result2db(self, save, typeinfo):
        """
        详情解析结果入库
        :param save: 传递的上下文
        :param typeinfo: 域名信息
        :return:
        """
        result_id = self.task.get("rid", None)
        if not result_id:
            '''
            如果任务中没有文章id，则生成文章唯一索引，并判断是否已经存在。
            如果文章已经存在，则修改原数据，如果不存在，则新增数据
            '''
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                inserted, unid = (True, {"acid": "testing_mode", "ctime": self.crawl_id})
                self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
            else:
                # 生成文章唯一索引并验证是否已存在
                inserted, unid = self.db['UniqueDB'].insert(
                    self.get_unique_setting(self.response['final_url'], self.response['parsed']), self.crawl_id)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            # 格式化文章信息
            result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo,
                                             result=self.response['parsed'], crawlinfo=self.task['crawlinfo'], **unid)
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                result_id = 'testing_mode'
                self.debug("%s on_result: %s" % (self.__class__.__name__, result))
            else:
                self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                if inserted:
                    result_id = self.db['ArticlesDB'].insert(result)
                    HandlerUtils.send_result_into_queue(
                        self.queue, self.ctx.obj.get('app_config'), self.mode, result_id)
                    self.task['crawlinfo'] = result['crawlinfo']
                else:
                    item = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                    self.task['crawlinfo'] = item['crawlinfo']
                    result_id = item['rid']
                    self.db['ArticlesDB'].update(result_id, result)
                    HandlerUtils.send_result_into_queue(
                        self.queue, self.ctx.obj.get('app_config'), self.mode, result_id)
            self.task['rid'] = result_id
        else:
            if self.page == 1:
                '''
                对于已存在的文章，如果是第一页，则更新所有解析到的内容
                否则只追加content的内容
                '''
                # 格式化文章信息
                result = self._build_result_info(
                    final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'],
                    crawlinfo=self.task['crawlinfo'])

                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s on_result: %s" % (self.__class__.__name__, result))
                else:
                    self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                    self.db['ArticlesDB'].update(result_id, result)
                    HandlerUtils.send_result_into_queue(
                        self.queue, self.ctx.obj.get('app_config'), self.mode, result_id)
            else:
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s on_result: %s" % (self.__class__.__name__, self.response['parsed']))
                else:
                    result = self.db['ArticlesDB'].get_detail(result_id)
                    content = result['content']
                    if 'content' in self.response['parsed'] and self.response['parsed']['content']:
                        content = '%s\r\n\r\n%s' % (content, self.response['parsed']['content'])
                        self.debug("%s on_result content: %s" % (self.__class__.__name__, content))
                        self.db['ArticlesDB'].update(result_id, {"content": content})
                        HandlerUtils.send_result_into_queue(
                            self.queue, self.ctx.obj.get('app_config'), self.mode, result_id)
        if not result_id:
            raise CDSpiderDBError("Result insert failed")

    def finish(self, save):
        """
        怕却结束 记录抓取日志
        :param save: 传递的上下文
        :return:
        """
        super(GeneralItemHandler, self).finish(save)
        if self.task.get('rid') and self.task.get('crawlinfo') and not self.testing_mode:
            self.db['ArticlesDB'].update(self.task['rid'], {"crawlinfo": self.task['crawlinfo']})

