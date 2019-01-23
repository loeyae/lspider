#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-12 21:00:34
"""
import time
import copy
from . import BaseHandler
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from .traite import NewAttachmentTask
from cdspider.parser import ItemParser, CustomParser
from cdspider.parser.lib import TimeParser

class BbsItemHandler(BaseHandler, NewAttachmentTask):
    """
    general item handler
    :property task 爬虫任务信息 {"mode": "item", "rid": Article rid}
                   当测试该handler，数据应为 {"mode": "item", "url": url, "forumRule": 详情规则，参考详情规则}
    """

    def route(self, mode, save):
        """
        schedule 分发
        :param mode  project|site 分发模式: 按项目|按站点
        :param save 传递的上下文
        :return 包含uuid的迭代器，项目模式为项目的uuid，站点模式为站点的uuid
        :notice 该方法返回的迭代器用于router生成queue消息，以便plantask听取，消息格式为:
        {"mode": route mode, "h-mode": handler mode, "uuid": uuid}
        """
        if not "id" in save:
            save["id"] = 0
        if mode == ROUTER_MODE_PROJECT:
            for item in self.db['ProjectsDB'].get_new_list(save['id'], select=["uuid"]):
                if item['uuid'] > save['id']:
                    save['id'] = item["uuid"]
                yield item['uuid']
        elif mode == ROUTER_MODE_SITE:
            if not "pid" in save:
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                while True:
                    has_item = False
                    for each in self.db['SitesDB'].get_new_list(save['id'], item['uuid'], select=["uuid"]):
                        has_item = True
                        if each['uuid'] > save['id']:
                            save['id'] = each['uuid']
                        yield each['uuid']
                    if not has_item:
                        break
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']
        elif mode == ROUTER_MODE_TASK:
            '''
            按任务分发
            '''
            if not "pid" in save:
                '''
                初始化上下文中的pid参数,该参数用于项目数据查询
                '''
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                while True:
                    has_item = False
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid']}, select=["uuid"]):
                        has_item = True
                        if each['uuid'] > save['id']:
                            save['id'] = each['uuid']
                        yield each['uuid']
                    if not has_item:
                        break
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']

    def schedule(self, message, save):
        """
        根据router的queue消息，计划爬虫任务
        :param message route传递过来的消息
        :param save 传递的上下文
        :return 包含uuid, url的字典迭代器，为SpiderTaskDB中数据
        :notice 该方法返回的迭代器用于plantask生成queue消息，以便fetch听取，消息格式为
        {"mode": handler mode, "uuid": SpiderTask uuid, "url": SpiderTask url}
        """
        mode = message['mode']
        if not 'id' in save:
            save['id'] = 0
        if mode == ROUTER_MODE_PROJECT:
            if not 'tid' in save:
                save['tid'] = 0
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item']}):
                self.debug("%s schedule task: %s" % (self.__class__.__name__, str(item)))
                while True:
                    has_item = False
                    for each in self.schedule_by_task(item, message['h-mode'], save):
                        yield each
                        has_item = True
                    if not has_item:
                        self.debug("%s schedule task end" % (self.__class__.__name__))
                        break
                if item['uuid'] > save['tid']:
                    save['tid'] = item['uuid']
        elif mode == ROUTER_MODE_SITE:
            if not 'tid' in save:
                save['tid'] = 0
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"sid": message['item']}):
                self.debug("%s schedule task: %s" % (self.__class__.__name__, str(item)))
                while True:
                    has_item = False
                    for each in self.schedule_by_task(item, message['h-mode'], save):
                        yield each
                        has_item = True
                    if not has_item:
                        self.debug("%s schedule task end" % (self.__class__.__name__))
                        break
                if item['uuid'] > save['tid']:
                    save['tid'] = item['uuid']
        elif mode == ROUTER_MODE_TASK:
            task = self.db['TaskDB'].get_detail(message['item'])
            for each in self.schedule_by_task(task, message['h-mode'], save):
                yield each

    def schedule_by_task(self, task, mode, save):
        """
        获取站点下计划中的爬虫任务
        :param site 站点信息
        :param mode handler mode
        :param save 上下文参数
        :return 包含爬虫任务uuid, url的字典迭代器
        """
        rules = {}
        for item in self.db['SpiderTaskDB'].get_plan_list(mode, save['id'], plantime=save['now'], where={"tid": task['uuid'], "rid": {"$gt": 0}}, select=['uuid', 'url', 'rid']):
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                ruleId = item.pop('rid', 0)
                if str(ruleId) in rules:
                    rule = rules[str(ruleId)]
                else:
                    rule = self.db['ForumRuleDB'].get_detail(ruleId)
                    if rule:
                        rules[str(ruleId)] = rule
                if not rule:
                    continue
                frequency = str(rule.get('rate', self.DEFAULT_RATE))
                plantime = int(save['now']) + int(self.ratemap[frequency][0])
                self.db['SpiderTaskDB'].update(item['uuid'], mode, {"plantime": plantime, "frequency": frequency})
            if item['uuid'] > save['id']:
                save['id'] = item['uuid']
            yield item

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
            rule = self.match_rule() or {}
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        if 'forumRule' in self.task:
            typeinfo = utils.typeinfo(self.task['url'])
            if typeinfo['domain'] != self.task['forumRule']['domain'] or typeinfo['subdomain'] != self.task['forumRule']['subdomain']:
                raise CDSpiderNotUrlMatched()
            if  'urlPattern' in self.task['forumRule'] and self.task['forumRule']['urlPattern']:
                '''
                如果规则中存在url匹配规则，则进行url匹配规则验证
                '''
                u = utils.preg(self.task['url'], self.task['forumRule']['urlPattern'])
                if not u:
                    raise CDSpiderNotUrlMatched()
        if "parentid" in self.task:
            self.task['rid'] = self.task['parentid']
        #根据task中的rid获取文章信息
        rid = self.task.get('rid', None)
        if rid:
            article = self.db['ArticlesDB'].get_detail(rid, select=['rid', 'url', 'crawlinfo'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % rid)
            if not 'ulr' in self.task or not self.task['url']:
                self.task["url"] = article['url']
                save['base_url'] = article['url']
            self.task['article'] = article
        self.task.setdefault('crawlinfo', {})
        self.process = self.match_rule() or {"unique": {"data": None}}
        if not 'data' in self.process['unique'] or not self.process['unique']['data']:
            self.process['unique']['data'] = ','. join(self.process['parse']['item'].keys())
        save['paging'] = True
        if 'save' in self.task and self.task['save'] and 'page' in self.task['save']:
            self.page = self.task['save']['page']

    def match_rule(self):
        """
        匹配详情页规则
        """
        #优先获取task中详情规则
        parse_rule = self.task.get("forumRule", {})
        if not parse_rule:
            '''
            task中不存在详情规则，根据域名匹配规则库中的规则
            '''
            if 'rid' in self.task and self.task['rid']:
                return self.db['ForumRuleDB'].get_detail(self.task['rid'])
            url = self.task['url']
            subdomain, domain = utils.domain_info(url)
            if subdomain:
                '''
                优先获取子域名对应的规则
                '''
                parserule_list = self.db['ForumRuleDB'].get_list_by_subdomain(subdomain)
                for item in parserule_list:
                    print(item)
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
                parserule_list = self.db['ForumRuleDB'].get_list_by_domain(domain)
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
        文章解析
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parsed = {}
        if self.page == 1:
            main_parser = ItemParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule['one']), log_level=self.log_level, url=self.response['final_url'])
            parsed['main'] = main_parser.parse()
        replies_rule = {
            "filter": rule['filter'],
            'item': rule['item']
        }
        replies_parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(replies_rule), log_level=self.log_level, url=self.response['final_url'])
        parsed['replies'] = replies_parser.parse()
        self.response['parsed'] = parsed

    def _build_crawl_info(self, final_url):
        """
        构造爬虫log信息
        :param final_url 请求的url
        :input self.task 爬虫任务信息
        :input self.page 当前的页码
        """
        self.task['article']['crawlinfo']['mode'] = HANDLER_MODE_BBS_ITEM
        if not 'final_url' in self.task['article']['crawlinfo']:
            self.task['article']['crawlinfo']['final_url'] = {str(self.page): final_url}
        else:
            self.task['article']['crawlinfo']['final_url'][str(self.page)] = final_url
        if not 'forumRule' in self.task['article']['crawlinfo']:
            self.task['article']['crawlinfo']['forumRule'] = self.process.get('uuid', 0)
        self.task['article']['crawlinfo']['page'] = 1
        self.task['article']['crawlinfo']['forumRule'] = self.process.get('uuid', 0)

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
        """
        now = int(time.time())
        result = kwargs.pop('result')
        item = kwargs.pop('item', None) or {}
        #格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            'mediaType': self.process.get('mediaType', self.task.get('mediaType', MEDIA_TYPE_BBS)),
            "status": kwargs.get('status', ArticlesDB.STATUS_ACTIVE),
            'url': kwargs['final_url'],
            'title': result.pop('title', None) or item.get('title', None),              # 标题
            'author': result.pop('author', None) or item.get('author', None),      # 作者
            'content': result.pop('content', None) or item.get('content', None),
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

    def _build_replies_info(self, **kwargs):
        """
        构造评论数据
        """
        result = kwargs.pop('result')
        #爬虫信息记录
        result['crawlinfo'] = {
            'pid': self.task['article']['crawlinfo'].get('pid', 0),    # project id
            'sid': self.task['article']['crawlinfo'].get('sid', 0),    # site id
            'tid': self.task['article']['crawlinfo'].get('tid', 0),    # task id
            'uid': self.task['article']['crawlinfo'].get('uid', 0),    # url id
            'kid': self.task['article']['crawlinfo'].get('kid', 0),    # keyword id
            'ruleId': self.process['uuid'],                            # forumRule id
            'list_url': kwargs.pop('final_url'),                       # 列表url
        }
        result['mediaType'] = self.process.get('mediaType', self.task.get('mediaType', MEDIA_TYPE_BBS)),
        result['acid'] = self.task['article']['acid']                  # article acid
        result['rid'] = self.task['article']['rid']                    # article rid
        result['unid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result

    def build_replies_task(self, save):
        """
        构造回复任务
        """
        task = {
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_BBS)),
            'mode': HANDLER_MODE_BBS_ITEM,                           # handler mode
            'pid': self.task['article']['crawlinfo'].get('pid', 0), # project id
            'sid': self.task['article']['crawlinfo'].get('sid', 0), # site id
            'tid': self.task['article']['crawlinfo'].get('tid', 0), # task id
            'uid': self.task['article']['crawlinfo'].get('uid', 0), # url id
            'kid': self.task['article']['crawlinfo'].get('kid', 0), # keyword id
            'rid': self.process['uuid'],                            # rule id
            'url': self.task['url'],                                # url
            'parentid': self.task['article']['rid'],                # article id
            'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            'expire': 0 if int(self.process['expire']) == 0 else int(time.time()) + int(self.process['expire']),
        }
        self.debug("%s build replies task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.db['SpiderTaskDB'].get_list(HANDLER_MODE_BBS_ITEM, where={"parentid": task['parentid']})
                if len(list(l)) == 0:
                    uuid = self.db['SpiderTaskDB'].insert(task)
                    self.task['uuid'] = uuid
                    return uuid
                return None
            except:
                return None
        else:
            return 'testing_mode'

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        """
        self._build_crawl_info(final_url=self.response['final_url'])
        if self.response['parsed']:
            typeinfo = utils.typeinfo(self.response['final_url'])
            if not "uuid" in self.task:
                self.result2db(save, copy.deepcopy(typeinfo))
                self.result2attach(self.task['crawlinfo'], save, self.task['rid'], **typeinfo)
                if self.page == 1:
                    tid = self.build_replies_task(save)
                    if tid:
                        self.task['article']['crawlinfo']['forumRule'] = self.process['uuid']
                        self.task['article']['crawlinfo']['forumTaskId'] = tid
                        self.debug("%s new forum task: %s" % (self.__class__.__name__, str(tid)))

            self.replies2result()

    def result2db(self, save, typeinfo):
        """
        详情解析结果入库
        :param save 保存的上下文信息
        :param typeinfo 域名信息
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
                #生成文章唯一索引并验证是否已存在
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(self.response['final_url'], self.response['parsed']), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            if self.page == 1:
                #格式化文章信息
                result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed']['main'], crawlinfo=self.task['crawlinfo'], **unid)
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
                        result['rid'] = result_id
                        self.task['article'] = result
                    else:
                        item = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                        self.task['article'] = item
                        result_id = item['rid']
                        self.db['ArticlesDB'].update(result_id, result)
                self.task['rid'] = result_id
        else:
            if self.page == 1:
                result = self.db['ArticlesDB'].get_detail(result_id)
                '''
                对于已存在的文章，如果是第一页，则更新所有解析到的内容
                否则只追加content的内容
                '''
                #格式化文章信息
                result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed']['main'], crawlinfo=self.task['crawlinfo'], item = result)

                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s on_result: %s" % (self.__class__.__name__, result))
                else:
                    self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                    self.db['ArticlesDB'].update(result_id, result)
        if not result_id:
            raise CDSpiderDBError("Result insert failed")

    def replies2result(self):
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']['replies']:
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            for each in self.response['parsed']['replies']:
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成唯一ID, 并判断是否已存在
                    inserted, unid = self.db['RepliesUniqueDB'].insert(self.get_unique_setting(self.task['last_url'], each), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result = self.build_comment_info(result=each, final_url=self.response['final_url'], **unid)
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                    if not self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        result_id = self.db['RepliesDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(BbsItemHandler, self).finish(save)
        if self.page == 1:
            if self.task.get('rid') and self.task['article'].get('crawlinfo') and not self.testing_mode:
                self.db['ArticlesDB'].update(self.task['rid'], {"crawlinfo": self.task['article']['crawlinfo']})
                self.build_sync_task(self.task['rid'], 'ArticlesDB')
        if "uuid" in self.task and self.task['uuid']:
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
