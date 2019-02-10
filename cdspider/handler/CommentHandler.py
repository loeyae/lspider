#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-2 15:16:34
"""
import copy
import time
from . import BaseHandler
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import CustomParser
from cdspider.parser.lib import TimeParser

class CommentHandler(BaseHandler):
    """
    comment handler
    :property task 爬虫任务信息 {"mode": "comment", "uuid": SpiderTask.comment uuid}
                   当测试该handler，数据应为 {"mode": "comment", "url": url, "commentRule": 评论规则，参考评论规则}
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
            rule = self.match_rule() or {}
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        if "commentRule" in self.task:
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != self.task['commentRule']['domain'] or (self.task['commentRule']['subdomain'] and typeinfo['subdomain'] != self.task['commentRule']['subdomain']):
                raise CDSpiderNotUrlMatched()
            crawler = self.get_crawler(self.task.get('commentRule', {}).get('request'))
            crawler.crawl(url=self.task['parent_url'])
            data = utils.get_attach_data(CustomParser, crawler.page_source, self.task['parent_url'], self.task['commentRule'], self.log_level)
            if data == False:
                return None
            url, params = utils.build_attach_url(data, self.task['commentRule'], self.task['parent_url'])
            del crawler
            if not url:
                raise CDSpiderNotUrlMatched()
            self.task['url'] = url
            save['base_url'] = url
            save["hard_code"] = params
            self.task['commentRule']['request']['hard_code'] = params
        else:
            mediaType = self.task.get('mediaType', MEDIA_TYPE_OTHER)
            if mediaType == MEDIA_TYPE_WEIBO:
                article = self.db['WeiboInfoDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            else:
                article = self.db['ArticlesDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % self.task['parentid'])
            self.task['parent_url'] = article['url']
            self.task['acid'] = article['acid']
        self.process = self.match_rule()  or {"unique": {"data": None}}
        if not 'data' in self.process['unique'] or not self.process['unique']['data']:
            self.process['unique']['data'] = ','. join(self.process['parse']['item'].keys())
        save['paging'] = True

    def match_rule(self):
        """
        获取匹配的规则
        """
        parse_rule = self.task.get("commentRule", {})
        if not parse_rule:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['CommentRuleDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("CommentRule: %s not exists" % ruleId)
            if parse_rule['status'] != CommentRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("comment rule not active")
        return parse_rule

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
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
            for each in self.response['parsed']:
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成唯一ID, 并判断是否已存在
                    inserted, unid = self.db['CommentsUniqueDB'].insert(self.get_unique_setting(self.task['parent_url'], each), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result = self.build_comment_info(result=each, final_url=self.response['final_url'], **unid)
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                    if not self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        result_id = self.db['CommentsDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                        self.build_sync_task(result_id, self.task['parentid'])
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def build_comment_info(self, **kwargs):
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
        result['crawlinfo'] = {
            'pid': self.task['pid'],                        # project id
            'sid': self.task['sid'],                        # site id
            'tid': self.task['tid'],                        # task id
            'uid': self.task['uid'],                        # url id
            'kid': self.task['kid'],                        # url id
            'ruleId': self.task['rid'],                     # commentRule id
            'stid': self.task['uuid'],                      # spider task id
            'list_url': kwargs.pop('final_url'),            # 列表url
        }
        result['mediaType'] = self.task.get('mediaType', MEDIA_TYPE_OTHER)
        result['pubtime'] = pubtime                             # pubtime
        result['acid'] = self.task['acid']                      # article acid
        result['rid'] = self.task['parentid']                   # article rid
        result['unid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result

    def finish(self, save):
        """
        记录抓取日志
        """
        super(CommentHandler, self).finish(save)
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

    def build_sync_task(self, uuid, rid):
        """
        生成同步任务并入队
        """
        message = {'id': uuid, 'rid': rid}
        self.queue['comment2kafka'].put_nowait(message)
