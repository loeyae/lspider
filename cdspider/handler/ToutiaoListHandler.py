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
import re
from . import WemediaListHandler
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.parser import ListParser, CustomParser
from cdspider.libs import utils
from cdspider.parser.lib import TimeParser

class ToutiaoListHandler(WemediaListHandler):
    """
    general list handler
    :property task 爬虫任务信息 {"mode": "list", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "list", "url": url, "listRule": 列表规则，参考列表规则}
    """

    TAB_ARTICLE = 'article'
    TAB_VIDEO = 'video'
    TAB_TOUTIAO = 'toutiao'
    MAX_RETRY = 10
    EXPIRE = 7200

    LIST_URL = 'https://www.toutiao.com/pgc/ma/?page_type=1&max_behot_time={max_behot_time}&uid={uid}&media_id={mediaId}&output=json&is_json=1&count=20&from=user_profile_app&version=2&as={as}&cp={cp}&callback=jsonp4'
    VIDEO_URL = 'https://www.toutiao.com/pgc/ma/?page_type=0&max_behot_time={max_behot_time}&uid={uid}&media_id={mediaId}&output=json&is_json=1&count=20&from=user_profile_app&version=2&as={as}&cp={cp}&callback=jsonp4'
    TOUTIAO_URL = 'https://www.toutiao.com/api/pc/feed/?category=pc_profile_ugc&utm_source=toutiao&visit_user_id={uid}&max_behot_time={max_behot_time}'

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        rule = self.match_rule(save)
        self.process = rule

    def prepare(self, save):
        super(ToutiaoListHandler, self).prepare(save)
        uid = save['request']['hard_code'][0]['value']
        if not 'save' in self.task or not self.task['save']:
            self.task['save'] = {}
        ctime = self.task['save'].get('timestamp', 0)
        if not self.task['save'].get('honey') or not self.task['save'].get('mediaId') or self.crawl_id - int(ctime) > self.EXPIRE:
            crawler = self.get_crawler({"crawler": "selenium", "method": "open", "proxy": "never"})
            request_params = copy.deepcopy(self.request_params)
            request_params['method'] = 'open'
            crawler.crawl(**request_params)
            getHoneyjs='return (ascp.getHoney())'
            honey = dict(crawler._driver.execute_script(getHoneyjs))
    #        _signatureJs='return TAC.sign('+ uid +')'
    #        self.debug("%s sign js: %s" % (self.__class__.__name__, _signatureJs))
    #        _signature = crawler._driver.execute_script(_signatureJs)
    #        save['_signature'] = _signature
            mediaIdJs = 'return userInfo.mediaId'
            mediaId = crawler._driver.execute_script(mediaIdJs)
            self.task['save']['mediaId'] = mediaId
            self.task['save']['honey'] = honey
            del crawler
#        self.request_params['url'] = 'https://www.toutiao.com/c/user/article/?page_type=1&user_id=' + uid + '&max_behot_time=0&count=20&as='+ honey['as'] +'&cp='+ honey['cp'] +'&_signature='+ _signature
        self.request_params['url'] = utils.format_(self.process['jsonUrl'], {"uid": uid, "mediaId": self.task['save']['mediaId'], "max_behot_time": 0, "as": self.task['save']['honey']['as'], "cp": self.task['save']['honey']['cp']})
        save['base_url'] = self.request_params['url']
        self.request_params['headers'] = {'Host': 'www.toutiao.com'}

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        if not parsed:
            raise CDSpiderCrawlerNoResponse()
        self.response['parsed'] = parsed

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        tab = self.task.get('save', {}).get('tab', self.TAB_ARTICLE)
        if self.response['parsed']:
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
                        self.add_interact(rid=result_id, result=item, **unid)
                        self.build_item_task(result_id)
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

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
            'status': kwargs.get('status', ArticlesDB.STATUS_INIT),
            'mediaType': MEDIA_TYPE_TOUTIAO,
            'url': kwargs['final_url'],
            'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
            'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'crawlinfo': kwargs.get('crawlinfo'),
            'result': result,
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
            }
        return r

#    def on_next(self, save):
#        """
#        下一页解析
        #"""
#        if self.page > 10:
#            raise  CDSpiderCrawlerMoreThanMaximum()
#        self.page += 1
#        tab = self.task.get('save', {}).get('tab', self.TAB_ARTICLE)
#        rule = {
#            "has_more": {
#                "filter": "@json:has_more"
#            },
#            "max_behot_time": {
#                "filter": "@json:next.max_behot_time"
#            }
#        }
#        parser = CustomParser(source=self.response['last_source'], ruleset=rule, log_level=self.log_level, url=self.response['final_url'])
#        parsed = parser.parse()
#        if 'has_more' in parsed and parsed['has_more'] == '1':
#            uid = save['request']['hard_code'][0]['value']
#            if tab == self.TAB_VIDEO:
#                save['next_url'] = self.request_params['url'] = self.VIDEO_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'], **self.task['save']['honey'])
#            elif tab == self.TAB_TOUTIAO:
#                save['next_url'] = self.request_params['url'] = self.TOUTIAO_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'])
#            else:
#                save['next_url'] = self.request_params['url'] = self.LIST_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'], **self.task['save']['honey'])
#        else:
#            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=save.get('request_url'))
