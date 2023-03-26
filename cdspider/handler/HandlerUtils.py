# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019/4/14 21:27
"""
import copy
import re
import time
import traceback
from urllib.parse import urlparse, urlunparse

from cdspider.libs import utils
from cdspider.libs.constants import *


class HandlerUtils(object):
    """
    Handler辅助类
    """

    @classmethod
    def init_crawl_info(cls, crawl_id):
        return {
            "crawl_start": crawl_id,
            "crawl_end": None,
            "crawl_urls": {},
            "crawl_count": {
                "total": 0,
                "new_count": 0,
                "repeat_count": 0,
                "page": 0,
                "repeat_page": 0,
            },
            "traceback": None
        }

    @classmethod
    def init_reponse(cls, url):
        return {
            "source": None,
            "final_url": url,
            "orig_url": None,
            "content": None,
            "headers": None,
            "status_code": None,
            "url": None,
            "time": None,
            "cookies": None,
            "broken_exc": None,
            "parsed": None,
        }

    @classmethod
    def build_spider_task_where(cls, message):
        if 'pid' in message and message['pid']:
            return {"pid": message['pid']}
        if 'sid' in message and message['sid']:
            return {"sid": message['sid']}
        if 'rid' in message and message['rid']:
            return {"rid": message['rid']}
        if 'tid' in message and message['tid']:
            return {"tid": message['tid']}
        if 'kid' in message and message['kid']:
            return {"kid": message['kid']}
        if 'uid' in message and message['uid']:
            return {"uid": message['uid']}
        return {"uuid": 0}

    @classmethod
    def get_crawler(cls, rule, log_level):
        """
        load crawler
        :param rule: 爬虫规则
        """
        crawler = rule.get('crawler', '') or 'requests'
        return utils.load_crawler(crawler, proxy=rule.get('proxy'), log_level=log_level)

    @classmethod
    def build_log(cls, mode, task, crawl_id):
        return {
            'stid': task['uuid'],  # task id
            'pid': task['pid'],  # project id
            'sid': task['sid'],  # site id
            'tid': task['tid'],  # task id
            'uid': task.get('uid', 0),  # url id
            'kid': task.get('kid', 0),  # keyword id
            'rid': task.get('rid', 0),  # rule id
            'mode': mode,  # handler mode
            'frequency': task.get('frequency', None),
            'crawl_start': crawl_id,  # crawl start time
            'ctime': crawl_id  # create time
        }

    @classmethod
    def update_request(cls, request, update):
        for k, v in update.items():
            if k in request and isinstance(request[k], list):
                request[k].extend(v)
            elif k in request and isinstance(request[k], dict):
                request[k].update(v)
            else:
                request[k] = v
        return request

    @classmethod
    def format_paging(cls, paging):
        """
        格式化分页规则
        :param paging:
        :return:
        """
        if not paging or "url" not in paging or not paging['url'] or not isinstance(paging['url'], dict):
            return None

        def build_rule(rule_, item_):
            _type = item_.pop('type', 'incr_data')
            if _type == 'match_data':
                rule_['match_data'].update({item_.pop('name'): item_})
            else:
                rule_[_type].append(item_)

        if paging['url']['type'] == 'match' and not paging['url']['filter']:
            return None
        rule = {
            "url": paging['url'],
            'max': paging.get('max', 0),
            'first': paging.get('first', 0),
            'incr_data': [],
            'random': [],
            'cookie': [],
            'hard_code': [],
            'match_data': {}
        }
        if "rule" in paging and isinstance(paging['rule'], (list, tuple)):
            for item in paging['rule']:
                if 'name' not in item or not item['name']:
                    continue
                build_rule(rule, copy.deepcopy(item))
        elif "rule" in paging and isinstance(paging['rule'], dict):
            for item in paging['rule'].values():
                if 'name' not in item or not item['name']:
                    continue
                build_rule(rule, item)
        if not rule['incr_data'] and not rule['match_data'] and rule['url']['type'] != 'match':
            return None
        return rule

    @classmethod
    def get_unique_setting(cls, process, url, data):
        """
        获取生成唯一ID的字段
        :param url: 用来生成唯一索引的url
        :param data: 用来生成唯一索引的数据
        :return: 唯一索引的源字符串
        """
        # 获取唯一索引设置规则
        identify = process.get('unique', None)
        subdomain, domain = utils.parse_domain(url)
        if not subdomain:
            parsed = urlparse(url)
            arr = list(parsed)
            arr[1] = "www.%s" % arr[1]
            u = urlunparse(arr)
        else:
            u = url
        if identify:
            if 'url' in identify and identify['url']:
                rule, key = utils.rule2pattern(identify['url'])
                if rule and key:
                    ret = re.search(rule, url)
                    if ret:
                        u = ret.group(key)
                else:
                    ret = re.search(identify['url'], url)
                    if ret:
                        u = ret.group(0)
            if 'query' in identify and identify['query'] and identify['query'].strip(','):
                u = utils.build_filter_query(url, identify['query'].strip(',').split(','))
            if 'data' in identify and identify['data'] and identify['data'].strip(','):
                udict = dict.fromkeys(identify['data'].strip(',').split(','))
                query = utils.dictunion(data, udict)
                return utils.build_query(u, query)
        return u

    @classmethod
    def build_update_log(cls, crawl_info):
        return {
            'crawl_urls': crawl_info['crawl_urls'],  # {page: request url, ...}
            'crawl_end': int(time.time()),  # crawl end time
            'total': crawl_info['crawl_count']['total'],  # 抓取到的数据总数
            'new_count': crawl_info['crawl_count']['new_count'],  # 抓取到的数据入库数'
            'repeat_count': crawl_info['crawl_count']['repeat_count'],  # 抓取到的数据重复数
            'page': crawl_info['crawl_count']['page'],  # 抓取的页数
            'repeat_page': crawl_info['crawl_count']['repeat_page'],  # 重复的页数
            'errid': crawl_info.get('errid', 0),  # 如果有错误，关联的错误日志ID
        }

    @classmethod
    def build_error_log(cls, tid, mode, crawl_id, frequency, url, exc):
        return {
            'tid': tid,  # spider task id
            'mode': mode,
            'create_at': crawl_id,
            'frequency': frequency,  # process info
            'url': url,  # error message
            'error': str(exc),  # create time
            'msg': str(traceback.format_exc()),  # trace log
            'class': exc.__class__.__name__,  # error class
        }

    @classmethod
    def send_result_into_queue(cls, queue, config, mode, rid):
        queue_name = config.get(RESULT_SYNC_QUEUE_NAME, None)
        if queue_name is not None:
            if isinstance(queue_name, str):
                queue_name = [queue_name]
            for q in queue_name:
                queue[q].put_nowait({"mode": mode, "rid": rid})

    @classmethod
    def match_detail_rule(cls, db, url):
        parse_rule = None
        subdomain, domain = utils.domain_info(url)
        if subdomain:
            '''
            优先获取子域名对应的规则
            '''
            parserule_list = db['ParseRuleDB'].get_list_by_subdomain(subdomain)
            for item in parserule_list:
                if not parse_rule:
                    '''
                    将第一条规则选择为返回的默认值
                    '''
                    parse_rule = item
                if 'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return item
        if not parse_rule:
            '''
            获取域名对应的规则
            '''
            parserule_list = db['ParseRuleDB'].get_list_by_domain(domain)
            for item in parserule_list:
                if not parse_rule:
                    '''
                    将第一条规则选择为返回的默认值
                    '''
                    parse_rule = item
                if 'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return item
        return parse_rule
