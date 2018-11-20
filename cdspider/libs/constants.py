#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2018-11-17 9:22:03$"

#proxy mode
PROXY_TYPE_NEVER = 'never'
PROXY_TYPE_EVER = 'ever'
PROXY_TYPE_AUTO = 'auto'

#scripts default
DEFAULT_HANDLER_SCRIPTS = """
from cdspider.handler import %(handler)s

handler = %(handler)s(__moduler__['ctx'], __moduler__['task'], spider=__moduler__.get('spider', None), no_sync=__moduler__.get('no_sync', False))
"""

#exception set
from cdspider.exceptions import *

CONTINUE_EXCEPTIONS = (CDSpiderCrawlerProxyError, CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout, CDSpiderCrawlerNoResponse, CDSpiderCrawlerForbidden)

IGNORE_EXCEPTIONS = (CDSpiderCrawlerNoNextPage, CDSpiderCrawlerMoreThanMaximum, CDSpiderCrawlerProxyExpired, CDSpiderCrawlerNoExists, CDSpiderCrawlerNoSource)

RETRY_EXCEPTIONS = (CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout)

NOT_EXISTS_EXCEPTIONS = (CDSpiderCrawlerNotFound, CDSpiderCrawlerNoSource, CDSpiderParserError)

BROKEN_EXCEPTIONS = {
    'base': CDSpiderCrawlerBroken,
    'exists': CDSpiderCrawlerNoExists,
    'login': CDSpiderCrawlerDoLogin,
    'source': CDSpiderCrawlerNoSource,
    'page': CDSpiderCrawlerNoNextPage,
    'max': CDSpiderCrawlerMoreThanMaximum,
}

TIMEOUT_EXCEPTIONS = {
    'base': CDSpiderCrawlerTimeout,
    'connect': CDSpiderCrawlerConnectTimeout,
    'read': CDSpiderCrawlerReadTimeout,
}

PROXY_EXCEPTIONS = {
    'base': CDSpiderCrawlerProxyError,
    'expired': CDSpiderCrawlerProxyExpired,
}

#queue name
QUEUE_NAME_NEWTASK = "newtask_queue"
QUEUE_NAME_STATUS =  "status_queue"
QUEUE_NAME_SCHEDULER_TO_SPIDER = "scheduler2spider"
QUEUE_NAME_SPIDER_TO_RESULT = "spider2result"
QUEUE_NAME_SPIDER_TO_SCHEDULER = "spider2scheduler"
QUEUE_NAME_EXCINFO = "excinfo_queue"
QUEUE_NAME_SEARCH = "search_work"
QUEUE_NAME_RESULT_TO_KAFKA = "result2kafka"
QUEUE_NAME_RESULT_TO_NEWTASK = "result2newtask"
QUEUE_NAME_WECHAT_TO_REPLY = "wechat2reply"


#handler redister fn type
HANDLER_FUN_INIT = '0'
HANDLER_FUN_PROCESS = '1'
HANDLER_FUN_PREPARE = '2'
HANDLER_FUN_PRECRAWL = '3'
HANDLER_FUN_CRAWL = '4'
HANDLER_FUN_POSTCRAWL = '5'
HANDLER_FUN_PREPARSE = '6'
HANDLER_FUN_PARSE = '7'
HANDLER_FUN_POSTPARSE = '8'
HANDLER_FUN_RESULT = '9'
HANDLER_FUN_NEXT = '10'
HANDLER_FUN_CONTINUE = '11'
HANDLER_FUN_REPETITION = '12'
HANDLER_FUN_ERROR = '13'
HANDLER_FUN_FINISH = '14'


#handler mode
HANDLER_MODE_DEFAULT = 'default'
HANDLER_MODE_DEFAULT_LIST = 'list'
HANDLER_MODE_WECHAT_LIST = 'wechat-list'
HANDLER_MODE_TOUTIAO_LIST = 'toutiao-list'
HANDLER_MODE_BBS_LIST = 'bbs-list'
HANDLER_MODE_WEMEDIA_LIST = 'wemedia-list'
HANDLER_MODE_DEFAULT_ITEM = 'item'
HANDLER_MODE_WECHAT_ITEM = 'wechat-item'
HANDLER_MODE_TOUTIAO_ITEM = 'toutiao-item'
HANDLER_MODE_BBS_ITEM = 'bbs-item'
HANDLER_MODE_WEMEDIA_ITEM = 'wemedia-item'
HANDLER_MODE_LINKS_CLUSTER = 'links-cluster'

#handler mode mapping HANDLER
HANDLER_MODE_HANDLER_MAPPING = {
    HANDLER_MODE_DEFAULT: 'GeneralHandler',
    HANDLER_MODE_DEFAULT_LIST: 'GeneralListHandler',
    HANDLER_MODE_WECHAT_LIST: 'WechatListHandler',
    HANDLER_MODE_TOUTIAO_LIST: 'ToutiaoListHandler',
    HANDLER_MODE_BBS_LIST: 'BbsListHandler',
    HANDLER_MODE_WEMEDIA_LIST: 'WemediaListHandler',
    HANDLER_MODE_DEFAULT_ITEM: 'GeneralItemHandler',
    HANDLER_MODE_WECHAT_ITEM: 'WechatItemHandler',
    HANDLER_MODE_TOUTIAO_ITEM: 'ToutiaoItemHandler',
    HANDLER_MODE_BBS_ITEM: 'BbsItemHandler',
    HANDLER_MODE_WEMEDIA_ITEM: 'WemediaItemHandler',
    HANDLER_MODE_LINKS_CLUSTER: 'LinksClusterHandler',
}

#handler mode mapping db
HANDLER_MODE_DB_MAPPING = {
    HANDLER_MODE_DEFAULT: None,
    HANDLER_MODE_DEFAULT_LIST: 'url',
    HANDLER_MODE_WECHAT_LIST: 'url',
    HANDLER_MODE_TOUTIAO_LIST: 'url',
    HANDLER_MODE_BBS_LIST: 'url',
    HANDLER_MODE_WEMEDIA_LIST: 'url',
    HANDLER_MODE_DEFAULT_ITEM: 'GeneralItemHandler',
    HANDLER_MODE_WECHAT_ITEM: 'WechatItemHandler',
    HANDLER_MODE_TOUTIAO_ITEM: 'ToutiaoItemHandler',
    HANDLER_MODE_BBS_ITEM: 'BbsItemHandler',
    HANDLER_MODE_WEMEDIA_ITEM: 'WemediaItemHandler',
}
