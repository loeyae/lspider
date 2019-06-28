# -*- coding: utf-8 -*-
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

#default source
DEFAULT_SOURCE = "<html></html>"

#exception set
from cdspider.exceptions import *

CONTINUE_EXCEPTIONS = (CDSpiderCrawlerProxyError, CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout, CDSpiderCrawlerNoResponse, CDSpiderCrawlerForbidden)

IGNORE_EXCEPTIONS = (CDSpiderCrawlerNoNextPage, CDSpiderCrawlerMoreThanMaximum, CDSpiderCrawlerNoExists, CDSpiderCrawlerNoSource, CDSpiderCrawlerReturnBroken, CDSpiderCrawlerDebugBroken)

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
QUEUE_NAME_STATUS = "status_queue"
QUEUE_NAME_SCHEDULER_TO_TASK = "scheduler2task"
QUEUE_NAME_SCHEDULER_TO_SPIDER = "scheduler2spider"
QUEUE_NAME_SPIDER_TO_RESULT = "spider2result"
QUEUE_NAME_EXCINFO = "excinfo_queue"

#handler redister fn type
HANDLER_FUN_INIT = '0'                   # 初始化自定义方法类型
HANDLER_FUN_PROCESS = '1'                # 初始化流程自定义方法类型
HANDLER_FUN_PREPARE = '2'                # 预处理自定义方法类型
HANDLER_FUN_PRECRAWL = '3'               # 抓取前自定义方法类型
HANDLER_FUN_CRAWL = '4'                  # 抓取自定义方法类型（将代替系统的抓取方法）
HANDLER_FUN_POSTCRAWL = '5'              # 抓取后自定义方法类型
HANDLER_FUN_PREPARSE = '6'               # 解析前自定义方法类型
HANDLER_FUN_PARSE = '7'                  # 解析自定义方法类型（将代替系统的解析方法）
HANDLER_FUN_POSTPARSE = '8'              # 解析后自定义方法类型
HANDLER_FUN_RESULT = '9'                 # 结果处理发放类型
HANDLER_FUN_NEXT = '10'                  # 下一步自定义方法类型
HANDLER_FUN_CONTINUE = '11'              # continue自定义方法类型
HANDLER_FUN_REPETITION = '12'            # 重复处理自定义方法类型
HANDLER_FUN_ERROR = '13'                 # 错误处理自定义方法类型
HANDLER_FUN_FINISH = '14'                # finish自定义方法类型


#handler mode
HANDLER_MODE_DEFAULT = 'default'                      # 默认handler
HANDLER_MODE_DEFAULT_LIST = 'list'                    # 默认列表handler
HANDLER_MODE_DEFAULT_ITEM = 'item'                    # 默认详情handler
HANDLER_MODE_DEFAULT_SEARCH = 'search'                # 默认搜索handler
HANDLER_MODE_SITE_SEARCH = 'site-search'              # 站内搜索

#route mode
ROUTER_MODE_PROJECT = 'project'
ROUTER_MODE_SITE = 'site'
ROUTER_MODE_TASK = 'task'
ROUTER_MODE_URL = 'url'

#task type
TASK_TYPE_LIST = 1      # '列表类',
TASK_TYPE_SEARCH = 2    # '搜索类',
TASK_TYPE_AUTHOR = 3    # '作者类',
TASK_TYPE_OTHER = 4     # '其他类',

#media type
MEDIA_TYPE_NEWS = 1     # '新闻',
MEDIA_TYPE_BBS = 2      # '论坛',
MEDIA_TYPE_ECS = 3      # '电商',
MEDIA_TYPE_WECHAT = 4   # '微信',
MEDIA_TYPE_TOUTIAO = 5  # '头条',
MEDIA_TYPE_APP = 6      # 'APP',
MEDIA_TYPE_ASK = 7      # '问答',
MEDIA_TYPE_TIEBA = 8    # '贴吧',
MEDIA_TYPE_WEMEDIA = 9  # '自媒体',
MEDIA_TYPE_BLOG = 10    # '博客',
MEDIA_TYPE_VEDIO = 11   # '视频',
MEDIA_TYPE_WEIBO = 12   # '微博',
MEDIA_TYPE_PAPER = 13   # '平媒',
MEDIA_TYPE_OTHER = 99   # '其他',

RESULT_SYNC_QUEUE_NAME = 'result_sync_queue'  # app.json中结果同步队列名设置的key

FREQUENCY_ONCE = "0"
EXPIRE_NEVER = 0
URL_HANDLE_MODE_ITEM = "item"
URL_HANDLE_MODE_RESULT = "result"
URL_HANDLE_MODE_NONE = "none"