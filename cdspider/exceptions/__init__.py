#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import socket

class CDSpiderError(Exception):
    """
    异常基类
    """

    msg = 'CDSpider Error'

    def __init__(self, *args, **kwargs):
        l = len(args)
        if l >= 1:
            kwargs['msg'] = args[0]
        if l >= 2:
            kwargs['base_url'] = args[1]
        if l >= 3:
            kwargs['current_url'] = args[2]
        self.msg = kwargs.pop('msg', None) or self.msg
        self.hostname = socket.gethostname()
        self.localip = socket.gethostbyname(self.hostname)
        self.base_url = kwargs.pop('base_url', None)
        self.current_url = kwargs.pop('current_url', None)
        self.params = kwargs

    def __str__(self):
        exception_msg = 'Message: %s\r\n' % self.msg
        if self.hostname is not None:
            exception_msg += 'HostName: %s\r\n' % self.hostname
        if self.localip is not None:
            exception_msg += 'LocalIp: %s\r\n' % self.localip
        if self.base_url is not None:
            exception_msg += 'BaseUrl: %s\r\n' % self.base_url
        if self.current_url is not None:
            exception_msg += 'CurrentUrl: %s\r\n' % self.current_url
        if self.params:
            for k,v in self.params.items():
                exception_msg += '%s: %s\r\n' % (k.capitalize(), v)
        return exception_msg

class CDSpiderSettingError(CDSpiderError):
    """
    配置错误
    """

    msg = "CDSpider Setting Error"

class CDSpiderNotUrlMatched(CDSpiderError):
    """
    url匹配错误
    """

    msg = "CDSpider Not Url Matched"

class CDSpiderCrawlerError(CDSpiderError):
    """
    抓取异常基类
    """

    msg = "CDSpider Crawler Error"

class CDSpiderCrawlerNoMethod(CDSpiderCrawlerError):
    """
    调用了不存在的方法
    """

    msg = "CDSpider Crawler No Method"

class CDSpiderCrawlerRemoteServerError(CDSpiderCrawlerError):
    """
    抓取时目标站点错误
    """

    msg = "CDSpider Crawler Remote Server Error"

class CDSpiderCrawlerConnectionError(CDSpiderCrawlerError):
    """
    抓取时目标站点连接错误
    """

    msg = "CDSpider Crawler Connection Error"

class CDSpiderCrawlerWaitError(CDSpiderCrawlerError):
    """
    未发现等待的元素
    """

    msg = "CDSpider Crawler Wait Element Time Out"

class CDSpiderCrawlerTimeout(CDSpiderCrawlerError):
    """
    抓取时超时
    """

    msg = "CDSpider Crawler Time Out"

class CDSpiderCrawlerConnectTimeout(CDSpiderCrawlerTimeout):
    """
    抓取时连接超时
    """

    msg = "CDSpider Crawler Connect Time Out Error"

class CDSpiderCrawlerReadTimeout(CDSpiderCrawlerTimeout):
    """
    抓取时读取超时
    """

    msg = "CDSpider Crawler Read Time Out Error"

class CDSpiderCrawlerNotFound(CDSpiderCrawlerError):
    """
    抓取时目标站点无效
    """

    msg = "CDSpider Crawler Not Found"

class CDSpiderCrawlerNoResponse(CDSpiderCrawlerError):
    """
    抓取时Response不存在
    """

    msg = "CDSpider Crawler No Response"

class CDSpiderCrawlerBadRequest(CDSpiderCrawlerError):
    """
    抓取时请求无效
    """

    msg = "CDSpider Crawler Bad Request"

class CDSpiderCrawlerForbidden(CDSpiderCrawlerError):
    """
    抓取时目标站点禁止
    """

    msg = "CDSpider Crawler Forbidden"

class CDSpiderCrawlerProxyError(CDSpiderCrawlerError):
    """
    抓取时代理出错
    """

    msg = "CDSpider Crawler Proxy Error"

class CDSpiderCrawlerProxyExpired(CDSpiderCrawlerProxyError):
    """
    抓取时代理出错
    """

    msg = "CDSpider Crawler Proxy Expired"

class CDSpiderCrawlerBroken(CDSpiderCrawlerError):
    """
    抓取时中断抓取
    """

    msg = "CDSpider Crawler Broken"

class CDSpiderCrawlerDoLogin(CDSpiderCrawlerBroken):
    """
    抓取时需要登录验证
    """

    msg = "CDSpider Crawler Need Dologin"

class CDSpiderCrawlerNoExists(CDSpiderCrawlerBroken):
    """
    抓取时目标不存在
    """

    msg = "CDSpider Crawler No Exists"

class CDSpiderCrawlerNoSource(CDSpiderCrawlerBroken):
    """
    抓取时未获取到内容
    """

    msg = "CDSpider Crawler No Source"

class CDSpiderCrawlerNoNextPage(CDSpiderCrawlerBroken):
    """
    抓取时没有下一页
    """

    msg = "CDSpider Crawler No Next Page"

class CDSpiderCrawlerMoreThanMaximum(CDSpiderCrawlerBroken):
    """
    抓取时超过规定页数
    """

    msg = "CDSpider Crawler More Than Maximum"

class CDSpiderParserError(CDSpiderError):
    """
    解析异常基类
    """

    msg = "CDSpider Parse Error"

class CDSpiderParserNoContent(CDSpiderParserError):
    """
    解析时未匹配到内容
    """

    msg = "CDSpider Parse No Content"

class CDSpiderParserJsonLoadFaild(CDSpiderParserError):
    """
    解析时json数据load失败
    """

    msg = "CDSpider Parse Json Load Faild"

class CDSpiderRedisError(CDSpiderError):
    """
    redis异常基类
    """

    msg = "CDSpider Redis Error"

class CDSpiderDBError(CDSpiderError):
    """
    DB异常基类
    """

    msg = "CDSpider DB Error"

class CDSpiderDBDataNotFound(CDSpiderDBError):
    """
    DB数据获取失败
    """

    msg = "CDSpider DB Data Not Found"

class CDSpiderQueueError(CDSpiderError):
    """
    queue异常基类
    """

    msg = "CDSpider Queue Error"

class CDSpiderHandlerError(CDSpiderError):
    """
    Handler异常基类
    """

    msg = "CDSpider Handler Error"

class CDSpiderHandlerForbiddenWord(CDSpiderHandlerError):
    """
    自定义Handler使用禁用关键词和函数
    """

    msg = "CDSpider Handler Have Forbidden Word"

class CDSpiderMailerError(CDSpiderError):
    """
    mailer异常基类
    """
    msg = "CDSpider Mailer Error"
