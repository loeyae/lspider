#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:51:00
"""
import re
import time
import traceback
import tldextract
import urllib.request
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait
from cdspider.exceptions import *

class SearchHandler(BaseHandler, NewTaskTrait):
    """
    基于搜索的基础handler
    """

    def __init__(self, *args, **kwargs):
        super(SearchHandler, self).__init__(*args, **kwargs)

    def newtask(self):
        """
        生成新任务
        """
        if 'channel' in self.task:
            return self.build_newtask_by_channel()
        self.build_newtask_by_keywords()

    def url_prepare(self, url):
        """
        url预处理
        """
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = urllib.request.Request(url = url, headers = headers, method = 'GET')
        response = urllib.request.urlopen(req)
        furl = response.geturl()
        if furl != url:
            return furl
        else:
            content = response.read()
            urllist = re.findall(b'window\.location\.replace\((?:\'|")([^\'"]+)(?:\'|")\)', content)
            if urllist:
                return urllist[0]
            urllist = re.findall(b'window\.location\.href\s*=\s*(?:\'|")([^\'"]+)(?:\'|")\)', content)
            if urllist:
                return urllist[0]

        raise CDSpiderParserError('url parsed failed')
