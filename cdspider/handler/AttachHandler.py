#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-16 11:13:07
:version: SVN: $Id: AttachHandler.py 1997 2018-07-02 11:11:00Z zhangyi $
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait, ResultTrait
from cdspider.exceptions import *

class AttachHandler(BaseHandler, NewTaskTrait, ResultTrait):
    """
    附加数据型
    """

    def build_newtask(self):
        """
        生成新任务
        """
        self.build_newtask_by_attachment()

    def get_site_type(self, stid, url):
        res = self.sitetypedb.get_detail(stid)
        if res:
            return res
        result = tldextract.extract(url)
        domain = result.domain + '.' + result.suffix
        subdomain = result.subdomain
        return {
            'domain': domain,
            'subdomain': subdomain,
            'type': self.sitetypedb.SITETYPE_TYPE_NO_MATCH,
        }

    def on_result(self, task, data, broken_exc, page_source, mode):
        self.logger.debug("AttachHandler on_result: %s @ %s %s" % (str(data), mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=task.get('save', {}).get("base_url"), current_url=task.get('save', {}).get("request_url"))
        if mode == self.MODE_ATT:
            final_url = task.get('save').get("request_url")
            typeinfo = self.get_site_type(self.task['site']['stid'], final_url)
            for k, item in data.items():
                if isinstance(item, list):
                    self.list_to_result(final_url, {k: item}, typeinfo, task, page_source)
                else:
                    self.item_to_result(final_url, {k: item}, typeinfo, task, page_source)

    def on_error(self, task, exc, mode):
        self.logger.debug("AttachHandler on_error: %s @ %s" % (traceback.format_exc(), mode))
        if mode == self.MODE_ATT:
            super(AttachHandler, self).on_error(task, exc, mode)

    def finish(self, task, mode):
        if mode == self.MODE_ATT:
            super(AttachHandler, self).finish(task, mode)
