#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-15 22:37:51
:version: SVN: $Id: GeneralHandler.py 1995 2018-07-02 11:10:07Z zhangyi $
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait, ResultTrait
from cdspider.exceptions import *

class GeneralHandler(BaseHandler, NewTaskTrait, ResultTrait):
    """
    普通站点handler
    """

    def __init__(self, *args, **kwargs):
        super(GeneralHandler, self).__init__(*args, **kwargs)


    def build_newtask(self):
        """
        生成新任务
        """
        self.build_newtask_by_urls()

    def get_site_type(self, stid, url = None):
        res = self.sitetypedb.get_detail(stid)
        if res:
            return res
        domain = None
        subdomain = None
        if url:
            result = tldextract.extract(url)
            domain = result.domain + '.' + result.suffix
            subdomain = result.subdomain
        return {
            'domain': domain,
            'subdomain': subdomain,
            'type': self.sitetypedb.SITETYPE_TYPE_NO_MATCH,
        }

    def on_result(self, task, data, broken_exc, page_source, mode):
        self.logger.debug("GeneralHandler on_result: %s @ %s %s" % (str(data), mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=task.get('save', {}).get("base_url"), current_url=task.get('save', {}).get("request_url"))
        if mode == self.MODE_LIST:
            #pass
            self.list_to_work(data, task)
        elif mode == self.MODE_ITEM:
            final_url = task.get('save').get("request_url")
            typeinfo = self.get_site_type(self.task['site']['stid'], final_url)
            unique = False if not 'unid' in task else task['unid']
            rtid = self.item_to_result(final_url, data, typeinfo, task, page_source, unique)
            if task['site']['attachment_list']:
                for attachment in task['site']['attachment_list']:
                    self.item_to_attachment(rtid, final_url, attachment, data)

    def on_error(self, task, exc, mode):
        self.logger.debug("GeneralHandler on_error: %s @ %s" % (traceback.format_exc(), mode))
        if mode == self.MODE_LIST:
            super(GeneralHandler, self).on_error(task, exc, mode)
        elif mode == self.MODE_ITEM:
            super(GeneralHandler, self).on_error(task, exc, mode)

    def finish(self, task, mode):
        if mode == self.MODE_LIST:
            super(GeneralHandler, self).finish(task, mode)
