#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:51:00
:version: SVN: $Id: SearchHandler.py 1996 2018-07-02 11:10:29Z zhangyi $
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait, ResultTrait
from cdspider.exceptions import *

class SearchHandler(BaseHandler, NewTaskTrait, ResultTrait):
    """
    基于搜索的基础handler
    """

    def __init__(self, *args, **kwargs):
        super(SearchHandler, self).__init__(*args, **kwargs)
        self.siteinfo=self.sitedb.get_detail(6)

    def build_newtask(self):
        """
        生成新任务
        """
        self.build_newtask_by_keywords()

    def get_site_type(self, url):
        result = tldextract.extract(url)
        domain = result.domain + '.' + result.suffix
        subdomain = result.subdomain
        res = self.sitetypedb.get_detail_by_domain(domain=domain, subdomain=subdomain)
        if res:
            return res
        return {
            'domain': domain,
            'subdomain': subdomain,
            'type': self.sitetypedb.SITETYPE_TYPE_NO_MATCH,
        }

    def on_result(self, task, data, broken_exc, page_source, mode):
        self.logger.debug("SearchHandler on_result: %s @ %s %s" % (str(data), mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=task.get('save', {}).get("base_url"), current_url=task.get('save', {}).get("request_url"))
        if mode == self.MODE_LIST:
            #pass
            self.list_to_item(data, task)
        elif mode == self.MODE_ITEM:
            final_url = task.get('save').get("request_url")
            typeinfo = self.get_site_type(final_url)
            unique = False if not 'unid' in task else task['unid']
            rtid = self.item_to_result(final_url, data, typeinfo, task, page_source, unique)
            if task['site']['attachment_list']:
                for attachment in task['site']['attachment_list']:
                    self.item_to_attachment(rtid, final_url, attachment, data)

    def on_error(self, task, exc, mode):
        self.logger.debug("SearchHandler on_error: %s @ %s" % (traceback.format_exc(), mode))
        if mode == self.MODE_LIST:
            super(SearchHandler, self).on_error(task, exc, mode)
        elif mode == self.MODE_ITEM:
            super(SearchHandler, self).on_error(task, exc, mode)

    def finish(self, task, mode):
        if mode == self.MODE_LIST:
            super(SearchHandler, self).finish(task, mode)
