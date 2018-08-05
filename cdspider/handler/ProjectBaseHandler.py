#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 21:42:18
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait
from cdspider.exceptions import *

class ProjectBaseHandler(BaseHandler, ResultTrait):

    def on_result(self, task, data, broken_exc, page_source, final_url):
        """
        on result
        """
        self.logger.debug("%s on_result: %s @ %s %s" % (self.__class__.__name__, str(data), mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=self.task.get('save', {}).get("base_url"), current_url=self.task.get('save', {}).get("request_url"))
        if mode == self.MODE_CHANNEL:
            self.list_to_result(final_url, {k: item}, typeinfo, self.task, page_source)
        elif mode == self.MODE_LIST:
            self.list_to_work(data, self.task)
        elif mode == self.MODE_ITEM:
            final_url = self.task.get('save').get("request_url")
            unique = False if not 'unid' in self.task else self.task['unid']
            self.item_to_result(final_url, data, page_source, unique)
        elif mode == self.MODE_ATT:
            final_url = self.task.get('save').get("request_url")
            self.list_to_attach(final_url, {k: item}, typeinfo, task, page_source)


