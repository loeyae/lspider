#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 21:42:18
"""
from tld import get_tld
from cdspider.handler import BaseHandler, ResultTrait
from cdspider.exceptions import *

class ProjectBaseHandler(BaseHandler, ResultTrait):

    def on_result(self, data, broken_exc, page_source, final_url):
        """
        on result
        """
        self.debug("%s on_result: %s @ %s %s" % (self.__class__.__name__, str(data), self.mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=self.task.get('save', {}).get("base_url"), current_url=final_url)
        if self.mode == self.MODE_CHANNEL:
            typeinfo = self._typeinfo(final_url)
            self.channel_to_list(final_url, data, typeinfo, page_source)
        elif self.mode == self.MODE_LIST:
            typeinfo = self._typeinfo(final_url)
            self.list_to_item(final_url, data, typeinfo, page_source)
        elif self.mode == self.MODE_ITEM:
            typeinfo = self.parse_domain(final_url)
            unique = False if not 'unid' in self.task else self.task['unid']
            self.item_to_result(final_url, data, typeinfo, page_source, unique)
        elif self.mode == self.MODE_ATT:
            typeinfo = self.parse_domain(final_url)
            unique = False if not 'unid' in self.task else self.task['unid']
            self.attach_to_result(final_url, data, typeinfo, page_source, unique)

    def parse_domain(self, final_url):
        _url = final_url
        parent_url = self.task.get('save', {}).get('parent_url', None)
        if parent_url and get_tld(parent_url) == get_tld(final_url):
            _url = parent_url
        typeinfo = self._typeinfo(_url)
        return typeinfo
