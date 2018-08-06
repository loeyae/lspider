#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 21:42:18
"""
from cdspider.handler import BaseHandler, ResultTrait
from cdspider.exceptions import *

class ProjectBaseHandler(BaseHandler, ResultTrait):

    def on_result(self, data, broken_exc, page_source, final_url):
        """
        on result
        """
        self.logger.debug("%s on_result: %s @ %s %s" % (self.__class__.__name__, str(data), mode, str(broken_exc)))
        if not data:
            if broken_exc:
                raise broken_exc
            raise CDSpiderParserNoContent("No parsed content",
                base_url=self.task.get('save', {}).get("base_url"), current_url=final_url)
        if mode == self.MODE_CHANNEL:
            typeinfo = self._domain_info(final_url)
            self.channel_to_list(final_url, data, typeinfo, page_source)
        elif mode == self.MODE_LIST:
            typeinfo = self._domain_info(final_url)
            self.list_to_item(final_url, data, typeinfo, page_source)
        elif mode == self.MODE_ITEM:
            typeinfo = self._domain_info(self.task.get('save', {}).get('parent_url', final_url))
            unique = False if not 'unid' in self.task else self.task['unid']
            self.item_to_result(final_url, data, typeinfo, page_source, unique)
        elif mode == self.MODE_ATT:
            typeinfo = self._domain_info(self.task.get('save', {}).get('parent_url', final_url))
            unique = False if not 'unid' in self.task else self.task['unid']
            self.attach_to_result(final_url, data, typeinfo, page_source, unique)
