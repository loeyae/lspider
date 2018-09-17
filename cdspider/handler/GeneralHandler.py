#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-15 22:37:51
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait
from cdspider.exceptions import *

class GeneralHandler(BaseHandler, NewTaskTrait):
    """
    普通站点handler
    """

    def __init__(self, *args, **kwargs):
        super(GeneralHandler, self).__init__(*args, **kwargs)


    def newtask(self):
        """
        生成新任务
        """
        if 'channel' in self.task:
            return self.build_newtask_by_channel()
        return self.build_newtask_by_urls()
