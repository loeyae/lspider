#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:51:00
"""
import time
import traceback
import tldextract
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
        self.build_newtask_by_keywords()
