#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-16 11:13:07
"""
import time
import traceback
import tldextract
from cdspider.database.base import *
from cdspider.handler import BaseHandler, NewTaskTrait
from cdspider.exceptions import *

class AttachHandler(BaseHandler, NewTaskTrait):
    """
    附加数据型
    """

    def newtask(self):
        """
        生成新任务
        """
        self.build_newtask_by_attachment()
