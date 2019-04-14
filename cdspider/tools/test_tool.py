# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-22 14:31:27
"""
import time
from cdspider.tools import Base


class test_tool(Base):
    """
    put you comment
    """
    inqueue_key = None

    def __init__(self, context, daemon = False):
        super(test_tool, self).__init__(context, daemon)

    def process(self, *args):
        self.debug("%s got args: %s" % (self.__class__.__name__, args))
        self.debug("%s runing @ %s" % (self.__class__.__name__, time.time()))
        time.sleep(5)
