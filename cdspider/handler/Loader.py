#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-14 23:31:54
"""
from cdspider.exceptions import *
from cdspider.handler import *
from cdspider.libs.tools import ModulerLoader
from cdspider.libs.constants import *

class Loader(Object):
    """
    put you comment
    """
    def __init__(self, context, task, spider, no_sync = False, mod=None):
        self.ctx = context
        self.task = task
        self.params = {"spider": spider, "no_sync": no_sync}

    def get_moduler(self):
        scripts = self.get_scripts()
        moduler = {
            "name": "cdspider.handler.custom",
            "scripts": scripts,
            "ctx": self.ctx,
            "task": self.task
        }
        return moduler

    def get_scripts(self):
        hasscripts = False
        #TODO 根据任务从数据库获取详细信息
        if not hasscripts:
            return DEFAULT_HANDLER_SCRIPTS % {"handler": "GeneralHandler"}

    def load(self):
        try:
            moduler = self.get_moduler()
            moduler.update(self.params)
            mod = ModulerLoader(moduler).load_module()
            return mod.handler
        except:
            raise CDSpiderHandlerError()
