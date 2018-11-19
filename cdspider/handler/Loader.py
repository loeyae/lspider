#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-14 23:31:54
"""
import logging
from cdspider import Component
from cdspider.exceptions import *
from cdspider.handler import BaseHandler
from cdspider.libs.tools import ModulerLoader
from cdspider.libs.constants import *

class Loader(Component):
    """
    handler loader
    """
    def __init__(self, context, task, spider, no_sync = False):
        self.ctx = context
        self.task = task
        self.params = {"spider": spider, "no_sync": no_sync}
        logger = logging.getLogger('handler')
        log_level = logging.WARN
        if context.obj.get('debug', False):
            log_level = logging.DEBUG
        super(Loader, self).__init__(logger, log_level)

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
            mode = self.task.get('mode', HANDLER_MODE_DEFAULT)
            return DEFAULT_HANDLER_SCRIPTS % {"handler": HANDLER_MODE_HANDLER_MAPPING[mode]}

    def load(self):
        try:
            moduler = self.get_moduler()
            moduler.update(self.params)
            mod = ModulerLoader(moduler).load_module()
            if hasattr(mod, 'handler'):
                return mod.handler
            else:
                _class_list = []
                for each in list(six.itervalues(mod.__dict__)):
                    if inspect.isclass(each) and each is not BaseHandler and issubclass(each, BaseHandler):
                        _class_list.append(each)
                l = len(_class_list)
                self.info("matched handler: %s" % _class_list)
                if l > 0:
                    _class = None
                    for each in _class_list:
                        if not _class:
                            _class = each
                        else:
                            if issubclass(each, _class):
                                _class = each
                    self.info("selected handler: %s" % _class)
                    if _class:
                        return _class(moduler['ctx'], moduler['task'], spider=moduler.get('spider', None), no_sync=moduler.get('no_sync', False))
        except Exception as exc:
            raise CDSpiderHandlerError(exc)
