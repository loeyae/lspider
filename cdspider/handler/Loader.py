#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-14 23:31:54
"""
import six
import inspect
import logging
from cdspider import Component
from cdspider.exceptions import *
from cdspider.handler import BaseHandler
from cdspider.libs.tools import ModulerLoader
from cdspider.libs.utils import get_object
from cdspider.libs.constants import *

class Loader(Component):
    """
    handler loader
    """
    def __init__(self, context, task, no_sync = False):
        logger = logging.getLogger('handler')
        log_level = logging.WARN
        if context.obj.get('debug', False):
            log_level = logging.DEBUG
        super(Loader, self).__init__(logger, log_level)
        mode = task.get('mode', HANDLER_MODE_DEFAULT)
        _class = get_object('cdspider.handler.%s' % HANDLER_MODE_HANDLER_MAPPING[mode])
        self.params = {"context": context, "task": task, "no_sync": no_sync}
        self.handler = _class(**self.params)

    def get_moduler(self):
        scripts = self.get_scripts()
        if not scripts:
            return None
        moduler = {
            "name": "cdspider.handler.custom",
            "scripts": scripts
        }
        return moduler

    def get_scripts(self):
        scripts = self.handler.get_scripts()
        if scripts:
            return scripts.strip()
        return None

    def load(self):
        try:
            moduler = self.get_moduler()
            if moduler:
                mod = ModulerLoader(moduler).load_module(self.handler, self.params)
                if hasattr(mod, 'handler') and isinstance(mod.handler, BaseHandler):
                    return mod.handler
                else:
                    _class_list = [item for item in mod.__dict__.values() if (inspect.isclass(item) and issubclass(item, BaseHandler))]
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
                            return _class(**self.params)
            return self.handler
        except Exception as exc:
            raise CDSpiderHandlerError(exc)
