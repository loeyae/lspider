# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:27:08
"""

from cdspider.worker import  BaseWorker
from cdspider.libs.constants import *
from cdspider.libs.utils import load_handler

class NewtaskWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_NEWTASK

    def on_result(self, message):
        self.debug("got message: %s" % message)
        try:
            name = message.get('mode', HANDLER_MODE_DEFAULT)
            handler = load_handler(name, self.ctx, None)
            self.debug("Spider loaded handler: %s" % handler)
            handler.newtask(message)
            del handler
        except Exception as e:
            self.error(e)