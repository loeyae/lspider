#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-14 21:06:24
:version: SVN: $Id: exc_worker.py 1395 2018-06-22 02:41:32Z zhangyi $
"""
import traceback
from cdspider.mailer import BaseSender
from cdspider.worker import BaseWorker

class ExcWorker(BaseWorker):

    def on_result(self, message):
        if self.mailer and isinstance(self.mailer, BaseSender):
            try:
                subject = "CDSpider Error Notification"
                self.mailer.send(subject=subject, message=message)
            except:
                self.logger.error("got message: %s" % message)
                self.logger.exception(message, exc_info=traceback.format_exc())
        else:
            self.logger.error(message)
