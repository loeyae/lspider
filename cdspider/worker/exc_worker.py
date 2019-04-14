# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-14 21:06:24
"""
import traceback
from cdspider.libs.constants import *
from cdspider.mailer import BaseSender
from cdspider.worker import BaseWorker
from cdspider.libs import utils

class ExcWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_EXCINFO


    def __init__(self, context):
        super(ExcWorker, self).__init__(context)
        self.mailer = None
        try:
            config = self.g['app_config']['mail']
            mailer = config['mailer']
            sender = config['sender']
            receiver = self.g['app_config'].get('exc_work_receiver', None)
            if receiver:
                self.mailer = utils.load_mailer(mailer, sender=sender, receiver=receiver)
        except:
            self.error(traceback.format_exc())

    def on_result(self, message):
        if self.mailer and isinstance(self.mailer, BaseSender):
            try:
                subject = "CDSpider Error Notification"
                self.mailer.send(subject=subject, message=message)
            except:
                self.error("got message: %s" % message)
                self.exception(message, exc_info=traceback.format_exc())
        else:
            self.error(message)
