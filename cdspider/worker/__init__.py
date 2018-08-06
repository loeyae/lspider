#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import time
import logging
import traceback
from six.moves import queue

class BaseWorker():

    LOOP_INTERVAL = 0.1

    def __init__(self, db, queue, proxy, mailer = None, log_level=logging.WARN):
        self.db = db
        self.queue=queue
        self.mailer=mailer
        self.proxy=proxy
        self.log_level=log_level
        self.logger=logging.getLogger("worker")
        self.logger.setLevel(log_level)
        self._quit=False

    def on_result(self, message):
        raise NotImplementedError

    def on_error(self, exc):
        if self.excqueue:
            message = {
                'mode': 'result',
                'parsed': time.strftime("%Y-%m-%d %H:%M:%S"),
                'err_message': str(exc),
                'tracback': traceback.format_exc(),
            }
            self.excqueue.put_nowait(message)
        else:
            self.logger.exception(exc)

    def run_once(self):
        self.logger.info("%s once starting..." % self.__class__.__name__)
        message = self.inqueue.get_nowait()
        self.on_result(message)
        self.logger.info("%s once end" % self.__class__.__name__)

    def run(self):
        self.logger.info("%s starting..." % self.__class__.__name__)

        while not self._quit:
            try:
                message = self.inqueue.get_nowait()
                self.on_result(message)
            except queue.Empty:
                time.sleep(self.LOOP_INTERVAL)
                continue
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.on_error(e)
                time.sleep(self.LOOP_INTERVAL)
                continue

        self.logger.info("%s exiting..." % self.__class__.__name__)

    def quit(self):
        self._quit = True


from .result_worker import ResultWorker
from .exc_worker import ExcWorker
from .search_worker import SearchWorker
