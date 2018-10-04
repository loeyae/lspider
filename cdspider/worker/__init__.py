#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import time
import logging
import traceback
import copy
import json
import tornado.ioloop
from six.moves import queue
from cdspider import Component

class BaseWorker(Component):

    interval = 0.1

    inqueue_key = None
    excqueue_key = None

    def __init__(self, db, queue, proxy, mailer = None, log_level=logging.WARN):
        self.db = db
        self.queue=queue
        self.mailer=mailer
        self.proxy=proxy
        self._quit=False
        self.inqueue = None
        self.excqueue = None
        self.ioloop = tornado.ioloop.IOLoop()
        if self.inqueue_key:
            self.inqueue = self.queue[self.inqueue_key]
        if self.excqueue_key:
            self.excqueue = self.queue[self.excqueue_key]
        self.log_level = log_level
        logger = logging.getLogger('worker')
        super(BaseWorker, self).__init__(logger, log_level)

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
            self.error(traceback.format_exc())

    def run_once(self):
        self.info("%s once starting..." % self.__class__.__name__)
        message = None
        if self.inqueue:
            message = self.inqueue.get_nowait()
            self.debug("%s got message: %s" % (self.__class__.__name__, message))
        self.on_result(message)
        self.info("%s once end" % self.__class__.__name__)

    def run(self):
        """
        scheduler运行方法
        """
        self.info("%s starting..." % self.__class__.__name__)

        def queue_loop():
            while not self._quit:
                message = None
                try:
                    if self.inqueue:
                        message = self.inqueue.get_nowait()
                        self.debug("%s got message: %s" % (self.__class__.__name__, message))
                    self.on_result(message)
                    time.sleep(self.interval)
                except queue.Empty:
                    self.debug("empty queue")
                    time.sleep(5)
                    continue
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.on_error(e, message)
                    break

        tornado.ioloop.PeriodicCallback(queue_loop, 1000, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.info("%s exiting..." % self.__class__.__name__)

    def quit(self):
        self._quit = True


from .result_worker import ResultWorker
from .exc_worker import ExcWorker
from .sync_kafka_worker import SyncKafkaWorker
