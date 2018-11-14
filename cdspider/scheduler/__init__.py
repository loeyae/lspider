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

class BaseScheduler(Component):

    inqueue = None
    interval = 0.05

    def __init__(self, context):
        self._running = False
        self._quit = False
        self.ctx = context
        g = context.obj
        self.db = g.get('db')
        self.queue = g.get('queue')
        self.ioloop = tornado.ioloop.IOLoop()
        log_level = logging.WARN
        if g.get("debug", False):
            log_level = logging.DEBUG
        self.log_level = log_level
        logger = logging.getLogger('scheduler')
        super(BaseScheduler, self).__init__(logger, log_level)

    def schedule(self, message):
        raise NotImplementedError

    def quit(self):
        self._quit = True
        self._running = False

    def run_once(self):
        self.info("%s once starting..." % self.__class__.__name__)
        message = None
        if self.inqueue:
            message = self.inqueue.get_nowait()
            self.debug("%s got message: %s" % (self.__class__.__name__, message))
        self.schedule(message)
        self.info("%s once end" % self.__class__.__name__)

    def run(self):
        """
        scheduler运行方法
        """
        self.info("%s starting..." % self.__class__.__name__)

        def queue_loop():
            while not self._quit:
                try:
                    message = None
                    if self.inqueue:
                        message = self.inqueue.get_nowait()
                        self.debug("%s got message: %s" % (self.__class__.__name__, message))
                    self.schedule(message)
                    time.sleep(self.interval)
                except queue.Empty:
                    self.debug("empty queue")
                    time.sleep(5)
                    continue
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.exception(e)
                    break

        tornado.ioloop.PeriodicCallback(queue_loop, 1000, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.info("%s exiting..." % self.__class__.__name__)

from .Router import Router
from .PlantaskScheduler import PlantaskScheduler
from .SynctaskScheduler import SynctaskScheduler
from .NewtaskScheduler import NewtaskScheduler
from .StatusScheduler import StatusScheduler
from .SearchScheduler import SearchScheduler
