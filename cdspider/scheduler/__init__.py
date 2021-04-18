# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import gc
import time
import logging
import tornado.ioloop
from six.moves import queue
from cdspider import Component


class BaseScheduler(Component):

    inqueue = None
    interval = 500

    def __init__(self, context):
        self._running = False
        self._quit = False
        self.ctx = context
        g = context.obj
        self.testing_mode = g.get('testing_mode', False)
        self.db = g.get('db')
        self.queue = g.get('queue')
        self.pcker = g.get('pcker', True)
        self.ioloop = tornado.ioloop.IOLoop()
        log_level = logging.WARN
        if g.get("debug", False):
            log_level = logging.DEBUG
        self.log_level = log_level
        logger = logging.getLogger('scheduler')
        super(BaseScheduler, self).__init__(logger, log_level)

    def schedule(self, message):
        raise NotImplementedError

    def valid(self):
        return True

    def quit(self):
        self._quit = True
        self._running = False

    def run_once(self):
        self.info("%s once starting..." % self.__class__.__name__)
        if self.valid():
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
        self.t = 0
        def queue_loop():
            if not self.valid():
                time.sleep(2)
                return
            if self._quit:
                raise SystemExit
            self.t += 1
            try:
                message = None
                if self.inqueue:
                    message = self.inqueue.get_nowait()
                    self.debug("%s got message: %s" % (self.__class__.__name__, message))
                self.schedule(message)
                time.sleep(0.05)
            except queue.Empty:
                # self.debug("empty queue")
                time.sleep(2)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                self.exception(e)
                time.sleep(0.05)
            finally:
                self.flush()
                gc.collect()
                if self.t > 50:
                    if self.pcker:
                        self.info("%s broken" % self.__class__.__name__)
                        raise SystemExit
                    else:
                        self.t = 0

        tornado.ioloop.PeriodicCallback(queue_loop, self.interval, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.info("%s exiting..." % self.__class__.__name__)

from .Counter import Counter
from .Counter import CounterMananger
from .Router import Router
from .Scheduler import Scheduler
