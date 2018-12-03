#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-21 16:10:41
"""
import time
import logging
from . import BaseScheduler
from cdspider.exceptions import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.libs.utils import get_object, run_in_thread

class Router(BaseScheduler):
    """
    路由--初级任务分发
    """

    def __init__(self, context, mode = ROUTER_MODE_PROJECT):
        super(Router, self).__init__(context)
        self.outqueue = self.queue[QUEUE_NAME_SCHEDULER_TO_TASK]
        self.mode = mode
        self.interval = 5
        self._check_time = None

    def valid(self):
        if self.outqueue.qsize() > 0:
            self.debug("scheduler2task is running")
            return False
        return True

    def schedule(self, message = None):
        self.info("%s route starting..." % self.__class__.__name__)
        def handler_schedule(key, name, mode, ctx):
            handler = get_object("cdspider.handler.%s" % name)(ctx, None)
            self.info("%s loaded handler: %s" % (self.__class__.__name__, handler))
            save = {}
            while True:
                has_item = False
                for item in handler.route(mode, save):
                    if item:
                        has_item = True
                        message = {
                            "mode": mode,
                            "h-mode": key,
                            "item": item,
                        }
                        self.debug("%s route message: %s" % (self.__class__.__name__, str(message)))
                        if not self.testing_mode:
                            self.outqueue.put_nowait(message)
                if not has_item:
                    break
                time.sleep(0.1)
        threads = []
        for key, name in HANDLER_MODE_HANDLER_MAPPING.items():
            threads.append(run_in_thread(handler_schedule, key, name, self.mode, self.ctx))

        for each in threads:
            if not each.is_alive():
                continue
            if hasattr(each, 'terminate'):
                each.terminate()
            each.join()

        self.info("%s route end, %s threads was run" % (self.__class__.__name__, len(threads)))

    def xmlrpc_run(self, port=25555, bind='127.0.0.1'):
        import umsgpack
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = {"message": "xmlrpc is running"}
            return json.dumps(result)
        application.register_function(hello, 'hello')

        def newtask(task):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                name = task.get('mode', HANDLER_MODE_DEFAULT)
                handler = get_object("cdspider.handler.%s" % name)(self.ctx, None)
                handler.newtask(task)
                parsed = True
            except :
                broken_exc = traceback.format_exc()
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(newtask, 'newtask')

        def status(task):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                name = task.get('mode', HANDLER_MODE_DEFAULT)
                handler = get_object("cdspider.handler.%s" % name)(self.ctx, None)
                handler.status(task)
                parsed = True
            except :
                broken_exc = traceback.format_exc()
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(status, 'status')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.info('spider.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()
