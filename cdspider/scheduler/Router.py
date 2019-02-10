#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-21 16:10:41
"""
import time
import sys
import traceback
import json
from . import BaseScheduler
from cdspider.exceptions import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.libs.utils import get_object, run_in_thread

class Router(BaseScheduler):
    """
    路由--初级任务分发
    """

    def __init__(self, context, mode = ROUTER_MODE_PROJECT, outqueue = None):
        super(Router, self).__init__(context)
        if not outqueue:
            outqueue = QUEUE_NAME_SCHEDULER_TO_TASK
        self.outqueue = self.queue[outqueue]
        self.mode = mode
        self.interval = 5
        self._check_time = None

    def valid(self):
        if not self.testing_mode and int(time.strftime('%S')) < 10:
            self.debug("scheduler2task is running")
            return False
        return True

    def schedule(self, message = None):
        self.info("%s route starting..." % self.__class__.__name__)
        def handler_schedule(mode, name, rate, ctx):
            handler = get_object("cdspider.handler.%s" % name)(ctx, None)
            self.info("%s loaded handler: %s" % (self.__class__.__name__, handler))
            save = {}
            while True:
                has_item = False
                for item in handler.route(mode, rate, save):
                    if item:
                        has_item = True
                        message = {
                            "rate": rate,
                            "mode": mode,
                            "offset": item['offset'],
                            "count": item['count'],
                        }
                        self.debug("%s route message: %s" % (self.__class__.__name__, str(message)))
                        if not self.testing_mode:
                            self.outqueue.put_nowait(message)
                if not has_item:
                    break
                time.sleep(0.1)
            del handler
        threads = []
        ratemap = self.ctx.obj.get('app_config', {}).get('ratemap', {})
        for key, name in HANDLER_MODE_HANDLER_MAPPING.items():
            for rate in ratemap:
                threads.append(run_in_thread(handler_schedule, key, name, rate, self.ctx))

        for each in threads:
            if not each.is_alive():
                continue
            if hasattr(each, 'terminate'):
                each.terminate()
            each.join()

        self.info("%s route end, %s threads was run" % (self.__class__.__name__, len(threads)))

    def newtask(self, message):
        name = message.get('mode', HANDLER_MODE_DEFAULT)

        handler = get_object("cdspider.handler.%s" % HANDLER_MODE_HANDLER_MAPPING[name])(self.ctx, None)
        self.info("Spider loaded handler: %s" % handler)
        handler.newtask(message)
        del handler

    def status(self, message):
        name = message.get('mode', HANDLER_MODE_DEFAULT)
        handler = get_object("cdspider.handler.%s" % HANDLER_MODE_HANDLER_MAPPING[name])(self.ctx, None)
        self.info("Spider loaded handler: %s" % handler)
        handler.status(message)
        del handler

    def frequency(self, message):
        name = message.get('mode', HANDLER_MODE_DEFAULT)
        handler = get_object("cdspider.handler.%s" % HANDLER_MODE_HANDLER_MAPPING[name])(self.ctx, None)
        self.info("Spider loaded handler: %s" % handler)
        handler.frequency(message)
        del handler

    def expire(self, message):
        name = message.get('mode', HANDLER_MODE_DEFAULT)
        handler = get_object("cdspider.handler.%s" % HANDLER_MODE_HANDLER_MAPPING[name])(self.ctx, None)
        self.info("Spider loaded handler: %s" % handler)
        handler.expire(message)
        del handler

    def build_item_task(self, message):
        rid = message['rid']
        if not isinstance(rid, (list, tuple)):
            rid = [rid]
        for each in rid:
            m = {"rid": each, "mode": message.get('mode', HANDLER_MODE_DEFAULT_ITEM)}
            self.queue[QUEUE_NAME_SPIDER_TO_RESULT].put_nowait(m)

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

        def build(task):
            self.debug("%s rpc buid get message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = errmsg = None
            try:
                task = json.loads(task)
                self.build_item_task(task)
                parsed = True
            except Exception as exc:
                errmsg = str(exc)
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output, "errmsg": errmsg}

            return json.dumps(result)
        application.register_function(build, 'build')

        def newtask(task):
            self.debug("%s rpc newtask get message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = errmsg = None
            try:
                task = json.loads(task)
                self.newtask(task)
                parsed = True
            except Exception as exc:
                errmsg = str(exc)
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output, "errmsg": errmsg}

            return json.dumps(result)
        application.register_function(newtask, 'newtask')

        def status(task):
            self.debug("%s rpc status get message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                self.status(task)
                parsed = True
            except :
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(status, 'status')

        def frequency(task):
            self.debug("%s rpc frequency get message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                self.frequency(task)
                parsed = True
            except :
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(frequency, 'frequency')

        def expire(task):
            self.debug("%s rpc expire get message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                self.expire(task)
                parsed = True
            except :
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(expire, 'expire')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.info('spider.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()
