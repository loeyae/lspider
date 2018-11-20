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
from cdspider.libs.utils import get_object

class Router(BaseScheduler):
    """
    路由--初级任务分发
    """
    MODE_PROJECT = 'project'
    MODE_SITE = 'site'
    MODE_ITEM = 'item'

    def __init__(self, context, mode):
        super(Router, self).__init__(context)
        self.outqueue = self.queue["scheduler2task"]
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
        for key, name in HANDLER_MODE_HANDLER_MAPPING.items():
            handler = get_object("cdspider.handler.%s" % name)(self.ctx, None)
            save = {}
            while True:
                has_item = False
                for item in handler.route(self.mode, save):
                    has_item = True
                    message = {
                        "mode": self.mode,
                        "h-mode": key,
                        "item": item,
                    }
                    self.outqueue.put_nowait(message)
                if not has_item:
                    break
        self.info("%s route end" % self.__class__.__name__)

    def xmlrpc_run(self, port=23333, bind='127.0.0.1'):
        import umsgpack
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = {"message": "xmlrpc is running"}
            return json.dumps(result)
        application.register_function(hello, 'hello')

        def add_project(pid):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            broken_exc = None
            try:
                self.projects.add(pid)
                status = 200
            except :
                broken_exc = traceback.format_exc()
                status = 500
            output = sys.stdout.read()
            result = {"broken_exc": broken_exc, "stdout": output, 'status': status}

            return json.dumps(result)
        application.register_function(add_project, 'new_project')

        def remove_project(pid):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            broken_exc = None
            try:
                self.projects.remove(pid)
                status = 200
            except KeyError:
                status = 200
            except :
                broken_exc = traceback.format_exc()
                status = 500
            output = sys.stdout.read()
            result = {"broken_exc": broken_exc, "stdout": output, 'status': status}

            return json.dumps(result)
        application.register_function(remove_project, 'remove_project')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.info('spider.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()
