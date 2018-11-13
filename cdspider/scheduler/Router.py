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

class Router(BaseScheduler):
    """
    路由--初级任务分发
    """
    MODE_PROJECT = 'project'
    MODE_SITE = 'site'
    MODE_ITEM = 'item'

    def __init__(self, db, queue, mode, log_level = logging.WARN):
        super(Router, self).__init__(db, queue, log_level)
        self.outqueue = queue["scheduler2task"]
        self.mode = mode
        self.interval = 5
        self.projects = set()
        self._check_time = None
        self._check_projects()

    def _check_projects(self):
        self.info("%s check_projects starting..." % self.__class__.__name__)
        projectid = 0
        while True:
            projects = self.db['ProjectsDB'].get_list(where=[('pid', "$gt", projectid), ("status", self.db['ProjectsDB'].STATUS_ACTIVE)])
            i = 0
            for project in projects:
                self.debug("%s check_projects project: %s " % (self.__class__.__name__, str(project)))
                self.projects.add(project['pid'])
                if project['pid'] > projectid:
                    projectid = project['pid']
                i += 1
            if i == 0:
                self.debug("%s check_projects no projects" % self.__class__.__name__)
                break
        self._check_time = int(time.time())
        self.info("%s check_projects end" % self.__class__.__name__)

    def schedule(self, message = None):
        if self.outqueue.qsize() > 0:
            self.debug("%s outqueue is not empty" % self.__class__.__name__)
            return
        self.info("%s schedule starting..." % self.__class__.__name__)
        if self._check_time - int(time.time()) > 3600:
            self._check_projects()
        if self.mode == self.MODE_PROJECT:
            for pid in self.projects:
                self.debug("push pid: %s into queue" % pid)
                self.outqueue.put_nowait({"pid": pid})
        elif self.mode == self.MODE_SITE:
            for pid in self.projects:
                sid = 0
                while True:
                    sites = self.db['SitesDB'].get_new_list(sid, pid, where = {"status": self.db['SitesDB'].STATUS_ACTIVE}, select=["sid"])
                    i = 0
                    for site in sites:
                        sid = site['sid']
                        self.debug("push sid: %s into queue" % sid)
                        self.outqueue.put_nowait({"pid": pid, "sid": sid})
                        i += 1
                    if i == 0:
                        break
        elif self.mode == self.MODE_ITEM:
            for pid in self.projects:
                uid = 0
                while True:
                    urls = self.db['UrlsDB'].get_new_list_by_pid(uid, pid, where = {"status": self.db['UrlsDB'].STATUS_ACTIVE}, select=["uid"])
                    i = 0
                    for url in urls:
                        uid = url['uid']
                        self.debug("push uid: %s into queue" % uid)
                        self.outqueue.put_nowait({"pid": pid, "uid": uid})
                        i += 1
                    if i == 0:
                        break
                aid = 0
                while True:
                    attachments = self.db['AttachmentDB'].get_new_list_by_pid(uid, pid, where = {"status": self.db['AttachmentDB'].STATUS_ACTIVE}, select=['aid'])
                    i = 0
                    for attachment in attachments:
                        aid = attachment['aid']
                        self.debug("push aid: %s into queue" % aid)
                        self.outqueue.put_nowait({"pid": pid, "aid": aid})
                        i += 1
                    if i == 0:
                        break
                crid = 0
                while True:
                    channels = self.db['ChannelRulesDB'].get_new_list_by_pid(uid, pid, where = {"status": self.db['ChannelRulesDB'].STATUS_ACTIVE}, select=['crid'])
                    i = 0
                    for channel in channels:
                        crid = channel['crid']
                        self.debug("push crid: %s into queue" % crid)
                        self.outqueue.put_nowait({"pid": pid, "crid": crid})
                        i += 1
                    if i == 0:
                        break
        self.info("%s schedule end" % self.__class__.__name__)

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
