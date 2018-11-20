#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 18:01:26
:version: SVN: $Id: Spider.py 2266 2018-07-06 06:50:15Z zhangyi $
"""
import re
import time
import sys
import logging
import traceback
import copy
import json
import tornado.ioloop
from six.moves import queue

from cdspider import Component
from cdspider.handler import BaseHandler, Loader
from cdspider.exceptions import *
from cdspider.libs import utils
from cdspider.libs import UrlBuilder
from cdspider.libs.tools import *
from cdspider.database.base import *
from cdspider.libs.constants import *

class Spider(Component):
    """
    爬虫流程实现
    """

    def __init__(self, context, no_sync = False, handler=None, no_input=False):
        self._quit = False
        self._running = False
        self.ctx = context
        g = context.obj
        if no_input:
            queue = {}
        else:
            queue = g.get('queue')

        self.inqueue = queue.get('scheduler2spider')
        self.status_queue = queue.get('status_queue')
        self.queue = queue
        self.no_sync = no_sync
        self.ioloop = tornado.ioloop.IOLoop()
        self.set_handler(handler)

        log_level = logging.WARN
        if g.get("debug", False):
            log_level = logging.DEBUG
        self.log_level = log_level
        logger = logging.getLogger('spider')
        super(Spider, self).__init__(logger, log_level)

    def set_handler(self, handler):
        if handler and isinstance(handler, BaseHandler):
            self.handler = handler

    def fetch(self, task, return_result = False):
        """
        抓取操作
        """
        if not task:
            self.debug("Spider fetch exit with no task")
            return
        self.info("Spider fetch start, task: %s" % task)
        last_source = None
        save = {"base_url": task['url']}
        handler = self.get_handler(task)
        if return_result:
            return_data = []
        try:
            self.info("Spider loaded handler: %s" % handler)
            last_source_unid = None
            last_url = None
            self.info("Spider process start")
            try:
                self.info("Spider fetch prepare start")
                handler.init(save)
                save['retry'] = 0
                while True:
                    try:
                        handler.prepare(save)
                    except CONTINUE_EXCEPTIONS as e:
                        handler.on_continue(e, save)
                        continue
                    break
                self.info("Spider fetch prepare end")
                save['retry'] = 0
                while True:
                    self.info('Spider crawl start')
                    handler.crawl(save)
                    if isinstance(handler.response['broken_exc'], CONTINUE_EXCEPTIONS):
                        handler.on_continue(handler.response['broken_exc'], save)
                        continue
                    elif handler.response['broken_exc']:
                        raise handler.response['broken_exc']
                    if not handler.response['last_source']:
                        raise CDSpiderCrawlerError('Spider crawl failed')
                    unid = utils.md5(handler.response['last_source'])
                    if last_source_unid == unid or last_url == handler.response['last_url']:
                        raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=handler.response['last_url'])
                    last_source_unid = unid
                    last_url = handler.response['last_url']
                    self.info("Spider crawl end")
                    self.info("Spider parse start")
                    handler.parse()
                    self.info("Spider parse end, result: %s" % str(handler.response))
                    self.info("Spider result start")
                    handler.on_result(save)
                    self.info("Spider result end, result: %s" % str(handler.response))
                    if return_result:
                        return_data.append((handler.response['parsed'], None, handler.response['last_source'], handler.response['last_url'], save))

                        raise CDSpiderCrawlerBroken("DEBUG MODE BROKEN")
                    handler.on_next(save)
            finally:
                self.info("Spider process end")
        except Exception as e:
            if not return_result:
                handler.on_error(e)
            else:
                return_data.append((None, traceback.format_exc(), None, None, save))
                self.error(traceback.format_exc())
        finally:
            if not return_result:
                handler.finish()
            handler.close()
            self.info("Spider fetch end" )
            if return_result:
                return return_data

    def _run_condition(self, ruleset, source, save):
        """
        执行条件判断
        """
        haystack = None
        if 'parse' in ruleset:
            data = self._run_parse({"haystack": ruleset['parse']}, source, save.get('base_url'))
            if 'haystack' in data:
                haystack = data['haystack']
            else:
                haystack = data
        elif 'attr' in ruleset:
            assert 'name' in ruleset['attr'] and ruleset['attr']['name'], "Invalid attr setting: name"
            if ruleset['attr']['name'] in save:
                attr = save[ruleset['attr']['name']]
                if isinstance(attr, types.MethodType):
                    args = ruleset['attr'].get('args', [])
                    kwargs = ruleset['attr'].get('kwargs', {})
                    haystack = attr(*args, **kwargs)
                else:
                    haystack = attr
            else:
                self.info("Spider run condition save attr: %s" % ruleset['attr'])
                return False
        if 'type' in ruleset:
            _type = ruleset['type']
            self.debug("Spider run condition haystack type: %s type: %s" % (type(haystack), _type))
            if _type == 'None':
                return (haystack == None and [True] or [False])[0]
            if _type == 'empty':
                return ((haystack != None and not haystack) and [True] or [False])[0]
            if not _type:
                return (not haystack and [True] or [False])[0]
            return (not haystack and [False] or [True])[0]
        if 'value' in ruleset:
            needle = ruleset['value']
            operator = ruleset.get('operator', '$ne')
            self.debug("Spider run condition haystack: %s operator: %s needle: %s" % (haystack, operator, needle))
            if operator == '$gt':
                return (haystack > needle and [True] or [False])[0]
            if operator == '$gte':
                return (haystack >= needle and [True] or [False])[0]
            if operator == '$lt':
                return (haystack < needle and [True] or [False])[0]
            if operator == '$lte':
                return (haystack <= needle and [True] or [False])[0]
            if operator == '$ne':
                return (haystack != needle and [True] or [False])[0]
            if operator == '$in':
                return (haystack in needle and [True] or [False])[0]
            if operator == '$nin':
                return ((not haystack in needle) and [True] or [False])[0]
            return (haystack == needle and [True] or [False])[0]

    def _run_parse(self, rule, source, url=None):
        self.info("Spider run parse start")
        try:
            data = {}
            for k, item in rule.items():
                self.info("Spider run parse: %s => %s" % (k, item))
                for parser_name, r in item.items():
                    parser = utils.load_parser(parser_name, source=source, ruleset=copy.deepcopy(r), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
                    parsed = parser.parse()
                    self.info("Spider run parse matched data: %s" % str(parsed))
                    if parsed:
                        data[k] = parsed
                        break
        finally:
            self.info("Spider run parse end")
        return data

    def get_handler(self, task):
        if hasattr(self, 'handler'):
            return self.handler
        return Loader(self.ctx, task = task, spider = self, no_sync = self.no_sync).load()

    def get_task(self, message, no_check_status = False):
        """
        获取任务详细信息
        """

        return message


    def run_once(self):
        self.info("Spider once starting...")
        message = self.inqueue.get_nowait()
        self.debug("Spider get message: %s" % message)
        task = self.get_task(message)
        self.debug("Spider get task: %s" % task)
        self.fetch(task)
        if hasattr(self, 'handler'):
            del self.handler
        self.info("Spider once end")

    def run(self):
        """
        spider运行方法
        """
        self.info("Spider starting...")

        def queue_loop():
            if not self.inqueue:
                return
            while not self._quit:
                try:
                    message = self.inqueue.get_nowait()
                    task = self.get_task(message)
                    self.fetch(task)
                    time.sleep(0.1)
                except queue.Empty:
                    time.sleep(0.1)
                    continue
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.exception(e)
                    break

        tornado.ioloop.PeriodicCallback(queue_loop, 100, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.info("Spider exiting...")

    def xmlrpc_run(self, port=24444, bind='127.0.0.1'):
        import umsgpack
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = {"message": "xmlrpc is running"}
            return json.dumps(result)
        application.register_function(hello, 'hello')

        def fetch(task):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = None
            try:
                task = json.loads(task)
                ret = self.fetch(task)
                if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                    parsed, broken_exc, last_source, final_url = ret[0]
                else:
                    self.error(ret)
                if last_source:
                    last_source = utils.decode(last_source)
                if parsed:
                    parsed = True
            except :
                broken_exc = traceback.format_exc()
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output}

            return json.dumps(result)
        application.register_function(fetch, 'fetch')

        def get_task(data):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            task = broken_exc = None
            try:
                task = json.loads(data)
                task = self.get_task(task, no_check_status = True)
            except :
                broken_exc = traceback.format_exc()
            output = sys.stdout.read()
            result = {"task": task, "broken_exc": broken_exc, "stdout": output}
            return json.dumps(result)
        application.register_function(get_task, 'task')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.info('spider.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()

    def quit(self):
        self._quit = True
        self._running = False

    def _build_data(self, data, appended):
        if not data:
            data = appended
        else:
            data.update(appended)
        return data
