# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 18:01:26
:version: SVN: $Id: Spider.py 2266 2018-07-06 06:50:15Z zhangyi $
"""
import gc
import time
import traceback

import tornado.ioloop
from cdspider import Component
from cdspider.handler import BaseHandler, Loader
from cdspider.libs.constants import *
from cdspider.libs.tools import *
from six.moves import queue


class Spider(Component):
    """
    爬虫流程实现
    """
    interval = 500

    def __init__(self, context, no_sync = False, handler=None, inqueue = None):
        self._quit = False
        self._running = False
        self.ctx = context
        g = context.obj
        queue = g.get('queue')
        if inqueue == False:
            self.inqueue = None
        else:
            if not inqueue:
                inqueue = QUEUE_NAME_SCHEDULER_TO_SPIDER
            self.inqueue = queue[inqueue]

        self.queue = queue
        self.no_sync = no_sync
        self.sdebug = g.get('sdebug', False)
        self.pcker = g.get('pcker', True)
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
        save = {"base_url": task.get('url')}
        handler = self.get_handler(task)
        if return_result:
            return_data = []
        try:
            self.info("Spider loaded handler: %s" % handler)
            last_source_unid = None
            last_url = None
            last_parsed_unid = None
            self.info("Spider process start")
            handler.init(save)
            self.info("Spider fetch prepare start")
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
                if not handler.response['content']:
                    raise CDSpiderCrawlerError('Spider crawl failed')
                if not handler.response['broken_exc']:
                    unid = utils.md5(handler.response['content'])
                    if last_source_unid == unid or last_url == handler.response['url']:
                        raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=handler.response['url'])
                    last_source_unid = unid
                    validated = handler.validate(save=save)
                    if validated:
                        last_url = handler.response['url']
                        if self.sdebug:
                            self.info("Spider crawl end, source: %s" % utils.remove_whitespace(handler.response["source"]))
                        else:
                            self.info("Spider crawl end")
                        self.info("Spider parse start")
                        handler.parse(save=save)
                        self.info("Spider parse end, result: %s" % str(handler.response["parsed"]))
                        parsed_unid = utils.md5(handler.response['parsed'])
                        if last_parsed_unid == parsed_unid:
                            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=handler.response['url'])
                        last_parsed_unid = parsed_unid

                if isinstance(handler.response['broken_exc'], CONTINUE_EXCEPTIONS):
                    handler.on_continue(handler.response['broken_exc'], save)
                    continue
                elif handler.response['broken_exc']:
                    raise handler.response['broken_exc']

                if return_result:
                    return_data.append((handler.response['parsed'], None, handler.response['content'],
                                        handler.response['url'], save))
                    self.info("Spider next start")
                    handler.on_next(save)
                    self.info("Spider next end")
                    raise CDSpiderCrawlerDebugBroken("DEBUG MODE BROKEN")

                self.info("Spider result start")
                handler.on_result(save)
                self.info("Spider result end")
                self.info("Spider next start")
                handler.on_next(save)
                self.info("Spider next end")
        except Exception as e:
            if not return_result:
                if not isinstance(e, IGNORE_EXCEPTIONS):
                    handler.on_error(e, save)
            else:
                if isinstance(e, (CDSpiderCrawlerReturnBroken,)):
                    return_data.append((handler.response['parsed'], None, handler.response['content'], handler.response['url'], save))
                elif not isinstance(e, IGNORE_EXCEPTIONS):
                    return_data.append((None, traceback.format_exc(), None, None, save))
                    self.error(traceback.format_exc())
        finally:
            handler.close()
            self.info("Spider process end")
            if not return_result:
                handler.finish(save)
            del handler
            del save
            del task
            self.info("Spider fetch end" )
            if return_result:
                return return_data

    def get_handler(self, task):
        if hasattr(self, 'handler'):
            return self.handler
        return Loader(self.ctx, task=task, no_sync=self.no_sync).load()

    def get_task(self, message, no_check_status = False):
        """
        获取任务详细信息
        """

        return message


    def run_once(self):
        self.info("Spider once starting...")
        message = self.inqueue.get_nowait()
        self.debug("Spider got message: %s" % message)
        task = self.get_task(message)
        self.debug("Spider got task: %s" % task)
        self.fetch(task)
        if hasattr(self, 'handler'):
            del self.handler
        self.info("Spider once end")

    def run(self):
        """
        spider运行方法
        """
        self.info("Spider starting...")
        self.t = 0
        def queue_loop():
            if not self.inqueue:
                return
            if self._quit:
                raise SystemExit
            self.t += 1
            try:
                message = self.inqueue.get_nowait()
                self.debug("%s fetch got message %s" % (self.__class__.__name__, message))
                task = self.get_task(message)
                self.fetch(task)
                time.sleep(0.05)
            except queue.Empty:
                time.sleep(0.1)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                self.exception(e)
                time.sleep(0.05)
            finally:
                self.flush()
                gc.collect()
                if self.t > 10:
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

        self.info("Spider exiting...")

    def xmlrpc_run(self, port=24444, bind='127.0.0.1'):
        from cdspider.libs import WSGIXMLRPCApplication

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = {"message": "xmlrpc is running"}
            return json.dumps(result)
        application.register_function(hello, 'hello')

        def fetch(task):
            self.debug("%s rpc got message %s" % (self.__class__.__name__, task))
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            parsed = broken_exc = last_source = final_url = save = errmsg = None
            try:
                task_map = json.loads(task)
                return_result = task_map.pop('return_result', False)
                ret = self.fetch(task_map, return_result)
                if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                    parsed, broken_exc, last_source, final_url, save = ret[0]
                else:
                    self.error(ret)
                if last_source:
                    last_source = utils.decode(last_source)
                if parsed and not return_result:
                    parsed = True
            except Exception as exc:
                errmsg = str(exc)
                broken_exc = traceback.format_exc()
                self.error(broken_exc)
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output, "errmsg": errmsg}

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

        import tornado.httpserver
        import tornado.ioloop
        import tornado.wsgi

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
