#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 18:01:26
:version: SVN: $Id: Spider.py 2266 2018-07-06 06:50:15Z zhangyi $
"""
import logging
import traceback
import copy
import tornado.ioloop
from six.moves import queue

from cdspider.handler import BaseHandler
from cdspider.exceptions import *
from cdspider.libs import utils
from cdspider.libs import UrlBuilder
from cdspider.libs.tools import *
from cdspider.database.base import *

CONTINUE_EXCEPTIONS = (CDSpiderCrawlerProxyError, CDSpiderCrawlerTimeout, CDSpiderCrawlerNoResponse)

class Spider():
    """
    爬虫流程实现
    """

    def __init__(self, db, queue, attach_storage = None, handler=None, proxy=None, log_level=logging.WARN):
        self._quit = False
        self._running = False
        self.inqueue = queue.get('schedule2spider')
        self.outqueue = queue.get('schedule2spider')
        self.status_queue = queue.get('status_queue')
        self.ProjectsDB = db.get('ProjectsDB')
        self.SitesDB = db.get('SitesDB')
        self.UrlsDB = db.get('UrlsDB')
        self.AttachmentDB = db.get('AttachmentDB')
        self.KeywordsDB = db.get('KeywordsDB')
        self.TaskDB = db.get('TaskDB')
        self.db = db
        self.queue = queue
        self.proxy = proxy
        self.attach_storage = attach_storage
        self.logger = logging.getLogger('spider')
        self.log_level = log_level
        self.logger.setLevel(log_level)
        self.ioloop = tornado.ioloop.IOLoop()
        self.set_handler(handler)
        self.url_builder = UrlBuilder(self.logger, self.log_level)

    def set_handler(self, handler):
        if handler and isinstance(handler, BaseHandler):
            self.handler = handler

    def fetch(self, task, return_result = False):
        """
        抓取操作
        """
        if not task:
            self.logger.debug("Spider fetch exit with no task")
            return
        self.logger.info("Spider fetch start, task: %s" % task)
        last_source = None
        if not "save" in task or not task['save']:
            task['save'] = {}
        save = task['save']
        mode = save.get('mode', BaseHandler.MODE_DEFAULT)
        handler = self.get_handler(task)
        if return_result:
            return_data = []
        try:
            self.logger.info("Spider loaded handler: %s" % handler)
            save.setdefault('base_url', task['url'])
            save.setdefault('referer', task['url'])
            save.setdefault('continue_exceptions', handler.continue_exceptions)
            save.setdefault('proxy', self.proxy)
            referer = save.get('flush_referer', False)
            refresh_base = save.get('rebase', False)
            last_source_unid = None
            last_url = None
            try:
                self.logger.info("Spider fetch prepare start")
                handler.prepare(save)
                self.logger.info("Spider fetch prepare end")
                while True:
                    self.logger.info('Spider crawl start')
                    last_source, broken_exc, final_url = handler.crawl(save)
                    unid = utils.md5(last_source)
                    if last_source_unid == unid or last_url == final_url:
                        raise CDSpiderCrawlerNoNextPage()
                    last_source_unid = unid
                    last_url = final_url
                    if referer:
                        save['referer'] = final_url
                        crawler.set_header("Referer", final_url)
                    if refresh_base:
                        save['base_url'] = final_url
                    save['request_url'] = final_url
                    self.logger.info("Spider crawl end, result: %s" % str((last_source, broken_exc, final_url)))
                    self.logger.info('Spider parse start')
                    result = handler.parse(last_source, save.get("parent_url", final_url))
                    if return_result:
                        return_data.append((result, broken_exc, last_source, final_url, save))
                        raise CDSpiderCrawlerBroken("DEBUG MODE BROKEN")
                    else:
                        handler.on_result(result, broken_exc, last_source, final_url)
                        if mode == self.MODE_ITEM and handler.current_page == 1:
                            handler.on_attach(last_source, save.get("parent_url", final_url))
                        if broken_exc:
                            raise broken_exc
                    if not 'incr_data' in save:
                        break
            finally:
                self.logger.info("Spider process end, rule: %s" % process)
        except Exception as e:
            if 'incr_data' in save and isinstance(save['incr_data'], list) and save['incr_data']:
                for item in save['incr_data']:
                    if int(item.get('base_page', 1)) >= int(item.get('value', 1)):
                        item['first'] = True
            if not return_result:
                task['last_source'] = last_source
                handler.on_error(e)
            else:
                return_data.append((None, traceback.format_exc(), None, None, None))
            self.logger.error(traceback.format_exc())
        finally:
            if not return_result:
                handler.finish()
            self.logger.info("Spider fetch end, task: %s" % task)
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
                self.logger.info("Spider run condition save attr: %s" % ruleset['attr'])
                return False
        if 'type' in ruleset:
            _type = ruleset['type']
            self.logger.debug("Spider run condition haystack type: %s type: %s" % (type(haystack), _type))
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
            self.logger.debug("Spider run condition haystack: %s operator: %s needle: %s" % (haystack, operator, needle))
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
        self.logger.info("Spider run parse start")
        try:
            data = {}
            for k, item in rule.items():
                self.logger.info("Spider run parse: %s => %s" % (k, item))
                for parser_name, r in item.items():
                    parser = utils.load_parser(parser_name, source=source, ruleset=copy.deepcopy(r), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
                    parsed = parser.parse()
                    self.logger.info("Spider run parse matched data: %s" % str(parsed))
                    if parsed:
                        data[k] = parsed
                        break
        finally:
            self.logger.info("Spider run parse end")
        return data

    def get_handler(self, task):
        if hasattr(self, 'handler'):
            return self.handler
        task['project'].setdefault("name", "Project%s" % task.get("pid"))
        return load_handler(task = task, db = self.db, queue = self.queue, log_level=self.log_level, attach_storage = self.attach_storage)

    def _get_task_from_attachment(self, message, task, project, no_check_status = False):
        if not task:
            raise CDSpiderDBDataNotFound("Task: %s not found" % message['tid'])
        if not no_check_status and task.get('status', TaskDB.STATUS_INIT) != TaskDB.STATUS_ACTIVE:
            self.logger.debug("Task: %s not active" % task)
            return None
        attachment = self.AttachmentDB.get_detail(int(message['aid']))
        if not attachment:
            self.status_queue.put_nowait({"aid": task['aid'], 'status': AttachmentDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Attachment: %s not found" % task['aid'])
        if not no_check_status and attachment.get('status', AttachmentDB.STATUS_INIT) != AttachmentDB.STATUS_ACTIVE:
            self.logger.debug("Attachment: %s" % attachment)
            return None
        task['project'] = project
        task['site'] = {"sid": 0}
        task['attachment'] = attachment
        task['site']['scripts'] = attachment['scripts']
        if not 'save' in task or not task['save']:
            task['save'] = {}
        tasl['save']['mode'] = BaseHandler.MODE_ATT
        tasl['save']['base_url'] = task['url']
        return task

    def _get_task_from_project(self, message, task, project, no_check_status = False):
        if not task:
            raise CDSpiderDBDataNotFound("Task: %s not found" % message['tid'])
        if not no_check_status and task.get('status', TaskDB.STATUS_INIT) != TaskDB.STATUS_ACTIVE:
            self.logger.debug("Task: %s not active" % task)
            return None
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['save']['mode'] = message.get('mode', BaseHandler.MODE_DEFAULT);
        task['project'] = project
        site = self.SitesDB.get_detail(int(task['sid']))
        if not site:
            self.status_queue.put_nowait({"sid": task['sid'], 'status': SitesDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Site: %s not found" % task['sid'])
        if not no_check_status and site.get('status', SitesDB.STATUS_INIT) != SitesDB.STATUS_ACTIVE:
            self.logger.debug("Site: %s not active" % site)
            return None
        if not 'main_process' in site or not site['main_process']:
            site['main_process'] = {}
        if not 'sub_process' in site or not site['sub_process']:
            site['sub_process'] = {}
        task['site'] = site
        if 'uid' in task and task['uid']:
            urls = self.UrlsDB.get_detail(int(task['uid']))
            if not urls:
                self.status_queue.put_nowait({"aid": task['aid'], 'status': UrlsDB.STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Url: %s not found" % task['uid'])
            if not no_check_status and urls.get('status', UrlsDB.STATUS_INIT) != UrlsDB.STATUS_ACTIVE:
                self.logger.debug("Urls: %s" % urls)
                return None
            if not 'main_process' in urls or not urls['main_process']:
                urls['main_process'] = {}
            if not 'sub_process' in urls or not urls['sub_process']:
                urls['sub_process'] = {}
            task['urls'] = urls
        if 'kwid' in task and task['kwid']:
            keywords = self.KeywordsDB.get_detail(int(task['kwid']))
            if not keywords:
                self.status_queue.put_nowait({"kwid": task['kwid'], 'status': KeywordsDB.STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Keyword: %s not found" % task['kwid'])
            if not no_check_status and keywords.get('status', KeywordsDB.STATUS_INIT) != KeywordsDB.STATUS_ACTIVE:
                self.logger.debug("Keywords: %s" % keywords)
                return None
        task['save'].setdefault('base_url', task['url'])
        task['save'].setdefault('referer', task['url'])
        return task

    def _get_task_from_item(self, message, task, project, no_check_status = False):
        if not task:
            task = {}
        task.update({
                'pid': message['pid'],
                'project': project,
                'url': message['url'],
            })
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['rid'] = message['rid']
        task['unid'] = message['unid']
        task['save']['mode'] = message['mode'];
        task['save']['parent_url'] = message['parent_url'];
        task['save']['base_url'] = message['url']
        task['item'] = message['save']
        return task

    def get_task(self, message, task=None, no_check_status = False):
        """
        获取任务详细信息
        """
        mode = message.get('mode', BaseHandler.MODE_DEFAULT)
        pid = message.get('pid')
        taskid = message.get('tid')
        if task is None and taskid:
            task = self.TaskDB.get_detail(taskid, pid, True)
        project = self.ProjectsDB.get_detail(int(task['pid']))
        if not project:
            self.status_queue.put_nowait({"pid": pid, 'status': ProjectsDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Project: %s not found" % pid)
        if not no_check_status and project.get('status', ProjectsDB.STATUS_INIT) != ProjectsDB.STATUS_ACTIVE:
            self.logger.debug("Project: %s not active" % project)
            return None
        if mode == BaseHandler.MODE_ATT:
            task = self._get_task_from_attachment(message, task, project, no_check_status)
        elif mode == BaseHandler.MODE_ITEM:
            task = self._get_task_from_item(message, task, project, no_check_status)
        else:
            task = self._get_task_from_project(message, task, project, no_check_status)
        if "incr_data" in task['save'] and task['save']['incr_data']:
            for i in range(len(task['save']['incr_data'])):
                task['save']['incr_data'][i]['value'] = task['save']['incr_data'][i].get('base_page', 1)
        return task


    def run_once(self):
        self.logger.info("Spider once starting...")
        message = self.inqueue.get_nowait()
        self.logger.debug("Spider get message: %s" % message)
        task = self.get_task(message)
        self.logger.debug("Spider get task: %s" % task)
        self.fetch(task)
        if hasattr(self, 'handler'):
            del self.handler
        self.logger.info("Spider once end")

    def run(self):
        """
        spider运行方法
        """
        self.logger.info("Spider starting...")

        def queue_loop():
            if not self.inqueue:
                return
            while not self._quit:
                try:
                    if self.outqueue.full():
                        break
                    message = self.inqueue.get_nowait()
                    task = self.get_task(message)
                    self.fetch(task)
                except queue.Empty:
                    break
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.logger.exception(e)
                    break

        tornado.ioloop.PeriodicCallback(queue_loop, 100, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        self.logger.info("Spider exiting...")

    def xmlrpc_run(self, port=24444, bind='127.0.0.1'):
        import umsgpack
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = Binary(umsgpack.packb("xmlrpc is running"))
            return result
        application.register_function(hello, 'hello')

        def fetch(task):
            ret = self.fetch(task, True)
            if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                result, broken_exc, last_source, final_url, save = ret[0]
            else:
                self.logger.error(ret)
            last_source = utils.decode(last_source)
            result = (result, broken_exc, last_source, final_url, save)
            result = Binary(umsgpack.packb(result))
            return result
        application.register_function(fetch, 'fetch')

        def get_task(data):
            message, task = data
            task = self.get_task(message, task, no_check_status = True)
            return Binary(umsgpack.packb(task))
        application.register_function(get_task, 'task')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.logger.info('spider.xmlrpc listening on %s:%s', bind, port)
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
