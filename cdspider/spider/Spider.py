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

from cdspider import Component, DEFAULT_ITEM_SCRIPTS
from cdspider.handler import BaseHandler
from cdspider.exceptions import *
from cdspider.libs import utils
from cdspider.libs import UrlBuilder
from cdspider.libs.tools import *
from cdspider.database.base import *

CONTINUE_EXCEPTIONS = (CDSpiderCrawlerProxyError, CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout, CDSpiderCrawlerNoResponse, CDSpiderCrawlerForbidden)

class Spider(Component):
    """
    爬虫流程实现
    """

    def __init__(self, db, queue, attach_storage = None, handler=None, proxy=None, log_level=logging.WARN):
        self._quit = False
        self._running = False
        self.inqueue = queue.get('scheduler2spider')
        self.status_queue = queue.get('status_queue')
        self.ProjectsDB = db.get('ProjectsDB')
        self.SitesDB = db.get('SitesDB')
        self.UrlsDB = db.get('UrlsDB')
        self.ChannelRulseDB = db.get('UrlsDB')
        self.AttachmentDB = db.get('AttachmentDB')
        self.KeywordsDB = db.get('KeywordsDB')
        self.TaskDB = db.get('TaskDB')
        self.db = db
        self.queue = queue
        self.proxy = proxy
        self.attach_storage = attach_storage
        self.ioloop = tornado.ioloop.IOLoop()
        self.set_handler(handler)
        self.log_level = log_level
        logger = logging.getLogger('spider')
        self.url_builder = UrlBuilder(logger, log_level)
        super(Spider, self).__init__(logger, log_level)
        self.crawler = {}
#        self.crawler = {"requests": utils.load_crawler("requests", log_level=self.log_level)}

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
        if not "save" in task or not task['save']:
            task['save'] = {}
        save = task['save']
        mode = save.get('mode', BaseHandler.MODE_DEFAULT)
        handler = self.get_handler(task)
        if return_result:
            return_data = []
        try:
            self.info("Spider loaded handler: %s" % handler)
            save.setdefault('base_url', task['url'])
            save.setdefault('referer', task['url'])
            save.setdefault('continue_exceptions', handler.continue_exceptions)
            save['proxy'] = copy.deepcopy(self.proxy)
            referer = save.get('flush_referer', False)
            refresh_base = save.get('rebase', False)
            last_source_unid = None
            last_url = None
            self.info("Spider process start")
            try:
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
                    save['proxy'] = copy.deepcopy(self.proxy)
                    last_source, broken_exc, final_url = handler.crawl(save)
                    if isinstance(broken_exc, CONTINUE_EXCEPTIONS):
                        handler.on_continue(broken_exc, save)
                        continue
                    unid = utils.md5(last_source)
                    if last_source_unid == unid or last_url == final_url:
                        raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=final_url)
                    last_source_unid = unid
                    last_url = final_url
                    if referer:
                        save['referer'] = final_url
                        crawler.set_header("Referer", final_url)
                    if refresh_base:
                        save['base_url'] = final_url
                    save['request_url'] = final_url
                    self.info("Spider crawl end, result: %s" % str((last_source, broken_exc, final_url)))
                    self.info('Spider parse start')
                    if not last_source:
                        if broken_exc:
                            raise broken_exc
                        raise CDSpiderCrawlerError('Spider crawl failed')
                    result = handler.parse(last_source, save.get("parent_url", final_url))
                    self.info('Spider parse end, result: %s' % str(result))
                    result = handler.on_result(result, broken_exc, last_source, final_url, return_result)
                    attach_data = None
                    if mode == BaseHandler.MODE_ITEM and handler.current_page == 1:
                        parent_url = save.get("parent_url", None)
                        attach_data = handler.on_attach(last_source, final_url, parent_url, return_result)
                    if return_result:
                        return_data.append((result, broken_exc, last_source, final_url, save, attach_data))

                        raise CDSpiderCrawlerBroken("DEBUG MODE BROKEN")
                    if broken_exc:
                        raise broken_exc
                    if not 'incr_data' in save:
                        break
            finally:
                handler.on_sync()
                self.info("Spider process end")
        except Exception as e:
            if not return_result:
                if 'incr_data' in save and isinstance(save['incr_data'], list) and save['incr_data']:
                    for item in save['incr_data']:
                        if int(item.get('base_page', 1)) >= int(item.get('value', 1)):
                            item['isfirst'] = True
                task['last_source'] = last_source
                handler.on_error(e)
            else:
                return_data.append((None, traceback.format_exc(), None, None, None, None))
                self.error(traceback.format_exc())
        finally:
            if not return_result:
                handler.finish()
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
        task['project'].setdefault("name", "Project%s" % task.get("pid"))
        return load_handler(task = task, crawler = self.crawler, spider = self, db = self.db, queue = self.queue, log_level=self.log_level, attach_storage = self.attach_storage)

    def _get_task_from_attachment(self, message, task, project, no_check_status = False):
        if not task:
            raise CDSpiderDBDataNotFound("Task: %s not found" % message['tid'])
        if not no_check_status and task.get('status', TaskDB.STATUS_INIT) != TaskDB.STATUS_ACTIVE:
            self.debug("Task: %s not active" % task)
            return None
        attachment = task.get('attachment', None) or self.AttachmentDB.get_detail(int(task['aid']))
        if not attachment:
            self.status_queue.put_nowait({"aid": task['aid'], 'status': AttachmentDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Attachment: %s not found" % task['aid'])
        if not no_check_status and attachment.get('status', AttachmentDB.STATUS_INIT) != AttachmentDB.STATUS_ACTIVE:
            self.debug("Attachment: %s" % attachment)
            return None
        task['project'] = project
        task['site'] = {"sid": 0}
        task['attachment'] = attachment
        task['site']['scripts'] = attachment['scripts']
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['save']['mode'] = BaseHandler.MODE_ATT
        task['save']['base_url'] = task['url']
        return task

    def _get_task_from_channel(self, message, task, project, no_check_status = False):
        if not task:
            raise CDSpiderDBDataNotFound("Task: %s not found" % message['tid'])
        if not no_check_status and task.get('status', TaskDB.STATUS_INIT) != TaskDB.STATUS_ACTIVE:
            self.debug("Task: %s not active" % task)
            return None
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['save']['mode'] = BaseHandler.MODE_CHANNEL;
        task['project'] = project
        site = task.get('site', None) or self.SitesDB.get_detail(int(task['sid']))
        if not site:
            self.status_queue.put_nowait({"sid": task['sid'], 'status': SitesDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Site: %s not found" % task['sid'])
        if not no_check_status and site.get('status', SitesDB.STATUS_INIT) != SitesDB.STATUS_ACTIVE:
            self.debug("Site: %s not active" % site)
            return None
        if not 'main_process' in site or not site['main_process']:
            site['main_process'] = {}
        if not 'sub_process' in site or not site['sub_process']:
            site['sub_process'] = {}
        task['site'] = site

        channel = task.get('channel') or self.ChannelRulseDB.get_detail(int(task['crid']))
        if not channel:
            self.status_queue.put_nowait({"crid": task['aid'], 'status': ChannelRulseDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("ChannelRules: %s not found" % task['uid'])
        if not no_check_status and channel.get('status', ChannelRulseDB.STATUS_INIT) != ChannelRulseDB.STATUS_ACTIVE:
            self.debug("ChannelRule: %s" % channel)
            return None
        if not 'process' in channel or not urls['process']:
            urls['process'] = {}
        task['channel'] = channel
        task['save'].setdefault('base_url', task['url'])
        task['save'].setdefault('referer', task['url'])
        return task

    def _get_task_from_project(self, message, task, project, no_check_status = False):
        if not task:
            raise CDSpiderDBDataNotFound("Task: %s not found" % message['tid'])
        if not no_check_status and task.get('status', TaskDB.STATUS_INIT) != TaskDB.STATUS_ACTIVE:
            self.debug("Task: %s not active" % task)
            return None
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['save']['mode'] = message.get('mode', BaseHandler.MODE_DEFAULT);
        task['project'] = project
        site = task.get('site', None) or self.SitesDB.get_detail(int(task['sid']))
        if not site:
            self.status_queue.put_nowait({"sid": task['sid'], 'status': SitesDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Site: %s not found" % task['sid'])
        if not no_check_status and site.get('status', SitesDB.STATUS_INIT) != SitesDB.STATUS_ACTIVE:
            self.debug("Site: %s not active" % site)
            return None
        if not 'main_process' in site or not site['main_process']:
            site['main_process'] = {}
        if not 'sub_process' in site or not site['sub_process']:
            site['sub_process'] = {}
        task['site'] = site
        if 'uid' in task and task['uid']:
            urls = task.get('urls', None) or self.UrlsDB.get_detail(int(task['uid']))
            if not urls:
                self.status_queue.put_nowait({"uid": task['aid'], 'status': UrlsDB.STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Url: %s not found" % task['uid'])
            if not no_check_status and urls.get('status', UrlsDB.STATUS_INIT) != UrlsDB.STATUS_ACTIVE:
                self.debug("Urls: %s" % urls)
                return None
            if not 'main_process' in urls or not urls['main_process']:
                urls['main_process'] = {}
            if not 'sub_process' in urls or not urls['sub_process']:
                urls['sub_process'] = {}
            task['urls'] = urls
        if 'kwid' in task and task['kwid'] and task['url'].find('{keyword}') > 0:
                keywords = task.get('keyword') or self.KeywordsDB.get_detail(int(task['kwid']))
                if not keywords:
                    self.status_queue.put_nowait({"kwid": task['kwid'], 'status': KeywordsDB.STATUS_DELETED})
                    raise CDSpiderDBDataNotFound("Keyword: %s not found" % task['kwid'])
                if not no_check_status and keywords.get('status', KeywordsDB.STATUS_INIT) != KeywordsDB.STATUS_ACTIVE:
                    self.debug("Keywords: %s" % keywords)
                    return None
                task['save']['hard_code'] = [{'name': 'keyword', 'type': 'format', 'value': keywords['word']}]
        task['save'].setdefault('base_url', task['url'])
        task['save'].setdefault('referer', task['url'])
        return task

    def _get_task_from_item(self, message, task, project, no_check_status = False):
        if not task:
            task = {}
        task.update({
            'tid': 0,
            'pid': message['pid'],
            'project': project,
        })
        if not "save" in task or not task['save']:
            task['save'] = {}
        if 'rid' in message and message['rid']:
            task['queue'] = self.queue['scheduler2spider']
            task['queue_message'] = copy.deepcopy(message)
            task['rid'] = message['rid']
            article = self.db['ArticlesDB'].get_detail(message['rid'])
            task['unid'] = {"unid": article['acid'], "ctime": article['ctime']}
            task.update({
                'sid': article['crawlinfo']['sid'],
                'uid': article['crawlinfo'].get('uid', 0),
                'kwid': article['crawlinfo'].get('kwid', 0),
                'rid': article['rid'],
                'url': article['url'],
            })
            task['save']['parent_url'] = article['crawlinfo'].get('url');
            task['save']['base_url'] = article['url']
            task['item'] = article

        subdomain, domain = utils.parse_domain(task['url'])
        parse_rule = task.get("parse_rule", {})
        if not parse_rule and subdomain:
            parse_rule = self.db['ParseRuleDB'].get_detail_by_subdomain("%s.%s" % (subdomain, domain))
        if not parse_rule:
            self.db['ParseRuleDB'].get_detail_by_domain(domain)
        format_params = {"projectname": "Project%s" % message['pid'], "parenthandler": "SiteHandler"}
        if parse_rule and 'scripts' in parse_rule and parse_rule['scripts']:
            keylist = re.findall('\{(\w+)\}', parse_rule['scripts'])
            for key in keylist:
                if key in ('projectname', 'parenthandler'):
                    continue
                else:
                    format_params[key] = '{%s}' % key
            itemscript = parse_rule['scripts']
        else:
            itemscript = DEFAULT_ITEM_SCRIPTS
        task['scripts']= itemscript
        site = task.get('site') or self.SitesDB.get_detail(int(task['sid']))
        if not site:
            self.status_queue.put_nowait({"sid": task['sid'], 'status': SitesDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Site: %s not found" % task['sid'])
        if not no_check_status and site.get('status', SitesDB.STATUS_INIT) != SitesDB.STATUS_ACTIVE:
            self.debug("Site: %s not active" % site)
            return None
        task['site'] = site
        if 'uid' in task and task['uid']:
            urls = task.get('urls', None) or self.UrlsDB.get_detail(int(task['uid']))
            if urls:
                task['urls'] = urls
                format_params["parenthandler"] = "UrlHandler"
        if not 'save' in task or not task['save']:
            task['save'] = {}
        task['save'].setdefault('base_url', task['url'])
        task['save']['mode'] = message['mode'];
        task['save']['retry'] = message.get('retry', 0)
        task['scripts'] = task['scripts'].format(**format_params)
        return task

    def get_task(self, message, task=None, no_check_status = False):
        """
        获取任务详细信息
        """
        mode = message.get('mode', BaseHandler.MODE_DEFAULT)
        pid = message.get('pid')
        taskid = message.get('tid')
        if not task and taskid:
            task = self.TaskDB.get_detail(taskid, pid, True)
        project = (task.get('project', None) if task else None) or self.ProjectsDB.get_detail(pid)
        if not project:
            self.status_queue.put_nowait({"pid": pid, 'status': ProjectsDB.STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Project: %s not found" % pid)
        if not no_check_status and project.get('status', ProjectsDB.STATUS_INIT) != ProjectsDB.STATUS_ACTIVE:
            self.debug("Project: %s not active" % project)
            return None
        if mode == BaseHandler.MODE_ATT:
            task = self._get_task_from_attachment(message, task, project, no_check_status)
        elif mode == BaseHandler.MODE_ITEM:
            task = self._get_task_from_item(message, task, project, no_check_status)
        elif mode == BaseHandler.MODE_CHANNEL:
            task = self._get_task_from_channel(message, task, project, no_check_status)
        else:
            task = self._get_task_from_project(message, task, project, no_check_status)
        if not task:
            return None
        if "incr_data" in task['save'] and task['save']['incr_data']:
            for i in range(len(task['save']['incr_data'])):
                task['save']['incr_data'][i]['value'] = task['save']['incr_data'][i].get('base_page', 1)
        task['save'].setdefault('retry', 0)
        return task


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
            parsed = broken_exc = last_source = final_url = save = attach_task = None
            try:
                task = json.loads(task)
                ret = self.fetch(task, True)
                if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                    parsed, broken_exc, last_source, final_url, save, attach_task = ret[0]
                else:
                    self.error(ret)
                if last_source:
                    last_source = utils.decode(last_source)
            except :
                broken_exc = traceback.format_exc()
            output = sys.stdout.read()
            result = {"parsed": parsed, "broken_exc": broken_exc, "source": last_source, "url": final_url, "save": save, "stdout": output, 'attach_task': attach_task}

            return json.dumps(result)
        application.register_function(fetch, 'fetch')

        def get_task(data):
            r_obj = utils.__redirection__()
            sys.stdout = r_obj
            message = task = broken_exc = None
            try:
                message, task = json.loads(data)
                task = self.get_task(message, task, no_check_status = True)
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
