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

from cdspider.crawler import BaseCrawler
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

    KEYWORD_ACTIONS = 'actions'
    KEYWORD_CONTINUE = 'continue'
    KEYWORD_IF = 'if'
    KEYWORD_PARSE = 'parse'
    KEYWORD_PARSER = 'parser'
    KEYWORD_ALWAYS = 'always'
    KEYWORD_CRAWLER = 'crawler'
    KEYWORD_PREFIX = 'prefix'
    KEYWORD_SUFFIX = 'suffix'
    KEYWORD_CONDITION = 'condition'

    def __init__(self, inqueue, outqueue, status_queue, requeue, excqueue,
            projectdb, sitetypedb, taskdb, sitedb, resultdb, customdb, uniquedb, urlsdb, attachmentdb, keywordsdb,
            attach_storage = None, handler=None, proxy=None, log_level=logging.WARN):
        self._quit = False
        self._running = False
        self.inqueue = inqueue
        self.outqueue = outqueue
        self.excqueue = excqueue
        self.requeue = requeue
        self.status_queue = status_queue
        self.projectdb = projectdb
        self.sitetypedb = sitetypedb
        self.taskdb = taskdb
        self.sitedb = sitedb
        self.resultdb = resultdb
        self.customdb = customdb
        self.uniquedb = uniquedb
        self.attachmentdb = attachmentdb
        self.urlsdb = urlsdb
        self.keywordsdb = keywordsdb
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
        _crawler = []
        if not task:
            self.logger.debug("Spider fetch exit with no task")
            return
        self.logger.info("Spider fetch start, task: %s" % task)
        last_source = None
        if not "save" in task or not task['save']:
            task['save'] = {}
        task['last_rule'] = None
        save = task['save']
        handler = self.get_handler(task)
        mode = task.get('mode', handler.MODE_DEFAULT)
        if return_result:
            return_data = []
        try:
            self.logger.info("Spider loaded handler: %s" % handler)
            save.setdefault('base_url', task['url'])
            save.setdefault('referer', task['url'])
            save.setdefault('continue_exceptions', handler.continue_exceptions)
            save.setdefault('proxy', self.proxy)
            proxies = self.proxy
            base_request = handler.get_base_request(task, mode)
            task['last_rule'] = base_request
            b_crawler = None
            b_crawler_name = None
            headers = save.get("headers", [])
            cookies = save.get("cookies", [])
            if base_request and (not save.get('view_data', None) or base_request.get(self.KEYWORD_ALWAYS, False)):
                self.logger.info("Spider base request start, rule: %s" % base_request)
                try:
                    b_crawler_name = base_request.get(self.KEYWORD_CRAWLER, 'requests')
                    b_crawler = utils.load_crawler(b_crawler_name, headers=headers, cookies=cookies, proxy=proxies, log_level=self.log_level)
                    _crawler.append(b_crawler)
                    self.logger.info("Spider base request crawler: %s" % b_crawler)
                    callback, result, broken_exc, last_source, final_url = self._process(base_request, b_crawler, save, last_source)
                    self.logger.info("Spider base request result: %s" % str((result, broken_exc, final_url)))
                    for items in result.values():
                        if not 'view_data' in save:
                            save['view_data'] = items
                        else:
                            save['view_data'].update(items)
                    if broken_exc:
                        raise self._broken_exc
                    cookies = b_crawler.get_cookie()
                    headers = b_crawler.get_header()
                    save['base_url'] = final_url
                    save['referer'] = final_url
                    headers['Referer'] = final_url
                    self.logger.info("Spider base request cookis: %s" % cookies)
                    self.logger.info("Spider base request headers: %s" % headers)
                finally:
                    self.logger.info("Spider base request end, rule: %s" % base_request)

            crawler_name = handler.get_crawler(task, mode)
            if b_crawler and b_crawler_name == crawler_name:
                crawler = b_crawler
            else:
                crawler = utils.load_crawler(crawler_name, headers = headers, proxy=proxies, cookies = cookies, log_level=self.log_level)
                _crawler.append(crawler)
            self.logger.info("Spider request crawler: %s" % crawler)
            process = handler.get_process(task, mode, last_source)
            task['last_rule'] = process
            if process:
                self.logger.info("Spider process start, rule: %s" % process)
                referer = process.get('referer', False)
                refresh_base = process.get('rebase', False)
                last_source_unid = None
                last_url = None
                try:
                    while True:
                        callback, result, broken_exc, last_source, final_url = self._process(process, crawler, save, last_source)
                        unid = utils.md5(last_source)
                        if last_source_unid == unid or last_url == final_url:
                            raise CDSpiderCrawlerNoNextPage()
                        last_source_unid = unid
                        last_url = final_url
                        if referer:
                            save['referer'] = final_url
                            crawler.set_header("Referer", final_url)
                        if refresh_base and save.get('rebase', False):
                            save['base_url'] = final_url
                        save['request_url'] = final_url
                        self.logger.info("Spider process result: %s" % str((callback,result,broken_exc, final_url)))
                        if return_result:
                            return_data.append((result, broken_exc, last_source, final_url, save))
                            raise CDSpiderCrawlerBroken("DEBUG MODE BROKEN")
                        else:
                            getattr(handler, callback)(task, result, broken_exc, last_source, mode)
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
                handler.on_error(task, e, mode)
            else:
                return_data.append((None, traceback.format_exc(), None, None, None))
            self.logger.error(traceback.format_exc())
        finally:
            if not return_result:
                handler.finish(task, mode)
            for c in _crawler:
                if isinstance(c, BaseCrawler):
                    c.quit()
            self.logger.info("Spider fetch end, task: %s" % task)
            if return_result:
                return return_data

    def _process(self, rule, crawler, save, last_source):
        """
        执行爬取流程
        """
        data = None
        broken_exc = None
        page_source = None
        final_url = None
        callback = rule.pop('callback', 'on_result')
        if self.KEYWORD_ACTIONS in rule:
            actions = rule['actions']
            data = self._run_actions(crawler, actions, save, last_source)
        final_url = crawler.final_url
        page_source = utils.remove_whitespace(crawler.content or crawler.page_source)
        if self.KEYWORD_CONTINUE in rule:
            is_continue = self._run_continue(rule[self.KEYWORD_CONTINUE], crawler, page_source, save)
            if is_continue:
                callback = rule[self.KEYWORD_CONTINUE].get("callback", "on_continue")
                getattr(handler, callback)(crawler, save)
                return self._process(rule, crawler, save, last_source)
        if self.KEYWORD_IF in rule:
            try:
                self._run_if(rule[self.KEYWORD_IF], crawler, page_source, save)
            except Exception as e:
                broken_exc = e
        if broken_exc:
            return (callback, data, broken_exc, page_source, final_url)
        if self.KEYWORD_PARSE in rule:
            parsed = self._run_parse(rule[self.KEYWORD_PARSE], page_source, final_url)
            data = self._build_data(data, parsed)
        if self.KEYWORD_IF in rule:
            try:
                self._run_if(rule[self.KEYWORD_IF], crawler, page_source, save, fix="suffix")
            except Exception as e:
                broken_exc = e
        return (callback, data, broken_exc, page_source, final_url)

    def _run_actions(self, crawler, actions, save, last_source):
        """
        actions
        """
        self.logger.info("Spider run actions start")
        data = None
        try:
            if not isinstance(actions, list):
                actions = [actions]
            for item in actions:
                action = copy.deepcopy(item)
                self.logger.info("Spider run action: %s" % action)
                parse_rule = action.pop(self.KEYWORD_PARSE, None)
                self._run_action(crawler, action, save, last_source)
                if parse_rule:
                    page_source = utils.remove_whitespace(crawler.content or crawler.page_source)
                    parsed = self._run_parse(parse_rule, page_source, crawler.final_url)
                    if parsed:
                        data = self._build_data(data, parsed)
        finally:
            self.logger.info("Spider run actions end")
        return data

    def _run_action(self, crawler, rule, save, last_source):
        """
        action
        """
        retries = save.get('retries', 3)
        custom_continue_exceptions = save.get('continue_exceptions')
        for k, v in rule.items():
            if not hasattr(crawler, k):
                raise CDSpiderCrawlerNoMethod("Crawler: %s not has method: %s" % (crawler.__class__.__name__, k))
            params = self.url_builder.build(v, last_source, crawler, save)
            if 'url' in params:
                save['request_url'] = params['url']
                if 'proxy' in v and v['proxy'] == 'force' and save['proxy']:
                    params['proxy'] = copy.deepcopy(save['proxy'])
            while retries > 0:
                try:
                    getattr(crawler, k)(**params)
                except CONTINUE_EXCEPTIONS:
                    if 'proxy' in v and v['proxy'] == 'auto' and save['proxy'] and not 'proxy' in params:
                        params['proxy'] = copy.deepcopy(save['proxy'])
                    continue
                except Exception as e:
                    if custom_continue_exceptions and isinstance(e, custom_continue_exceptions):
                        if 'proxy' in v and v['proxy'] == 'auto' and save['proxy'] and not 'proxy' in params:
                            params['proxy'] = copy.deepcopy(save['proxy'])
                        continue
                    else:
                        raise e
                else:
                    break
                finally:
                    retries -= 1

    def _run_if(self, rule, crawler, source, save, fix = 'prefix'):
        """
        if
        """
        self.logger.info("Spider run if start")
        try:
            if not isinstance(rule, list):
                rule = [rule]
            for item in rule:
                self.logger.info("Spider run if: %s" % item)
                if item.get('fix', self.KEYWORD_PREFIX) != fix:
                    continue
                flag = True
                condition = item.get(self.KEYWORD_CONDITION, None)
                if condition:
                    flag = self._run_condition(condition, source, save)
                    self.logger.info("Spider run condition: %s" % flag)
                actions = item.get(self.KEYWORD_ACTIONS)
                if flag:
                    self._run_actions(crawler, actions, save, source)
        finally:
            self.logger.info("Spider run if end")

    def _run_continue(self, rule, crawler, source, save):
        """
        continue
        """
        self.logger.info("Spider run continue start")
        try:
            if not isinstance(rule, list):
                rule = [rule]
            for item in rule:
                self.logger.info("Spider run continue: %s" % item)
                flag = True
                condition = item.get(self.KEYWORD_CONDITION, None)
                if condition:
                    flag = self._run_condition(condition, source, save)
                    self.logger.info("Spider run condition: %s" % flag)
                if flag:
                    self._run_actions(crawler, actions, save, source)
                    return True
            return False
        finally:
            self.logger.info("Spider run continue end")

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
        task['project'].setdefault("name", "Project%s" % task.get("projectid"))
        return load_handler(task=task, spider=self, outqueue=self.outqueue,
                status_queue=self.status_queue, requeue=self.requeue, excqueue=self.excqueue,
                taskdb=self.taskdb,sitedb=self.sitedb, sitetypedb=self.sitetypedb, uniquedb=self.uniquedb,
                resultdb=self.resultdb, urlsdb=self.urlsdb, attachmentdb=self.attachmentdb,
                customdb=self.customdb, log_level=self.log_level, attach_storage = self.attach_storage)

    def get_task(self, message, task=None, no_check_status = False):
        """
        获取任务详细信息
        """
        projectid = int(message['pid'])
        if not task:
            taskid = int(message['id'])
            task = self.taskdb.get_detail(taskid, projectid, True)
            if not task:
                raise CDSpiderDBDataNotFound("Task: %s not found" % taskid)
        if not no_check_status and task.get('status', TaskDB.TASK_STATUS_INIT) != TaskDB.TASK_STATUS_ACTIVE:
            self.logger.debug("Task: %s" % task)
            return None
        if not 'save' in task or not task['save']:
            task['save'] = {}
        project = self.projectdb.get_detail(int(task['projectid']))
        if not project:
            self.status_queue.put_nowait({"projectid": task['projectid'], 'status': ProjectDB.PROJECT_STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Project: %s not found" % task['projectid'])
        if not no_check_status and project.get('status', ProjectDB.PROJECT_STATUS_INIT) != ProjectDB.PROJECT_STATUS_ACTIVE:
            self.logger.debug("Project: %s" % project)
            return None
        if not 'base_request' in project or  not project['base_request']:
            project['base_request'] = {}
        if not 'main_process' in project or not project['main_process']:
            project['main_process'] = {}
        if not 'sub_process' in project or not project['sub_process']:
            project['sub_process'] = {}
        task['project'] = project
        site = self.sitedb.get_detail(int(task['siteid']))
        if not site:
            self.status_queue.put_nowait({"siteid": task['siteid'], 'status': SiteDB.SITE_STATUS_DELETED})
            raise CDSpiderDBDataNotFound("Site: %s not found" % task['siteid'])
        if not no_check_status and site.get('status', SiteDB.SITE_STATUS_INIT) != SiteDB.SITE_STATUS_ACTIVE:
            self.logger.debug("Site: %s" % site)
            return None
        if not 'base_request' in site or not site['base_request']:
            site['base_request'] = {}
        if not 'main_process' in site or not site['main_process']:
            site['main_process'] = {}
        if not 'sub_process' in site or not site['sub_process']:
            site['sub_process'] = {}
        site['attachment_list'] = []
        if  not 'atid' in task or not task['atid']:
            attachment_list = self.attachmentdb.get_list({"siteid": int(task['siteid'])})
            for item in attachment_list:
                site['attachment_list'].append(item)
        task['site'] = site
        if  'atid' in task and task['atid']:
            attachment = self.attachmentdb.get_detail(int(task['atid']))
            if not attachment:
                self.status_queue.put_nowait({"atid": task['atid'], 'status': AttachmentDB.ATTACHMENT_STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Attachment: %s not found" % task['atid'])
            if not no_check_status and attachment.get('status', AttachmentDB.ATTACHMENT_STATUS_INIT) != AttachmentDB.ATTACHMENT_STATUS_ACTIVE:
                self.logger.debug("Attachment: %s" % attachment)
                return None
            task['attachment'] = attachment
            task['project']['script'] = attachment['script']
            task['site']['script'] = None
            task['mode'] = BaseHandler.MODE_ATT
        elif 'urlid' in task and task['urlid']:
            urls = self.urlsdb.get_detail(int(task['urlid']))
            if not urls:
                self.status_queue.put_nowait({"atid": task['atid'], 'status': UrlsDB.ATTACHMENT_STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Url: %s not found" % task['urlid'])
            if not no_check_status and urls.get('status', UrlsDB.URLS_STATUS_INIT) != UrlsDB.URLS_STATUS_ACTIVE:
                self.logger.debug("Urls: %s" % urls)
                return None
            task['save'].setdefault('base_url', urls['url'])
            task['save'].setdefault('referer', urls['url'])
            task['urls'] = urls
        if 'kwid' in task and task['kwid']:
            keywords = self.keywordsdb.get_detail(int(task['kwid']))
            if not keywords:
                self.status_queue.put_nowait({"kwid": task['kwid'], 'status': KeywordsDB.KEYWORDS_STATUS_DELETED})
                raise CDSpiderDBDataNotFound("Keyword: %s not found" % task['kwid'])
            if not no_check_status and keywords.get('status', KeywordsDB.KEYWORDS_STATUS_INIT) != KeywordsDB.KEYWORDS_STATUS_ACTIVE:
                self.logger.debug("Keywords: %s" % keywords)
                return None
            task['save']['hard_data'] = {"keyword": keywords['word']}
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
