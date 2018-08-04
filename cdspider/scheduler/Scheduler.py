#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 18:00:11
:version: SVN: $Id: Scheduler.py 2177 2018-07-04 12:44:06Z zhangyi $
"""
import time
import traceback
import logging
from six.moves import queue as Queue
from cdspider.libs.tools import *
from cdspider.database.base import *
from .status_schedule import StatusSchedule

class Scheduler(object):
    """
    爬虫调度
    """
    LOOP_INTERVAL = 0.1
    EXCEPTION_LIMIT = 3
    DEFAULT_RATE = [7200, "每2小时一次"]

    def __init__(self, newtask_queue, inqueue, outqueue, status_queue, search_work,
            projectdb, taskdb, sitedb, urlsdb, keywordsdb, customdb, attachmentdb,
            rate_map, log_level = logging.WARN):
        self.newtask_queue = newtask_queue
        self.inqueue = inqueue
        self.outqueue = outqueue
        self.status_queue = status_queue
        self.searchwork_queue = search_work
        self.projectdb = projectdb
        self.taskdb = taskdb
        self.sitedb = sitedb
        self.urlsdb = urlsdb
        self.attachmentdb = attachmentdb
        self.keywordsdb = keywordsdb
        self.customdb = customdb
        self.rate_map = rate_map
        self.logger = logging.getLogger("scheduler")
        self.log_level = log_level
        self.logger.setLevel(log_level)
        self._exceptions = 0
        self._quit = False
        self.projects = set()

    def _check_projects(self):
        self.logger.info("Schedule check_projects starting...")
        self.projects = set()
        projectid = 0
        while True:
            projects = self.projectdb.get_list(where=[('pid', "$gt", projectid), ("status", ProjectDB.PROJECT_STATUS_ACTIVE)])
            i = 0
            for project in projects:
                self.logger.debug("Schedule check_projects project: %s " % str(project))
                self.projects.add(project['pid'])
                if project['pid'] > projectid:
                    projectid = project['pid']
                i += 1
            if i == 0:
                self.logger.debug("Schedule check_projects no projects")
                break
        self.logger.info("Schedule check_projects end")

    def _build_attachment_task(self, task):
        self.logger.debug("Schedule build_attachment_task task: %s starting..." % str(task))
        attachment = self.attachmentdb.get_detail(task['atid'])
        if attachment:
            project = {
                'type': ProjectDB.PROJECT_TYPE_ATTACHE,          # 项目类型
                'status': ProjectDB.PROJECT_STATUS_ACTIVE,       # 项目状态
                'script': attachment['script'],                  # 自定义 handler
                'name': "AttachmentProject"
            }
            task['project'] = project
            task['attachment'] = attachment
            handler = load_handler(task=task, spider=None,
                    projectdb=self.projectdb, sitedb=self.sitedb, keywordsdb=self.keywordsdb,
                    urlsdb=self.urlsdb, attachmentdb=self.attachmentdb, taskdb=self.taskdb,
                    log_level=self.log_level)
            handler.build_newtask()
            self.logger.debug("Schedule build_attachment_task success")
        self.logger.debug("Schedule build_attachment_task failed")

    def _build_task(self, task):
        self.logger.info("Schedule build_task task: %s starting..." % str(task))
        if 'siteid' in task and 'kwid' in task:
            keyword = self.keywordsdb.get_detail(task['kwid'])
            site = self.sitedb.get_detail(task['siteid'])
            lastkwid = site.get('lastkwid', 0)
            project = self.projectdb.get_detail(site['projectid'])
            project['name'] = "Project%s" % project['pid']
            task['project'] = project
            task['site'] = site
            task['keyword'] = keyword
        elif 'urlsid' in task:
            urls = self.urlsdb.get_detail(task['urlsid'])
            site = self.sitedb.get_detail(urls['siteid'])
            project = self.projectdb.get_detail(site['projectid'])
            project['name'] = "Project%s" % project['pid']
            task['project'] = project
            task['site'] = site
            task['urls'] = urls
        else:
            return self.logger.debug("Schedule build_task failed")
        handler = load_handler(task=task, spider=None,
            projectdb=self.projectdb, sitedb=self.sitedb, keywordsdb=self.keywordsdb,
            urlsdb=self.urlsdb, attachmentdb=self.attachmentdb, taskdb=self.taskdb,
            log_level=self.log_level)
        handler.build_newtask()
        self.logger.debug("Schedule build_task success")

    def _check_tasks(self):
        self.logger.info("Schedule check_tasks starting...")
        for projectid in self.projects:
            while True:
                newtask_list = self.taskdb.get_init_list(projectid)
                i = 0
                for task in newtask_list:
                    self.logger.debug("Schedule check_tasks task@%s: %s " % (projectid, str(task)))
                    self.plan_task(task, True)
                    i += 1
                if i == 0:
                    self.logger.debug("Schedule check_tasks no newtask@%s" % projectid)
                    break
        self.logger.info("Schedule check_tasks end")

    def plan_task(self, task, init = False, replan = False):
        currenttime = int(time.time())
        if task['rate'] != 0 or init:
            self.send_task(task)
            obj = {
                'queuetime': currenttime,
                'plantime': currenttime if init or replan else currenttime + int(self.rate_map.get(str(task['rate']), self.DEFAULT_RATE)[0])
            }
        if init:
            obj['status'] = TaskDB.TASK_STATUS_ACTIVE
        elif task['rate'] == 0:
            obj['status'] = TaskDB.TASK_STATUS_DELETE
        self.taskdb.update(task['tid'], task['projectid'], obj=obj)

    def send_task(self, task):
        if self.outqueue:
            self.outqueue.put_nowait({"id": task['tid'], 'pid': task['projectid']})

    def _check_cronjob(self):
        self.logger.info("Schedule check_cronjob starting...")
        currenttime = int(time.time())
        for projectid in self.projects:
            while True:
                task_list = self.taskdb.get_plan_list(projectid, currenttime)
                i = 0
                for task in task_list:
                    self.logger.info("Schedule check_cronjob task@%s: %s" % (projectid, str(task)))
                    self.plan_task(task)
                    i += 1
                if i == 0:
                    self.logger.info("Schedule check_cronjob no task@%s" % projectid)
                    break
        self.logger.info("Schedule check_cronjob end")

    def _check_status(self):
        self.logger.info("Schedule check_status starting...")
        if self.status_queue:
            while True:
                try:
                    message = self.status_queue.get_nowait()
                    self.logger.debug("Schedule check_status: %s" % str(message))
                    StatusSchedule(status_queue = self.status_queue, projectdb = self.projectdb, taskdb = self.taskdb, sitedb = self.sitedb, urlsdb = self.urlsdb, attachmentdb = self.attachmentdb, keywordsdb = self.keywordsdb, customdb = self.customdb).schedule(message)
                except Queue.Empty:
                    break
        self.logger.info("Schedule check_status end")

    def _check_newtask(self):
        self.logger.info("Schedule check_newtask starting...")
        if self.newtask_queue:
            while True:
                try:
                    message = self.newtask_queue.get_nowait()
                    self.logger.debug("Schedule check_newtask: %s" % str(message))
                    self._build_task(message)
                except Queue.Empty:
                    break
        self.logger.info("Schedule check_newtask end")

    def _check_retask(self):
        self.logger.info("Schedule check_retask starting...")
        if self.inqueue:
            while True:
                try:
                    message = self.inqueue.get_nowait()
                    self.logger.debug("Schedule check_retask: %s" % str(message))
                    if 'atid' in message:
                        self._build_attachment_task(message)
                    else:
                        self.plan_task(message, replan = True)
                except Queue.Empty:
                    break
        self.logger.info("Schedule check_retask end")

    def newtask(self, task):
        try:
            if not 'urlsid' in task or not task['urlsid']:
                return {"status": 500, "message": "无效的URLS ID"}
                urls = self.urlsdb.get_detail(task['urlsid'])
                if not urls:
                    return {"status": 500, "message": "无效的URLS"}
            if not 'siteid' in task or not task['siteid']:
                return {"status": 500, "message": "无效的站点ID"}
                site = self.sitedb.get_detail(task['siteid'])
                if not site:
                    return {"status": 500, "message": "无效的站点"}
            self.newtask_queue.put_nowait(task)
            return {"status": 200, "message": "Ok"}
        except Exception as e:
            return {"status": 500, "message": "无效的参数", "error": str(e)}

    def status(self, task):
        try:
            if 'keywordid' in task:
                if not task['keywordid']:
                    return {"status": 500, "message": "无效的关键词ID"}
                keyword  = self.keywordsdb.get_detail(task['keywordid'])
                if not keyword:
                    return {"status": 500, "message": "无效的关键词"}
            elif 'projectid' in task:
                if not task['projectid']:
                    return {"status": 500, "message": "无效的项目"}
                project = self.projectdb.get_detail(task['projectid'])
                if not project:
                    return {"status": 500, "message": "无效的站点"}
            elif 'siteid' in task:
                if not task['siteid']:
                    return {"status": 500, "message": "无效的站点ID"}
                site = self.sitedb.get_detail(task['siteid'])
                if not site:
                    return {"status": 500, "message": "无效的站点"}
            elif 'urlsid' in task:
                if not task['urlsid']:
                    return {"status": 500, "message": "无效的URLS ID"}
                urls = self.urlsdb.get_detail(task['urlsid'])
                if not urls:
                    return {"status": 500, "message": "无效的URLS"}
            elif 'attachid' in task:
                if not task['attachid']:
                    return {"status": 500, "message": "无效的附加任务ID"}
                attachment = self.attachmentdb.get_detail(task['attachid'])
                if not attachment:
                    return {"status": 500, "message": "无效的附加任务"}
            else:
                return {"status": 500, "message": "无效的参数"}
            self.status_queue.put_nowait(task)
            return {"status": 200, "message": "Ok"}
        except Exception as e:
            return {"status": 500, "message": "无效的参数", "error": str(e)}

    def search_work(self, task):
        try:
            if 'kwid' in task:
                if not task['kwid']:
                    return {"status": 500, "message": "无效的关键词ID"}
                keyword  = self.keywordsdb.get_detail(task['kwid'])
                if not keyword:
                    return {"status": 500, "message": "无效的关键词"}
            elif 'siteid' in task:
                if not task['siteid']:
                    return {"status": 500, "message": "无效的站点ID"}
                site = self.sitedb.get_detail(task['siteid'])
                if not site:
                    return {"status": 500, "message": "无效的站点"}
            else:
                return {"status": 500, "message": "无效的参数"}
            self.searchwork_queue.put_nowait(task)
            return {"status": 200, "message": "Ok"}
        except Exception as e:
            return {"status": 500, "message": "无效的参数", "error": str(e)}

    def run_once(self):
        """
        scheduler 执行操作
        """
        self.logger.info("Scheduler once starting...")
        self._check_newtask()
        self._check_retask()
        self._check_status()
        self._check_projects()
        self._check_tasks()
        self._check_cronjob()
        self.logger.info("Scheduler once end")

    def run(self):
        """
        scheduler 进程
        """
        self.logger.info("Scheduler starting...")

        while not self._quit:
            try:
                time.sleep(self.LOOP_INTERVAL)
                self.run_once()
                self._exceptions = 0
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break

        self.logger.info("scheduler exit")

    def quit(self):
        self.logger.debug("scheduler quit")
        self._quit = True

    def xmlrpc_run(self, port=23333, bind='127.0.0.1'):
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')

        def hello():
            result = Binary("xmlrpc is running")
            return result
        application.register_function(hello, 'hello')

        def newtask(task):
            ret = self.newtask(task)
            return Binary(ret)
        application.register_function(newtask, 'newtask')

        def status(task):
            ret = self.status(task)
            return Binary(ret)
        application.register_function(status, 'status')

        def search_work(task):
            ret = self.search_work(task)
            return Binary(ret)
        application.register_function(search_work, 'search_work')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.logger.info('schedule.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()
