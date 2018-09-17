#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 18:00:11
"""
import time
import traceback
import logging
from six.moves import queue as Queue
from cdspider.libs.tools import *
from cdspider.database.base import *
from .status_schedule import StatusSchedule
from six.moves import queue

class Scheduler(object):
    """
    爬虫调度
    """
    EXCEPTION_LIMIT = 3
    DEFAULT_RATE = [7200, "每2小时一次"]
    DEFAULT_INTERVAL = 0.1

    def __init__(self, db,queue,rate_map, log_level = logging.WARN, interval = DEFAULT_INTERVAL):
        self.db=db
        self.queue=queue
        self.rate_map = rate_map
        self.logger = logging.getLogger("scheduler")
        self.log_level = log_level
        self.logger.setLevel(log_level)
        self._exceptions = 0
        self._quit = False
        self.projects = set()
        self.interval = int(interval)

    def _check_projects(self):
        self.logger.info("Schedule check_projects starting...")
        self.projects = set()
        projectid = 0
        while True:
            projects = self.db['ProjectsDB'].get_list(where=[('pid', "$gt", projectid), ("status", Base.STATUS_ACTIVE)])
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


    def _build_task(self, task):
        self.logger.info("Schedule build_task task: %s starting..." % str(task))
        if 'kwid' in task and task['kwid'] and 'uid' in task and task['uid']:
            url=self.db['UrlsDB'].get_detail(task['uid'])
            site=self.db['SitesDB'].get_detail(url['sid'])
            project=self.db['ProjectsDB'].get_detail(site['pid'])
            keyword = self.db['KeywordsDB'].get_detail(task['kwid'])
            task['urls']=url
            task['site']=site
            task['project']=project
            task['keyword'] = keyword
        elif 'kwid' in task and task['kwid']:
            self.queue['search_work'].put_nowait({'kwid':task['kwid']})
            return
        elif 'uid' in task and task['uid']:
            url=self.db['UrlsDB'].get_detail(task['uid'])
            site=self.db['SitesDB'].get_detail(url['sid'])
            if site['type']=='2':
                self.queue['search_work'].put_nowait({'uid':task['uid']})
                return
            project=self.db['ProjectsDB'].get_detail(site['pid'])
            task['urls']=url
            task['site']=site
            task['project']=project
        elif 'aid' in task and task['aid']:
            attachment=self.db['AttachmentDB'].get_detail(task['aid'])
            project=self.db['ProjectsDB'].get_detail(attachment['pid'])
            task['attachment']=attachment
            task['project']=project
            task['site'] = {"sid": 0}
            task['site']['scripts'] = attachment['scripts']
            task['save']={}
        elif 'crid' in task and task['crid']:
            channel=self.db['ChannelRulesDB'].get_detail(task['crid'])
            site = self.db['SitesDB'].get_detail(channel['sid'])
            project=self.db['ProjectsDB'].get_detail(site['pid'])
            task['channel'] = attachment
            task['site'] = site
            task['project'] = project
            task['site'] = {"sid": 0}
            task['site']['scripts'] = attachment['scripts']
            task['save']={}
        else:
            return self.logger.debug("Schedule NewTask failed")
        handler=load_handler(task, db=self.db,queue=self.queue)
        handler.newtask()
        self.logger.debug("Schedule build_task success")

    def _check_tasks(self):
        self.logger.info("Schedule check_tasks starting...")
        for projectid in self.projects:
            while True:
                newtask_list = self.db['TaskDB'].get_list(projectid,
                                                          where={'$and':[{'status':TaskDB.STATUS_ACTIVE},{'plantime':{'$lte':int(time.time())}},{'$or':[{'expire':0},{'expire':{'$gt':int(time.time())}}]}]},
                                                          sort=[('plantime', 1)],hits=200)
                i = 0
                for task in newtask_list:
                    self.logger.debug("Schedule check_tasks task@%s: %s " % (projectid, str(task)))
                    obj={}
                    if task['aid'] > 0:
                        obj['mode']='att'
                    elif 'crid' in task and task['crid'] > 0:
                        obj['mode'] = 'channel'
                    else:
                        obj['mode']='list'
                    obj['pid']=task['pid']
                    obj['tid']=task['tid']
                    self.queue['scheduler2spider'].put_nowait(obj)
                    self.plan_task(task, False if task['plantime'] > 0 else True)
                    i += 1
                if i == 0:
                    self.logger.debug("Schedule check_tasks no newtask@%s" % projectid)
                    break
                time.sleep(1)
        self.logger.info("Schedule check_tasks end")

    def plan_task(self, task, init = False, replan = False):
        currenttime = int(time.time())
        if task['rate'] != 0 or init:
#             self.send_task(task)
            obj = {
                'queuetime': currenttime,
                'plantime': currenttime if init or replan else currenttime + int(self.rate_map.get(str(task['rate']), self.DEFAULT_RATE)[0])
            }
        self.db['TaskDB'].update(task['tid'], task['pid'], obj=obj)

    def send_task(self, task):
        if self.queue['scheduler2spider']:
            self.queue['scheduler2spider'].put_nowait({"id": task['tid'], 'pid': task['projectid']})



    def run_once(self):
        """
        scheduler 执行操作
        """
        self.logger.info("Scheduler once starting...")
        self._check_projects()
        self._check_tasks()
        self.logger.info("Scheduler once end")

    def run(self):
        """
        scheduler 进程
        """
        self.logger.info("Scheduler starting...")

        while not self._quit:
            try:
                self.run_once()
                self._exceptions = 0
                time.sleep(self.interval)
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break
                time.sleep(self.interval)

        self.logger.info("scheduler exit")

    def quit(self):
        self.logger.debug("scheduler quit")
        self._quit = True



        def newtask(task):
            ret = self.newtask(task)
            return Binary(ret)
        application.register_function(newtask, 'newtask')


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


    def status_run(self):
        """
        newTask_schedule 进程
        """
        self.logger.info("newTask_schedule starting...")

        while not self._quit:
            try:
                self.status_run_once()
                self._exceptions = 0
                time.sleep(self.interval)
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break
                time.sleep(self.interval)

    def status_run_once(self):
        self.logger.info("status_schedule once starting...")
        try:
            q_data=self.queue['status_queue'].get_nowait()
            for k,v in q_data.items():
                q_data[k]=int(v)
        except queue.Empty:
            time.sleep(0.5)
            return
        statusSchedule=StatusSchedule(self.db,self.queue,self.rate_map)
        if 'sid' in q_data:
            pid=(self.db['SitesDB'].get_detail(q_data['sid']) or {"pid": 0})['pid']
            if not pid:
                return self.logger.warn("not pid: %s" % q_data)
            statusSchedule.schedule(q_data,'sid',pid)
        elif 'uid' in q_data:
            sid=(self.db['UrlsDB'].get_detail(q_data['uid']) or {"sid": 0})['sid']
            if not sid:
                return self.logger.warn("not sid: %s" % q_data)
            pid=(self.db['SitesDB'].get_detail(sid) or {"pid": 0})['pid']
            if not pid:
                return self.logger.warn("not pid: %s" % q_data)
            statusSchedule.schedule(q_data,'uid',pid)
        elif 'kwid' in q_data:
            pid=(self.db['KeywordsDB'].get_detail(q_data['kwid']) or {"pid": 0})['pid']
            if not pid:
                return self.logger.warn("not pid: %s" % q_data)
            statusSchedule.schedule(q_data,'kwid',pid)
        elif 'pid' in q_data:
           statusSchedule.schedule(q_data,'pid',q_data['pid'])
        elif 'aid' in q_data:
            pid=(self.db['AttachmentDB'].get_detail(q_data['aid']) or {"pid": 0})['pid']
            if not pid:
                return self.logger.warn("not pid: %s" % q_data)
            statusSchedule.schedule(q_data,'aid',pid)
        elif 'crid' in q_data:
            sid=(self.db['ChannelRulesDB'].get_detail(q_data['crid']) or {"sid": 0})['sid']
            if not sid:
                return self.logger.warn("not sid: %s" % q_data)
            pid=(self.db['SitesDB'].get_detail(sid) or {"pid": 0})['pid']
            if not pid:
                return self.logger.warn("not pid: %s" % q_data)
            statusSchedule.schedule(q_data, 'crid', pid)
        else:
            return self.logger.debug("Schedule status_task failed")
        self.logger.info("status_schedule once end")

    def newTask_run(self):
        """
        newTask_schedule 进程
        """
        self.logger.info("newTask_schedule starting...")

        while not self._quit:
            try:
                self.newTask_run_once()
                self._exceptions = 0
                time.sleep(self.interval)
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break
                time.sleep(self.interval)

    def newTask_run_once(self):
        try:
            self.logger.info("newTask_schedule once starting...")
            q_data=self.queue['newtask_queue'].get_nowait()
            for k,v in q_data.items():
                try:
                    q_data[k]=int(v)
                except:
                    pass
            self._build_task(q_data)
            self.logger.info("newTask_schedule once end")
        except queue.Empty:
            time.sleep(0.5)
            return
