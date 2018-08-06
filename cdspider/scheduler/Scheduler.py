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

class Scheduler(object):
    """
    爬虫调度
    """
    LOOP_INTERVAL = 0.1
    EXCEPTION_LIMIT = 3
    DEFAULT_RATE = [7200, "每2小时一次"]

    def __init__(self, db,queue,rate_map, log_level = logging.WARN):
        self.db=db
        self.queue=queue
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
        if 'sid' in task:
            site=self.db['SitesDB'].get_detail(task['sid'])
            project=self.db['ProjectsDB'].get_detail(site['pid'])
            task['site']=site
            task['project']=project
        elif 'kwid' in task:
            keyword=self.db['KeywordsDB'].get_detail(task['kwid'])
            project=self.db['ProjectsDB'].get_detail(keyword['pid'])
            task['keyword']=keyword
            task['project']=project
        elif 'uid' in task:
            url=self.db['UrlsDB'].get_detail(task['uid'])
            site=self.db['SitesDB'].get_detail(url['sid'])
            project=self.db['ProjectsDB'].get_detail(site['pid'])
            task['url']=url
            task['site']=site
            task['project']=project
        elif 'aid' in task:
            attachment=self.db['AttachmentDB'].get_detail(task['aid'])
            project=self.db['ProjectsDB'].get_detail(attachment['pid'])
            task['attachment']=attachment
            task['project']=project
            task['site'] = {"sid": 0}
            task['site']['scripts'] = attachment['scripts']
        else:
            return self.logger.debug("Schedule NewTask failed")
        handler=load_handler(task, spider=None)
        handler.newtask()
        self.logger.debug("Schedule build_task success")

    def _check_tasks(self):
        self.logger.info("Schedule check_tasks starting...")
        for projectid in self.projects:
            while True:
                newtask_list = self.db['TaskDB'].get_list(projectid,where={'status':TaskDB.STATUS_ACTIVE},sort=[('plantime', 1)])
                i = 0
                for task in newtask_list:
                    self.logger.debug("Schedule check_tasks task@%s: %s " % (projectid, str(task)))
                    obj={}
                    if task['aid']==0:
                        obj['mode']='list'
                    else:
                        obj['mode']='att'
                    obj['pid']=task['pid']
                    obj['tid']=task['tid']
                    self.queue['scheduler2spider'].put_nowait(obj)
                    self.plan_task(task)
                    i += 1
                if i == 0:
                    self.logger.debug("Schedule check_tasks no newtask@%s" % projectid)
                    break
        self.logger.info("Schedule check_tasks end")

    def plan_task(self, task):
        currenttime = int(time.time())
        if task['rate'] != 0 or init:
            self.send_task(task)
            obj = {
                'queuetime': currenttime,
                'plantime': currenttime+task['rate']
            }
        self.db['TaskDB'].update(task['tid'], task['projectid'], obj=obj)

    def send_task(self, task):
        if self.outqueue:
            self.outqueue.put_nowait({"id": task['tid'], 'pid': task['projectid']})



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
                time.sleep(self.LOOP_INTERVAL)
                self.status_run_once()
                self._exceptions = 0
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break
    
    def status_run_once(self):
        self.logger.info("status_schedule once starting...")
        StatusSchedule=StatusSchedule(db,queue,self.rate_map)
        q_data=self.queue['status_queue'].get_nowait()
        try:
            q_data=json.loads(q_data)
        except:
            self.logger.error("status_schedule get queue data is not json")
            return
        if 'sid' in q_data:
            pid=self.db['SitesDB'].get_detail(q_data['sid'])['pid']
            StatusSchedule.schedule(q_data, 'SitesDB','sid',pid)
        elif 'uid' in q_data:
            sid=self.db['UrlsDB'].get_detail(q_data['uid'])['sid']
            pid=self.db['SitesDB'].get_detail(q_data['sid'])['pid']
            StatusSchedule.schedule(q_data, 'UrlsDB','uid',pid)
        elif 'kwid' in q_data:
            pid=self.db['KeywordsDB'].get_detail(q_data['wid'])['pid']
            StatusSchedule.schedule(q_data, 'KeywordsDB','wid',pid)
        elif 'pid' in q_data:
           StatusSchedule.schedule(q_data, 'ProjectsDB','pid',q_data['pid'])
        elif 'aid' in q_data:
            pid=self.db['AttachmentDB'].get_detail(q_data['aid'])['pid']
            StatusSchedule.schedule(q_data, 'AttachmentDB','aid',pid)
        else:
            return self.logger.debug("Schedule status_task failed")
        self.logger.info("status_schedule once end")
        
#     def _status_udpate_mongo(self,data,db_name,id_type,pid):
#         obj={}
#         if 'rate' in data:
#             rate=data['data']
#             try:
#                 obj['rate']=int(rate)
#                 obj['plantime']=int(time.time)+obj['rate']
#             except:
#                 pass
#             
#         if 'status' in data:
#             status=data['status']
#             try:
#                 obj['status']=int(status)
#                 if obj['status']==1:
#                     obj['plantime']=int(time.time)
#             except:
#                 pass
#                 
#         for item in self.db['TaskDB'].get_list(pid,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}):
#             if item['status']==Base.STATUS_INIT:
#                 self.db['TaskDB'].update(item['tid'],pid,obj=obj)
#                 self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data['id_type']})
#             elif item['status']==Base.STATUS_ACTIVE:
#                 if item['rate']>obj['rate']:
#                     self.db['TaskDB'].update(item['tid'],pid,obj=obj)
#             else:
#                 self.logger.error("Schedule status value is error")
#             
#         return self.logger.debug("Schedule update success")
    
    def newTask_run(self):
        """
        newTask_schedule 进程
        """
        self.logger.info("newTask_schedule starting...")

        while not self._quit:
            try:
                time.sleep(self.LOOP_INTERVAL)
                self.newTask_run_once()
                self._exceptions = 0
            except KeyboardInterrupt:
                break
            except:
                self.logger.error(traceback.format_exc())
                self._exceptions += 1
                if self._exceptions > self.EXCEPTION_LIMIT:
                    break
    
    def newTask_run_once(self):
        self.logger.info("newTask_schedule once starting...")
        q_data=self.queue['newtask_queue'].get_nowait()
        try:
            q_data=json.loads(q_data)
        except:
            self.logger.error("newTask_schedule get queue data is not json")
            return
        self._build_task(task)
        self.logger.info("newTask_schedule once end")
