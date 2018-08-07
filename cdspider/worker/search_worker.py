#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-14 21:06:24
"""
import traceback
from cdspider.database.base import *
from cdspider.worker import BaseWorker

class SearchWorker(BaseWorker):

    def on_result(self, message):
        if 'uid' in message:
            lastkwid = 0
            maxkwid = self.KeywordsDB.get_max_id()
            while True:
                keywords = self.KeywordsDB.get_new_list(lastkwid)
                for keyword in keywords:
                    self.logger.debug("%s build_search_work_by_site keyword: %s" % (self.__class__.__name__, keyword))
                    self.outqueue.put_nowait({'kwid': keyword['kid'], 'siteid': message['siteid']})
                    if keyword['kid'] > lastkwid:
                        lastkwid = keyword['kid']
                if lastkwid >= maxkwid:
                    self.logger.debug("%s build_search_work_by_site end lastkwid: %s" % (self.__class__.__name__, lastkwid))
                    break
        elif 'kwid' in message:
            projectid = 0
            while True:
                projects = self.ProjectsDB.get_list(where=[("status", ProjectDB.PROJECT_STATUS_ACTIVE), ("pid", "$gt", projectid)])
                i = 0
                for project in projects:
                    self.logger.debug("%s build_search_work_by_kwid project: %s " % (self.__class__.__name__, str(project)))
                    self.build_search_work_by_site(project, kwid)
                    if project['pid'] > projectid:
                        projectid = project['pid']
                    i += 1
                if i == 0:
                    self.logger.debug("%s build_search_work_by_kwid end lastpid: %s" % (self.__class__.__name__, projectid))
                    break

    def build_search_work_by_site(self, project, kwid):
        siteid = 0
        while True:
            sites = self.SitesDB.get_list([("projectid", project['pid']),("sid", "$gt", siteid)], hits = 10)
            i = 0
            for site in sites:
                self.logger.debug("%s build_search_work_by_kwid site: %s" % (self.__class__.__name__, site['sid']))
                self.outqueue.put_nowait({'kwid': kwid, 'siteid': site['sid']})
                if site['sid'] > siteid:
                    siteid = site['sid']
                i += 1
            if i == 0:
                self.logger.debug("%s build_search_work_by_kwid lastsite: %s" % (self.__class__.__name__, siteid))
                break
