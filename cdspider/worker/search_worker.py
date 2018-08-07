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

    inqueue_key = 'search_work'

    def on_result(self, message):
        if 'uid' in message:
            lastkwid = 0
            urls = self.db['UrlsDB'].get_detail(message['uid'])
            while True:
                keywords = self.db['KeywordsDB'].get_new_list(lastkwid, where = {'pid': urls['pid']})
                for keyword in keywords:
                    self.logger.debug("%s build_search_work_by_site keyword: %s" % (self.__class__.__name__, keyword))
                    self.queue['newtask_queue'].put_nowait({'kwid': keyword['kwid'], 'uid': message['uid']})
                    if keyword['kwid'] > lastkwid:
                        lastkwid = keyword['kwid']
                if lastkwid >= maxkwid:
                    self.logger.debug("%s build_search_work_by_site end lastkwid: %s" % (self.__class__.__name__, lastkwid))
                    break
        elif 'kwid' in message:
            keyword = self.db['KeywordsDB'].get_detail(message['kwid'])
            lastsid = 0
            while True:
                sites = self.db['SitesDB'].get_new_list(lastsid, keyword['pid'])
                i = 0
                for site in sites:
                    self.logger.debug("%s build_search_work_by_kwid project: %s " % (self.__class__.__name__, str(project)))
                    self.build_search_work_by_site(site, kwid)
                    if site['sid'] > lastsid:
                        lastsid = site['sid']
                    i += 1
                if i == 0:
                    self.logger.debug("%s build_search_work_by_kwid end lastsid: %s" % (self.__class__.__name__, lastsid))
                    break

    def build_search_work_by_site(self, site, kwid):
        lastuid = 0
        while True:
            urls = self.db['UrlsDB'].get_new_list(lastuid, site['sid'])
            i = 0
            for url in urls:
                self.logger.debug("%s build_search_work_by_kwid urls: %s" % (self.__class__.__name__, url['uid']))
                self.queue['newtask_queue'].put_nowait({'kwid': kwid, 'uid': url['uid']})
                if url['uid'] > lastuid:
                    lastuid = url['uid']
                i += 1
            if i == 0:
                self.logger.debug("%s build_search_work_by_kwid lastuid: %s" % (self.__class__.__name__, lastuid))
                break
