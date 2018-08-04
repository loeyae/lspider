#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-23 14:58:57
:version: SVN: $Id: status_schedule.py 2336 2018-07-08 05:43:35Z zhangyi $
"""
from cdspider.database.base import *

class StatusSchedule(object):
    """
    put you comment
    """
    def __init__(self, status_queue, projectdb, taskdb, sitedb, urlsdb, attachmentdb, keywordsdb, customdb):
        self.status_queue = status_queue
        self.projectdb = projectdb
        self.taskdb = taskdb
        self.sitedb = sitedb
        self.urlsdb = urlsdb
        self.attachmentdb = attachmentdb
        self.keywordsdb = keywordsdb
        self.customdb = customdb

    def schedule(self, message):
        if 'keywordid' in message:
            self.schedule_keyword(**message)
        elif 'projectid' in message:
            self.schedule_project(**message)
        elif 'siteid' in message:
            self.schedule_site(**message)
        elif 'urlsid' in message:
            self.schedule_urls(**message)
        elif 'attachid' in message:
            self.schedule_attachment(**message)

    def schedule_keyword(self, keywordid, status):
        project_list = self.projectdb.get_list(where={'type': ProjectDB.PROJECT_TYPE_SEARCH})
        if status == KeywordsDB.KEYWORDS_STATUS_INIT:
            for item in project_list:
                self.taskdb.enable_by_keyword(keywordid, item['pid'], where = {'status': KeywordsDB.KEYWORDS_STATUS_ACTIVE})
        elif status == KeywordsDB.KEYWORDS_STATUS_DELETED:
            for item in project_list:
                self.taskdb.delete_by_keyword(keywordid, item['pid'])
        elif status == KeywordsDB.KEYWORDS_STATUS_DISABLE:
            for item in project_list:
                self.taskdb.disable_by_keyword(keywordid, item['pid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == KeywordsDB.KEYWORDS_STATUS_ACTIVE:
            for item in project_list:
                self.taskdb.active_by_keyword(keywordid, item['pid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})

    def schedule_project(self, projectid, status):
        if status == ProjectDB.PROJECT_STATUS_INIT:
            self.urlsdb.enable_by_project(projectid, where = {"status": UrlsDB.URLS_STATUS_ACTIVE})
            self.attachmentdb.enable_by_project(projectid, where = {"status": AttachmentDB.ATTACHMENT_STATUS_ACTIVE})
            self.sitedb.enable_by_project(projectid, where = {"status": SiteDB.SITE_STATUS_ACTIVE})
            self.taskdb.enable_by_project(projectid, where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == ProjectDB.PROJECT_STATUS_DELETED:
            self.urlsdb.delete_by_project(projectid)
            self.attachmentdb.delete_by_project(projectid)
            self.sitedb.delete_by_project(projectid)
            self.taskdb.delete_by_project(projectid)
        elif status == ProjectDB.PROJECT_STATUS_DISABLE:
            self.urlsdb.disable_by_project(projectid, where = {"status": [UrlsDB.URLS_STATUS_INIT, UrlsDB.URLS_STATUS_ACTIVE]})
            self.attachmentdb.disable_by_project(projectid, where = {"status": [AttachmentDB.ATTACHMENT_STATUS_INIT, AttachmentDB.ATTACHMENT_STATUS_ACTIVE]})
            self.sitedb.disable_by_project(projectid, where = {"status": [SiteDB.SITE_STATUS_INIT, SiteDB.SITE_STATUS_ACTIVE]})
            self.taskdb.disable_by_project(projectid, where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})

    def schedule_site(self, siteid, status):
        site = self.sitedb.get_detail(siteid)
        if status == SiteDB.SITE_STATUS_INIT:
            self.urlsdb.enable_by_site(siteid, where = {"status": UrlsDB.URLS_STATUS_ACTIVE})
            self.attachmentdb.enable_by_site(siteid, where = {"status": AttachmentDB.ATTACHMENT_STATUS_ACTIVE})
            self.taskdb.enable_by_site(siteid, site['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == SiteDB.SITE_STATUS_DELETED:
            self.urlsdb.delete_by_site(siteid)
            self.attachmentdb.delete_by_site(siteid)
            self.taskdb.delete_by_site(siteid, site['projectid'])
        elif status == SiteDB.SITE_STATUS_DISABLE:
            self.urlsdb.disable_by_site(siteid, where = {"status": [UrlsDB.URLS_STATUS_INIT, UrlsDB.URLS_STATUS_ACTIVE]})
            self.attachmentdb.disable_by_site(siteid, where = {"status": [AttachmentDB.ATTACHMENT_STATUS_INIT, AttachmentDB.ATTACHMENT_STATUS_ACTIVE]})
            self.taskdb.disable_by_site(siteid, site['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == SiteDB.SITE_STATUS_ACTIVE:
            self.taskdb.active_by_site(siteid, site['projectid'], where = {"status": TaskDB.TASK_STATUS_DISABLE})

    def schedule_urls(self, urlsid, status):
        urls = self.urlsdb.get_detail(urlsid)
        if status == UrlsDB.URLS_STATUS_INIT:
            self.taskdb.enable_by_urls(urlsid, urls['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == UrlsDB.URLS_STATUS_DELETED:
            self.taskdb.delete_by_urls(urlsid, urls['projectid'])
        elif status == UrlsDB.URLS_STATUS_DISABLE:
            self.taskdb.disable_by_urls(urlsid, urls['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == UrlsDB.URLS_STATUS_ACTIVE:
            self.taskdb.active_by_urls(urlsid, urls['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})

    def schedule_attachment(self, attachid, status):
        attachment = self.attachmentdb.get_detail(attachid)
        if status == AttachmentDB.ATTACHMENT_STATUS_INIT:
            self.taskdb.enable_by_attachment(attachid, attachment['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == AttachmentDB.ATTACHMENT_STATUS_DELETED:
            self.taskdb.delete_by_attachment(attachid, attachment['projectid'])
        elif status == AttachmentDB.ATTACHMENT_STATUS_DISABLE:
            self.taskdb.disable_by_attachment(attachid, attachment['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == AttachmentDB.ATTACHMENT_STATUS_ACTIVE:
            self.taskdb.active_by_attachment(attachid, attachment['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})
