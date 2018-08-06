#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-23 14:58:57
"""
from cdspider.database.base import *

class StatusSchedule(object):
    """
    put you comment
    """
    def __init__(self, status_queue, ProjectsDB, TaskDB, SitesDB, UrlsDB, AttachmentDB, KeywordsDB, customdb):
        self.status_queue = status_queue
        self.ProjectsDB = ProjectsDB
        self.TaskDB = TaskDB
        self.SitesDB = SitesDB
        self.UrlsDB = UrlsDB
        self.AttachmentDB = AttachmentDB
        self.KeywordsDB = KeywordsDB
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
        project_list = self.ProjectsDB.get_list(where={'type': ProjectDB.PROJECT_TYPE_SEARCH})
        if status == KeywordsDB.KEYWORDS_STATUS_INIT:
            for item in project_list:
                self.TaskDB.enable_by_keyword(keywordid, item['pid'], where = {'status': KeywordsDB.KEYWORDS_STATUS_ACTIVE})
        elif status == KeywordsDB.KEYWORDS_STATUS_DELETED:
            for item in project_list:
                self.TaskDB.delete_by_keyword(keywordid, item['pid'])
        elif status == KeywordsDB.KEYWORDS_STATUS_DISABLE:
            for item in project_list:
                self.TaskDB.disable_by_keyword(keywordid, item['pid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == KeywordsDB.KEYWORDS_STATUS_ACTIVE:
            for item in project_list:
                self.TaskDB.active_by_keyword(keywordid, item['pid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})

    def schedule_project(self, projectid, status):
        if status == ProjectDB.PROJECT_STATUS_INIT:
            self.UrlsDB.enable_by_project(projectid, where = {"status": UrlsDB.URLS_STATUS_ACTIVE})
            self.AttachmentDB.enable_by_project(projectid, where = {"status": AttachmentDB.ATTACHMENT_STATUS_ACTIVE})
            self.SitesDB.enable_by_project(projectid, where = {"status": SiteDB.SITE_STATUS_ACTIVE})
            self.TaskDB.enable_by_project(projectid, where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == ProjectDB.PROJECT_STATUS_DELETED:
            self.UrlsDB.delete_by_project(projectid)
            self.AttachmentDB.delete_by_project(projectid)
            self.SitesDB.delete_by_project(projectid)
            self.TaskDB.delete_by_project(projectid)
        elif status == ProjectDB.PROJECT_STATUS_DISABLE:
            self.UrlsDB.disable_by_project(projectid, where = {"status": [UrlsDB.URLS_STATUS_INIT, UrlsDB.URLS_STATUS_ACTIVE]})
            self.AttachmentDB.disable_by_project(projectid, where = {"status": [AttachmentDB.ATTACHMENT_STATUS_INIT, AttachmentDB.ATTACHMENT_STATUS_ACTIVE]})
            self.SitesDB.disable_by_project(projectid, where = {"status": [SiteDB.SITE_STATUS_INIT, SiteDB.SITE_STATUS_ACTIVE]})
            self.TaskDB.disable_by_project(projectid, where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})

    def schedule_site(self, siteid, status):
        site = self.SitesDB.get_detail(siteid)
        if status == SiteDB.SITE_STATUS_INIT:
            self.UrlsDB.enable_by_site(siteid, where = {"status": UrlsDB.URLS_STATUS_ACTIVE})
            self.AttachmentDB.enable_by_site(siteid, where = {"status": AttachmentDB.ATTACHMENT_STATUS_ACTIVE})
            self.TaskDB.enable_by_site(siteid, site['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == SiteDB.SITE_STATUS_DELETED:
            self.UrlsDB.delete_by_site(siteid)
            self.AttachmentDB.delete_by_site(siteid)
            self.TaskDB.delete_by_site(siteid, site['projectid'])
        elif status == SiteDB.SITE_STATUS_DISABLE:
            self.UrlsDB.disable_by_site(siteid, where = {"status": [UrlsDB.URLS_STATUS_INIT, UrlsDB.URLS_STATUS_ACTIVE]})
            self.AttachmentDB.disable_by_site(siteid, where = {"status": [AttachmentDB.ATTACHMENT_STATUS_INIT, AttachmentDB.ATTACHMENT_STATUS_ACTIVE]})
            self.TaskDB.disable_by_site(siteid, site['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == SiteDB.SITE_STATUS_ACTIVE:
            self.TaskDB.active_by_site(siteid, site['projectid'], where = {"status": TaskDB.TASK_STATUS_DISABLE})

    def schedule_urls(self, urlsid, status):
        urls = self.UrlsDB.get_detail(urlsid)
        if status == UrlsDB.URLS_STATUS_INIT:
            self.TaskDB.enable_by_urls(urlsid, urls['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == UrlsDB.URLS_STATUS_DELETED:
            self.TaskDB.delete_by_urls(urlsid, urls['projectid'])
        elif status == UrlsDB.URLS_STATUS_DISABLE:
            self.TaskDB.disable_by_urls(urlsid, urls['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == UrlsDB.URLS_STATUS_ACTIVE:
            self.TaskDB.active_by_urls(urlsid, urls['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})

    def schedule_attachment(self, attachid, status):
        attachment = self.AttachmentDB.get_detail(attachid)
        if status == AttachmentDB.ATTACHMENT_STATUS_INIT:
            self.TaskDB.enable_by_attachment(attachid, attachment['projectid'], where = {"status": TaskDB.TASK_STATUS_ACTIVE})
        elif status == AttachmentDB.ATTACHMENT_STATUS_DELETED:
            self.TaskDB.delete_by_attachment(attachid, attachment['projectid'])
        elif status == AttachmentDB.ATTACHMENT_STATUS_DISABLE:
            self.TaskDB.disable_by_attachment(attachid, attachment['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_ACTIVE]})
        elif status == AttachmentDB.ATTACHMENT_STATUS_ACTIVE:
            self.TaskDB.active_by_attachment(attachid, attachment['projectid'], where = {"status": [TaskDB.TASK_STATUS_INIT, TaskDB.TASK_STATUS_DISABLE]})
