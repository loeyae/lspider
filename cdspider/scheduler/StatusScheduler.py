#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-25 16:50:34
"""
import time
import logging
from . import BaseScheduler
from cdspider.libs.tools import *
from cdspider.exceptions import *
from cdspider.database.base import *

class StatusScheduler(BaseScheduler):
    """
    status change scheduler
    """
    def __init__(self, context):
        super(StatusScheduler, self).__init__(context)
        self.inqueue = self.queue["status_queue"]

    def schedule(self, message):
        obj={"utime": int(time.time())}
        if 'rate' in message:
            rate=message['rate']
            try:
                obj['rate']=int(rate)
            except:
                pass

        if 'status' in message:
            status=message['status']
            try:
                obj['status']=int(status)
            except:
                pass
        if 'kwid' in message and message['kwid']:
            self.schedule_keyword(int(message['kwid']), obj)
        elif 'pid' in message and message['pid']:
            self.schedule_project(int(message['pid']), obj)
        elif 'sid' in message and message['sid']:
            self.schedule_site(int(message['sid']), obj)
        elif 'uid' in message and message['uid']:
            self.schedule_urls(int(message['uid']), obj)
        elif 'aid' in message and message['aid']:
            self.schedule_attachment(int(message['aid']), obj)
        elif 'crid' in message and message['crid']:
            self.schedule_channel(int(message['crid']), obj)

    def schedule_keyword(self, kwid, obj):
        keyword = self.db['KeywordsDB'].get_detail(kwid)
        if not keyword:
            raise CDSpiderError('Keyword: %s no exists' % kwid)
        pid = keyword['pid']
        if pid:
            if 'status' in obj:
                self.db['KeywordsDB'].update(kwid, obj)
                self.db['TaskDB'].update_many(pid, obj=obj, where={"kwid": kwid, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}})
            else:
                where={"kwid": kwid, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}}
                self.db['TaskDB'].update_many(pid, obj=obj, where=where)
                self.db['KeywordsDB'].update(kwid, obj)

    def schedule_project(self, pid, obj):
        project = self.db['ProjectsDB'].get_detail(pid)
        if not project:
            raise CDSpiderError('Project: %s no exists' % pid)
        if 'status' in obj:
            if obj['status']==Base.STATUS_DELETED or obj['status']==Base.STATUS_INIT:
                self.db['ProjectsDB'].update(pid,obj)
                self.db['KeywordsDB'].update_many(obj, where={"pid": pid})
                self.db['SitesDB'].update_many(obj, where={"pid": pid})
                self.db['AttachmentDB'].update_many(obj, where={"pid": pid})
                self.db['UrlsDB'].update_many(obj, where={"pid": pid})
                self.db['ChannelRulesDB'].update_many(obj, where={"pid": pid})
                self.db['TaskDB'].update_many(pid,obj=obj, where={"pid": pid})
            elif obj['status']==Base.STATUS_ACTIVE:
                self.db['ProjectsDB'].update(pid, obj)

    def schedule_site(self, sid, obj):
        site = self.db['SitesDB'].get_detail(sid)
        if not site:
            raise CDSpiderError('Site: %s no exists' % sid)
        pid = site['pid']
        if 'status' in obj:
            self.db['SitesDB'].update(sid, obj)
            if obj['status'] == Base.STATUS_DELETED or obj['status'] == Base.STATUS_INIT:
                self.db['UrlsDB'].update_many(obj,where={"sid": sid, 'status':{'$in': [Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
                self.db['ChannelRulesDB'].update_many(obj,where={"sid": sid, 'status': {'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
                self.db['TaskDB'].update_many(pid,obj=obj,where={"sid": sid, 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            self.db['SitesDB'].update(sid, obj)
            u_rate={}
            for item in self.db['UrlsDB'].get_list(where={'sid': sid}):
                u_rate[item['uid']]=item['rate']
            for k,v in u_rate.items():
                if v > obj['rate']:
                    obj['rate']=v
                where={"sid": sid,'uid': int(k), 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}
                self.db['TaskDB'].update_many(pid, obj=obj, where=where)

    def schedule_urls(self,uid, obj):
        urls = self.db['UrlsDB'].get_detail(uid)
        if not urls:
            raise CDSpiderError('Url: %s no exists' % uid)
        pid = urls['pid']
        if 'status' in obj:
                self.db['UrlsDB'].update(uid, obj)
                self.db['TaskDB'].update_many(pid, obj=obj, where={"uid": uid, 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            where={"uid": uid, 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}}
            self.db['UrlsDB'].update(uid, obj)
            s_rate=self.db['SitesDB'].get_detail(urls['sid'])['rate']
            if s_rate > obj['rate']:
                obj['rate'] = s_rate
            self.db['TaskDB'].update_many(pid, obj=obj, where=where)

    def schedule_channel(self, crid, obj):
        channel = self.db['ChannelRulesDB'].get_detail(crid)
        if not channel:
            raise CDSpiderError('Channel: %s no exists' % crid)
        pid = channel['pid']
        if 'status' in obj:
                self.db['ChannelRulesDB'].update(crid, obj)
                self.db['TaskDB'].update_many(pid, obj=obj, where={"crid": crid, 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}})
        else:
            where={"crid": crid, 'aid': 0, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}}
            self.db['ChannelRulesDB'].update(crid, obj)
            self.db['TaskDB'].update_many(pid, obj= obj, where=where)

    def schedule_attachment(self, aid, obj):
        attachment = self.db['AttachmentDB'].get_detail(aid)
        if not attachment:
            raise CDSpiderError('Attachment: %s no exists' % aid)
        pid = attachment['pid']
        if 'status' in obj:
            self.db['AttachmentDB'].update(aid, obj)
            if obj['status'] == Base.STATUS_DELETED or obj['status'] == Base.STATUS_INIT:
                self.db['TaskDB'].update_many(pid, obj=obj, where={"aid": aid,'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}})
            else:
                p_status=self.db['ProjectsDB'].get_detail(pid)['status']
                if p_status==Base.STATUS_ACTIVE:
                    self.db['TaskDB'].update_many(pid, obj=obj, where={"aid": aid,'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}})
        else:
            where={"aid": aid, 'status':{'$in':[Base.STATUS_ACTIVE, Base.STATUS_INIT]}}
            self.db['TaskDB'].update_many(pid, obj=obj, where=where)
            self.db['AttachmentDB'].update(aid, obj)
