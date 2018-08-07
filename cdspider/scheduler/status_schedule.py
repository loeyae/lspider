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
    def __init__(self, db,queue,rate):
        self.db=db
        self.queue=queue
        self.rate=rate

    def schedule(self, data,db_name,id_type,pid):
        obj={}
        if 'rate' in data:
            rate=data['rate']
            try:
                obj['rate']=int(rate)
            except:
                pass
             
        if 'status' in data:
            status=data['status']
            try:
                obj['status']=int(status)
            except:
                pass
        if 'kwid'==id_type:
            self.schedule_keyword(data,obj,db_name,id_type,pid)
        elif 'pid'==id_type:
            self.schedule_project(data,obj,db_name,id_type,pid)
        elif 'sid'==id_type:
            self.schedule_site(data,obj,db_name,id_type,pid)
        elif 'uid'==id_type:
            self.schedule_urls(data,obj,db_name,id_type,pid)
        elif 'aid'==id_type:
            self.schedule_attachment(data,obj,db_name,id_type,pid)

    def schedule_keyword(self,data, obj,db_name,id_type,pid):
        if 'status' in obj:
            self.db['KeywordsDB'].update(data[id_type],obj)
            self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}
            self.db['TaskDB'].update_many(pid,obj=obj,where=where)
            self.db['KeywordsDB'].update(data[id_type],{'rate':obj['rate']})

    def schedule_project(self, data,obj,db_name,id_type,pid):
        if 'status' in obj:
            if obj['status']==Base.STATUS_DELETED or obj['status']==Base.STATUS_INIT:
                self.db['ProjectsDB'].update(data[id_type],obj)
                self.db['KeywordsDB'].update_many(obj,where={id_type:data[id_type]})
                self.db['SitesDB'].update_many(obj,where={id_type:data[id_type]})
                self.db['AttachmentDB'].update_many(obj,where={id_type:data[id_type]})
                self.db['UrlsDB'].update_many(obj,where={id_type:data[id_type]})
                self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data[id_type]})
            elif obj['status']==Base.STATUS_ACTIVE:
                self.db['ProjectsDB'].update(data[id_type],obj)
            

    def schedule_site(self, data,obj,db_name,id_type,pid):
        if 'status' in obj:
            self.db['SitesDB'].update(data[id_type],obj)
            if obj['status']==Base.STATUS_DELETED or obj['status']==Base.STATUS_INIT:
                self.db['UrlsDB'].update_many(obj,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
                self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            self.db['SitesDB'].update(data[id_type],{'rate':obj['rate']})
            u_rate={}
            for item in self.db['UrlsDB'].get_list(where={'sid':data[id_type]}):
                u_rate[item['uid']]=item['rate']
            for k,v in u_rate.items():
                if v>obj['rate']:
                    obj['rate']=v
                where={id_type:data[id_type],'uid':int(k),'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}
                self.db['TaskDB'].update_many(pid,obj={'rate':obj['rate']},where=where)

    def schedule_urls(self,data,obj,db_name,id_type,pid):
        if 'status' in obj:
                self.db['UrlsDB'].update(data[id_type],obj)
                self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}
            self.db['UrlsDB'].update(data[id_type],{'rate':obj['rate']})
            sid=self.db['UrlsDB'].get_detail(data[id_type])['sid']
            s_rate=self.db['SitesDB'].get_detail(sid)['rate']
            if s_rate>obj['rate']:
                obj['rate']=s_rate
            self.db['TaskDB'].update_many(pid,obj={'rate':obj['rate']},where=where)

    def schedule_attachment(self, obj,db_name,id_type,pid):
        if 'status' in obj:
            self.db['AttachmentDB'].update(data[id_type],obj)
            self.db['TaskDB'].update_many(pid,obj=obj,where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}})
        else:
            where={id_type:data[id_type],'status':{'$in':[Base.STATUS_ACTIVE,Base.STATUS_INIT]}}
            self.db['TaskDB'].update_many(pid,obj=obj,where=where)
            self.db['AttachmentDB'].update(data[id_type],{'rate':obj['rate']})