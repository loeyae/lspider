# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:27:08
"""

from cdspider.worker import  BaseWorker
from cdspider.libs.constants import *
from cdspider.database.base import *
from cdspider.libs.utils import call_extension

class StatusWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_STATUS

    status_map = {
        str(SpiderTaskDB.STATUS_INIT): "enable",
        str(SpiderTaskDB.STATUS_ACTIVE): "active",
        str(SpiderTaskDB.STATUS_DISABLE): "disable",
        str(SpiderTaskDB.STATUS_DELETED): "delete",
    }


    def on_result(self, message):
        self.debug("got message: %s" % message)
        mode = message.pop('mode', None)
        status = message.pop("status")
        s = [item for item in message.items()]
        key, value = s[0]
        ns = self.status_map.get(str(status))
        if not ns:
            self.error("invalid status: %s" % status)
            return
        pn = "%s_by_%s" % (ns, key)
        try:
            getattr(self, pn)(value, mode)
        except AttributeError as e:
            self.error(e)

    def active_by_pid(self, pid, mode = None):
        pass

    def active_by_sid(self, sid, mode = None):
        pass

    def active_by_tid(self, tid, mode = None):
        pass

    def active_by_uid(self, uid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].active_by_uid(uid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].active_by_uid(data, ext.name)
            call_extension("handler", execut, uid, context=self.ctx, task=None)

    def active_by_kid(self, kid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].active_by_kid(kid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].active_by_kid(data, ext.name)
            call_extension("handler", execut, kid, context=self.ctx, task=None)

    def disable_by_pid(self, pid, mode = None):
        self.db['SitesDB'].disable_by_project(pid)
        self.db['TaskDB'].disable_by_project(pid)
        self.db['UrlsDB'].disable_by_project(pid)
        if mode:
            self.db['SpiderTaskDB'].disable_by_pid(pid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].disable_by_pid(data, ext.name)
            call_extension("handler", execut, pid, context=self.ctx, task=None)

    def disable_by_sid(self, sid, mode = None):
        self.db['TaskDB'].disable_by_site(sid)
        self.db['UrlsDB'].disable_by_site(sid)
        if mode:
            self.db['SpiderTaskDB'].disable_by_sid(sid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].disable_by_sid(data, ext.name)
            call_extension("handler", execut, sid, context=self.ctx, task=None)

    def disable_by_tid(self, tid, mode = None):
        self.db['UrlsDB'].disable_by_task(tid)
        if mode:
            self.db['SpiderTaskDB'].disable_by_tid(tid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].disable_by_tid(data, ext.name)
            call_extension("handler", execut, tid, context=self.ctx, task=None)

    def disable_by_uid(self, uid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].disable_by_uid(uid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].disable_by_uid(data, ext.name)
            call_extension("handler", execut, uid, context=self.ctx, task=None)

    def disable_by_kid(self, kid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].disable_by_kid(kid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].disable_by_kid(data, ext.name)
            call_extension("handler", execut, kid, context=self.ctx, task=None)

    def delete_by_pid(self, pid, mode = None):
        self.db['SitesDB'].delete_by_project(pid)
        self.db['TaskDB'].delete_by_project(pid)
        self.db['UrlsDB'].delete_by_project(pid)
        if mode:
            self.db['SpiderTaskDB'].delete_by_pid(pid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].delete_by_pid(data, ext.name)
            call_extension("handler", execut, pid, context=self.ctx, task=None)

    def delete_by_sid(self, sid, mode = None):
        self.db['TaskDB'].delete_by_site(sid)
        self.db['UrlsDB'].delete_by_site(sid)
        if mode:
            self.db['SpiderTaskDB'].delete_by_sid(sid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].delete_by_sid(data, ext.name)
            call_extension("handler", execut, sid, context=self.ctx, task=None)

    def delete_by_tid(self, tid, mode = None):
        self.db['UrlsDB'].delete_by_task(tid)
        if mode:
            self.db['SpiderTaskDB'].delete_by_tid(tid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].delete_by_tid(data, ext.name)
            call_extension("handler", execut, tid, context=self.ctx, task=None)

    def delete_by_uid(self, uid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].delete_by_uid(uid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].delete_by_uid(data, ext.name)
            call_extension("handler", execut, uid, context=self.ctx, task=None)

    def delete_by_kid(self, kid, mode = None):
        if mode:
            self.db['SpiderTaskDB'].delete_by_kid(kid, mode)
        else:
            def execut(ext, data):
                self.db['SpiderTaskDB'].delete_by_kid(data, ext.name)
            call_extension("handler", execut, kid, context=self.ctx, task=None)
            
    def enable_by_pid(self, pid, mode = None):
        pass

    def enable_by_sid(self, sid, mode = None):
        pass

    def enable_by_tid(self, sid, mode = None):
        pass

    def enable_by_uid(self, sid, mode = None):
        pass

    def enable_by_kid(self, sid, mode = None):
        pass