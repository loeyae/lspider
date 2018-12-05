#-*- coding: utf-8 -*-

'''
Created on 2018年6月20日

@author: Wang Fengwei
'''
import time
from cdspider.tools import Base
from cdspider.libs.constants import *

class active_task_by_pid(Base):

    def process(self, *args):
        testing_mode = self.g.get('testing_mode', False)
        pid = int(self.get_arg(args, 0, 'Pleas input pid'))
        uid = 0
        if len(args) > 1:
            uid = int(args[1])
        mode = HANDLER_MODE_DEFAULT_LIST
        if len(args) > 2:
            mode = args[2]
        self.broken('Project not exists', pid)
        project = self.g['db']['ProjectsDB'].get_detail(pid)
        self.broken('Project: %s not exists' % pid, project)
        self.notice('Selected Project Info:', project)
        UrlsDB = self.g['db']['UrlsDB']
        while True:
            has_item = False
            for item in UrlsDB.get_new_list_by_pid(uid, pid, where={'status': UrlsDB.STATUS_ACTIVE, 'ruleStatus': UrlsDB.STATUS_ACTIVE}):
                self.info("active task by uid: %s" %  str(item['uuid']))
                if not testing_mode:
                    self.g['db']['SpiderTaskDB'].active_by_url(item['uuid'], mode)
                has_item = True
                if item['uuid'] > uid:
                    uid = item['uuid']
            if not has_item:
                return
            time.sleep(0.1)
