#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-25 16:52:37
"""
import time
import logging
from . import BaseScheduler
from cdspider.libs.tools import *
from cdspider.exceptions import *

class NewtaskScheduler(BaseScheduler):
    """
    new task scheduler
    """
    def __init__(self, db, queue, log_level = logging.WARN):
        super(NewtaskScheduler, self).__init__(db, queue, log_level)
        self.inqueue = queue["newtask_queue"]

    def schedule(self, task):
        self.info("NewtaskScheduler schedule task: %s starting..." % str(task))
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
            self.queue['newtask4search'].put_nowait({'kwid':task['kwid']})
            return
        elif 'uid' in task and task['uid']:
            url=self.db['UrlsDB'].get_detail(task['uid'])
            site=self.db['SitesDB'].get_detail(url['sid'])
            if site['type']=='2':
                self.queue['newtask4search'].put_nowait({'uid':task['uid']})
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
            return self.debug("NewtaskScheduler NewTask failed")
        handler=load_handler(task, db=self.db,queue=self.queue)
        handler.newtask()
        self.logger.debug("NewtaskScheduler schedule success")
