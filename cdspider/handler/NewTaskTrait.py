#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 23:25:38
"""
import time
import copy
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.database.base import *


class NewTaskTrait(object):

    def _new_task(self, *args, **kwargs):
        kwargs['pid'] = args[0] if len(args) > 0 else kwargs['pid']
        kwargs['sid'] = args[1] if len(args) > 1 else kwargs['sid']
        kwargs['url'] = args[2] if len(args) > 2 else kwargs['url']
        kwargs['rate'] = args[3] if len(args) > 3 else kwargs['rate']
        kwargs['uid'] = args[4] if len(args) > 4 else kwargs.get('uid', 0)
        kwargs['aid'] = args[5] if len(args) > 5 else kwargs.get('kwid', 0)
        kwargs['kwid'] = args[6] if len(args) > 6 else kwargs.get('uid', 0)
        kwargs['status'] = args[7] if len(args) > 7 else kwargs.get('status', 0)
        kwargs['save'] = args[8] if len(args) > 8 else kwargs.get('save', None)
        kwargs['expire'] = args[9] if len(args) > 9 else kwargs.get('expire', 0)
        kwargs['rid'] = args[10] if len(args) > 10 else kwargs.get('rid', 0)
        kwargs['crid'] = args[11] if len(args) > 11 else kwargs.get('crid', 0)
        task = copy.deepcopy(kwargs)
        task.update({
            'queuetime': 0,                        # 入队时间
            'crawltime': 0,                        # 最近一次抓取时间
            'crawlinfo': None,                     # 最近十次抓取信息
            'plantime': 0,                         # 下一次入队时间
        })
        return self.db['TaskDB'].insert(task)

    def build_newtask_by_attachment(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        attachment = self.task.get("attachment")
        if not attachment:
            raise CDSpiderHandlerError('No Attachment')
        self.debug("%s build_newtask_by_attachment attachment: %s" % (self.__class__.__name__, attachment))
        status = 1 if attachment['status'] == AttachmentDB.STATUS_ACTIVE else 0
        count = self.db['TaskDB'].get_count(project['pid'], {"aid": attachment['aid']}) or 0
        self._new_task(project['pid'], 0, self.task['url'], int(attachment['rate'] or 5), count + 1, attachment['aid'], 0, status, self.task['save'], int(time.time()) + int(attachment['expire'] or 2592000) * self.EXPIRE_STEP, self.task['rid'])

    def build_newtask_by_keywords(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        site = self.task.get("site")
        if not site:
            raise CDSpiderHandlerError('No site')
        urls = self.task.get("urls")
        if not urls:
            raise CDSpiderHandlerError('No urls')
        keyword = self.task.get("keyword")
        if not keyword:
            raise CDSpiderHandlerError('No keyword')
        count = self.db['TaskDB'].get_count(project['pid'], {"uid": urls['uid'], "kwid": keyword['kwid']})
        if count:
            return
        self.debug("%s build_newtask_by_urls urls: %s" % (self.__class__.__name__, urls))
        srate = int(site.get('rate', 0))
        urate = int(urls.get('rate', 0))
        krate = int(keyword.get('rate', 0))
        rate = urate if urate > srate else srate
        rate =  krate if krate > rate else rate
        status = 1 if project['status'] == ProjectsDB.STATUS_ACTIVE and site['status'] == SitesDB.STATUS_ACTIVE and urls['status'] == UrlsDB.STATUS_ACTIVE and keyword['status'] == KeywordsDB.STATUS_ACTIVE else 0
        expire = int(keyword.get('expire', 0)) or 0
        if expire > 0:
            expire = int(time.time()) + expire * self.EXPIRE_STEP
#        url = utils.build_url_by_rule({"mode": 'format', "base": urls['url']}, {"keyword": keyword['word']})
        self._new_task(project['pid'], site['sid'], urls['url'], rate, urls['uid'], 0, keyword['kwid'], status)

    def build_newtask_by_urls(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        site = self.task.get("site")
        if not site:
            raise CDSpiderHandlerError('No site')
        urls = self.task.get('urls')
        if not urls:
            raise CDSpiderHandlerError('No urls')
        self.debug("%s build_newtask_by_urls urls: %s" % (self.__class__.__name__, urls))
        srate = int(site.get('rate', 0))
        urate = int(urls.get('rate', 0))
        rate = urate if urate > srate else srate
        status = 1 if project['status'] == ProjectsDB.STATUS_ACTIVE and site['status'] == SitesDB.STATUS_ACTIVE and urls['status'] == UrlsDB.STATUS_ACTIVE else 0
        expire = int(urls.get('expire', 0)) or 0
        if expire > 0:
            expire = int(time.time()) + expire * self.EXPIRE_STEP
        self._new_task(project['pid'], site['sid'], urls['url'], rate, urls['uid'], status=status, expire=expire)

    def build_newtask_by_channel(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        site = self.task.get("site")
        if not site:
            raise CDSpiderHandlerError('No site')
        channel = self.task.get('channel')
        if not channel:
            raise CDSpiderHandlerError('No channel')
        self.debug("%s build_newtask_by_channel channel: %s" % (self.__class__.__name__, channel))
        rate = channel['rate']
        status = 1 if project['status'] == ProjectsDB.STATUS_ACTIVE and site['status'] == SitesDB.STATUS_ACTIVE and channel['status'] == ChannelRulesDB.STATUS_ACTIVE else 0
        expire = int(channel.get('expire', 0)) or 0
        if expire > 0:
            expire = int(time.time()) + expire * self.EXPIRE_STEP
        self._new_task(project['pid'], site['sid'], channel['url'], rate, 0, status=status, expire=expire, crid=channel['crid'])
