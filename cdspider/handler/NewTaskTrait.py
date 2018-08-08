#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 23:25:38
"""
import time
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.database.base import *


class NewTaskTrait(object):

    def _new_task(self, pid, sid, url, rate, uid=0, aid=0, kwid=0, status=0, save=None, expire = 0,rid=0):
        task = {
            'rid': rid,
            'pid': pid,                            # project id
            'sid': sid,                            # site id
            'kwid': kwid,                          # keyword id, if exists, default: 0
            'uid': uid,                            # url id, if exists, default: 0
            'aid': aid,                            # url id, if exists, default: 0
            'url': url,                            # base url
            'rate': rate,                          # 频率
            'status': status,                      # status, default: 0
            'expire': expire,                      # 有效期
            'save': save,                          # 保留的参数
            'queuetime': 0,                        # 入队时间
            'crawltime': 0,                        # 最近一次抓取时间
            'crawlinfo': None,                     # 最近十次抓取信息
            'plantime': 0,                         # 下一次入队时间
        }
        return self.db['TaskDB'].insert(task)

    def build_newtask_by_attachment(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        attachment = self.task.get("attachment")
        if not attachment:
            raise CDSpiderHandlerError('No Attachment')
        self.logger.debug("%s build_newtask_by_attachment attachment: %s" % (self.__class__.__name__, attachment))
        status = 1 if attachment['status'] == AttachmentDB.STATUS_ACTIVE else 0
        count = self.db['TaskDB'].get_count(project['pid'], {"aid": attachment['aid']}) or 0
        self._new_task(project['pid'], 0, self.task['url'], attachment['rate'], count + 1, attachment['aid'], 0, status, self.task['save'], int(time.time()) + int(attachment['expire']) * self.EXPIRE_STEP,self.task['rid'])

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
        count = self.task.get_count(project['pid'], {"uid": url['uid'], "kwid": keyword['kwid']})
        if count:
            return
        self.logger.debug("%s build_newtask_by_urls urls: %s" % (self.__class__.__name__, urls))
        srate = site.get('rate', 0)
        urate = urls.get('rate', 0)
        krate = keyword.get('rate', 0)
        rate = urate if urate > srate else srate
        rate =  krate if krate > rate else rate
        status = 1 if project['status'] == ProjectsDB.STATUS_ACTIVE and site['status'] == SitesDB.STATUS_ACTIVE and urls['status'] == UrlsDB.STATUS_ACTIVE and keyword['status'] == KeywordsDB.STATUS_ACTIVE else 0
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
        self.logger.debug("%s build_newtask_by_urls urls: %s" % (self.__class__.__name__, urls))
        prate = project.get('rate', 0)
        srate = site.get('rate', 0)
        urate = urls.get('rate', 0)
        rate = urate if urate > srate else (srate if srate > prate else prate)
        status = 1 if project['status'] == ProjectsDB.STATUS_ACTIVE and site['status'] == SitesDB.STATUS_ACTIVE and urls['status'] == UrlsDB.STATUS_ACTIVE else 0
        self._new_task(project['pid'], site['sid'], urls['url'], rate, urls['uid'], 0, 0, status)
