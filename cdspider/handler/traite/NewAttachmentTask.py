#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-27 16:56:09
"""
import time
import traceback
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import CustomParser

class NewAttachmentTask(object):
    """
    生成附加任务
    """

    def result2attach(self, save, domain, subdomain=None, data = None, url = None):
        """
        根据详情页生成附加任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        self.debug("%s new attach task starting" % (self.__class__.__name__))
        if self.page != 1:
            '''
            只在第一页时执行
            '''
            return
        self.debug("%s new comment task starting" % (self.__class__.__name__))
        self.result2comment(save, domain, subdomain, data, url)
        self.debug("%s new comment task end" % (self.__class__.__name__))
        self.debug("%s new interact task starting" % (self.__class__.__name__))
        self.result2interact(save, domain, subdomain, data, url)
        self.debug("%s new interact task end" % (self.__class__.__name__))
        self.debug("%s new attach task end" % (self.__class__.__name__))

    def result2comment(self, save, domain, subdomain = None, data = None, url = None):
        """
        根据详情页生成评论任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def build_task(rule, data = None, final_url = None):
            try:
                if final_url is None:
                    final_url = self.response['final_url']
                if data is None:
                    params, data = utils.get_attach_data(CustomParser, self.response['last_source'], final_url, rule, self.log_level)
                if data == False:
                    return None
                url, params = utils.build_attach_url(data, rule, self.response['final_url'])
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_comment_task(url, params, rule)
                    if cid:
                        self.task['crawlinfo']['commentRule'] = rule['uuid']
                        self.task['crawlinfo']['commentTaskId'] = cid
                        self.debug("%s new comment task: %s" % (self.__class__.__name__, str(cid)))
                    return True
                return False
            except:
                self.error(traceback.format_exc())
                return False
        #通过子域名获取评论任务
        ruleset = self.db['CommentRuleDB'].get_list_by_subdomain(subdomain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s comment task rule: %s" % (self.__class__.__name__, str(rule)))
            if build_task(rule, data, url):
                return
        #通过域名获取评论任务
        ruleset = self.db['CommentRuleDB'].get_list_by_domain(domain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s comment task rule: %s" % (self.__class__.__name__, str(rule)))
            if build_task(rule, data, url):
                return

    def result2interact(self, save, domain, subdomain = None, data = None, url = None):
        """
        根据详情页生成互动数任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def buid_task(rule, data = None, final_url = None):
            try:
                if final_url is None:
                    final_url = self.response['final_url']
                if data is None:
                    data = utils.get_attach_data(CustomParser, self.response['last_source'], final_url, rule, self.log_level)
                if data == False:
                    return None
                url, params = utils.build_attach_url(data, rule, self.response['final_url'])
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_interact_task(url, params, rule)
                    if cid:
                        self.task['crawlinfo']['interactRule'] = rule['uuid']
                        self.task['crawlinfo']['interactTaskId'] = cid
                        if 'interactRuleList' in  self.task['crawlinfo']:
                             self.task['crawlinfo']['interactRuleList'][str(rule['uuid'])] = cid
                        else:
                            self.task['crawlinfo']['interactRuleList'] = {str(rule['uuid']): cid}
                        self.debug("%s new interact task: %s" % (self.__class__.__name__, str(cid)))
            except:
                self.error(traceback.format_exc())
        #通过子域名获取互动数任务
        ruleset = self.db['AttachmentDB'].get_list_by_subdomain(subdomain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s interact task rule: %s" % (self.__class__.__name__, str(rule)))
            buid_task(rule, data, url)
        #通过域名获取互动数任务
        ruleset = self.db['AttachmentDB'].get_list_by_domain(domain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s interact task rule: %s" % (self.__class__.__name__, str(rule)))
            buid_task(rule, data, url)

    def build_comment_task(self, url, data, rule):
        """
        构造评论任务
        :param url taks url
        :param rule 评论任务规则
        """
        task = {
            'mediaType': MEDIA_TYPE_WEIBO,
            'mode': HANDLER_MODE_COMMENT,                           # handler mode
            'pid': self.task['crawlinfo'].get('pid', 0),            # project id
            'sid': self.task['crawlinfo'].get('sid', 0),            # site id
            'tid': self.task['crawlinfo'].get('tid', 0),            # task id
            'uid': self.task['crawlinfo'].get('uid', 0),            # url id
            'kid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': self.task['rid'],                           # article id
            'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.debug("%s build comment task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "kid": task['kid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'

    def build_interact_task(self, url, data, rule):
        """
        构造互动数任务
        :param url taks url
        :param rule 互动数任务规则
        """
        task = {
            'mediaType': MEDIA_TYPE_WEIBO,
            'mode': HANDLER_MODE_INTERACT,                          # handler mode
            'pid': self.task['crawlinfo'].get('pid', 0),            # project id
            'sid': self.task['crawlinfo'].get('sid', 0),            # site id
            'tid': self.task['crawlinfo'].get('tid', 0),            # task id
            'uid': self.task['crawlinfo'].get('uid', 0),            # url id
            'kid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': self.task['rid'],                           # article id
            'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.debug("%s build interact task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "kid": task['kid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'
