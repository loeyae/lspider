#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: __init__.py 2428 2018-07-31 01:28:52Z zhangyi $
import abc
import re
import six
import time
import logging
import traceback
import copy
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.libs.tools import *
from cdspider.libs.time_parser import Parser as TimeParser

IGNORE_EXCEPTIONS = (CDSpiderCrawlerNoNextPage, CDSpiderCrawlerMoreThanMaximum, CDSpiderCrawlerNoExists, CDSpiderCrawlerNoSource)
RETRY_EXCEPTIONS = (CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout)
NOT_EXISTS_EXCEPTIONS = (CDSpiderCrawlerNotFound, CDSpiderCrawlerNoSource, CDSpiderParserError)

@six.add_metaclass(abc.ABCMeta)
class BaseHandler(object):
    """
    handler基类
    """
    MODE_DEFAULT = 'list'
    MODE_LIST = MODE_DEFAULT
    MODE_ITEM = 'item'
    MODE_ATT = 'att'
    CRAWL_INFO_LIMIT_COUNT = 10
    CONTINUE_EXCEPTIONS = ()

    def __init__(self, *args, **kwargs):
        """
        init
        """
        self.logger = kwargs.pop('logger', logging.getLogger('handler'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self.task = kwargs.pop('task')
        self.spider = kwargs.pop('spider')
        self.outqueue = kwargs.pop("outqueue", None)
        self.requeue = kwargs.pop("requeue", None)
        self.status_queue = kwargs.pop("status_queue", None)
        self.excqueue = kwargs.pop("excqueue", None)
        self.uniquedb = kwargs.pop("uniquedb", None)
        self.projectdb = kwargs.pop("projectdb", None)
        self.taskdb = kwargs.pop("taskdb", None)
        self.sitedb = kwargs.pop("sitedb", None)
        self.sitetypedb = kwargs.pop('sitetypedb', None)
        self.urlsdb = kwargs.pop("urlsdb", None)
        self.attachmentdb = kwargs.pop("attachmentdb", None)
        self.keywordsdb = kwargs.pop("keywordsdb", None)
        self.resultdb = kwargs.pop("resultdb", None)
        self.customdb = kwargs.pop('customdb', None)
        self.crawl_info  = {"crawl_count": {"count": 0, "new_count": 0, "req_error": 0, "parsed_count": 0, "repet_count": 0},"broken": None, "crawl_start": int(time.time())}
        self.crawl_id = self.task.get('save', {}).get('crawl_id', int(time.time()))
        self._settings = kwargs or {}

    def get_crawler(self, task, mode):
        if 'process' in task and task['process']:
            return task.get('process', {}).get('crawler', 'requests')
        if mode == self.MODE_LIST:
            return task.get("site").get("main_process", {}).get("crawler", task.get("project").get("main_process", {}).get("crawler", "requests"))
        elif mode == self.MODE_ATT:
            return task.get('attachment').get("main_process", {}).get("crawler", "requests")
        return task.get("site").get("sub_process", {}).get("crawler", task.get("project").get("sub_process", {}).get("crawler", "requests"))

    def get_base_request(self, task, mode):
        """
        获取定义的基础请求
        """
        if 'base_request' in task and task['base_request']:
            return task['base_request']
        if mode == self.MODE_ATT:
            return task.get("attachment").get("base_request")
        return task.get("site").get("base_request", task.get("project").get("base_request", None))

    @property
    def continue_exceptions(self):
        """
        获取自定义
        """
        return self.CONTINUE_EXCEPTIONS

    def get_unique_setting(self, url, data):
        """
        获取生成唯一ID的字段
        """
        project = self.task.get('project')
        site = self.task.get('site')
        urls = self.task.get('urls', {})
        attachment = self.task.get('attachment')
        if attachment:
            identify = attachment.get('identify', None)
        else:
            identify = urls.get('identify', site.get('identify', project.get('identify', None)))
        u = url
        if identify:
            if 'url' in identify and identify['url']:
                rule, key = utils.rule2pattern(identify['url'])
                if rule and key:
                    ret = re.search(rule, url)
                    if ret:
                        u = ret.group(key)
                else:
                    ret = re.search(identify['url'], url)
                    if ret:
                        u = ret.group(0)
            if 'query' in identify and identify['query']:
                u = utils.build_filter_query(url, identify['query'])
            if 'data' in identify:
                udict = dict.fromkeys(identify['data'])
                query = utils.dictunion(data, udict)
                return utils.build_query(u, query)
        return u

    @abc.abstractmethod
    def build_newtask(self):
        """
        获取新任务
        """

    def get_process(self, task, mode, last_source):
        """
        获取爬虫抓取流程配置
        """
        if 'process' in task and task['process']:
            return task.get('process')
        if mode == self.MODE_ATT:
            return task.get("attachment").get("main_process")
        if mode == self.MODE_LIST:
            return task.get("site").get("main_process", task.get("project").get("main_process", None))
        sub_process = task.get("site").get("sub_process", task.get("project").get("sub_process", None))
        attachment_list = task.get("site").get("attachment_list")
        if attachment_list:
            for attachment in attachment_list:
                base_request = attachment.get("base_request")
                if "parse" in base_request and base_request['parse'] and 'data' in base_request['parse'] and base_request['parse']['data']:
                    sub_process['parse'].update({"data_attach_%s" % attachment['aid']: base_request['parse']['data']})
        return sub_process

    def on_repetition(self):
        raise CDSpiderCrawlerNoNextPage()

    def on_error(self, task, exc, mode):
        """
        错误处理
        """
        self.crawl_info['broken'] = str(exc)
        if 'queue' in task and task['queue'] and 'queue_message' in task and task['queue_message']:
            if isinstance(exc, RETRY_EXCEPTIONS) or not isinstance(exc, CDSpiderError):
                task['queue'].put_nowait(task['queue_message'])
                return
        if isinstance(exc, NOT_EXISTS_EXCEPTIONS) and 'rid' in task and self.resultdb:
            self.resultdb.update(task['rid'], {"status": self.resultdb.RESULT_STATUS_DELETED})
            return
        if not isinstance(exc, IGNORE_EXCEPTIONS) and self.excqueue:
            message = {
                'mode':  mode,
                'base_url': self.task.get("save", {}).get("base_url", None),
                'request_url': self.task.get("save", {}).get("request_url", None),
                'project': self.task.get("projectid", None),
                'site': self.task.get("siteid", None),
                'urls': self.task.get("urlid", None),
                'site': self.task.get("kwid", None),
                'crawltime': time.strftime("%Y-%m-%d %H:%M:%S"),
                'err_message': str(exc),
                'tracback': traceback.format_exc(),
                'last_source': self.task.get('last_source', None),
                'last_rule': self.task.get('last_rule', None),
            }
            self.excqueue.put_nowait(message)

    @abc.abstractmethod
    def on_result(self, task, data, broken_exc, page_source, mode):
        """
        数据处理
        """
        pass

    def on_continue(self, crawler, save):
        if 'incr_data' in save:
            for i in range(len(save['incr_data'])):
                save['incr_data'][i]['value'] -= save['incr_data'][i].get('step', 1)

    def finish(self, task, mode):
        if self.taskdb and self.task.get('tid', None):
            crawlinfo = self.task.get('crawlinfo', {}) or {}
            self.crawl_info['crawl_end'] = int(time.time())
            crawlinfo[str(self.crawl_id)] = self.crawl_info
            crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
            if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
                del crawlinfo_sorted[0]
            self.taskdb.update(self.task.get('tid'), self.task.get('projectid'), {"crawltime": int(time.time()), "crawlinfo": dict(crawlinfo_sorted), "save": self.task.get("save")})
            #TODO 自动调节抓取频率

class NewTaskTrait(object):

    def _new_task(self, pid, sid, url, rate, uid=0, atid=0, kwid=0, status=0, save=None):
        task = {
            'projectid': pid,                      # project id
            'siteid': sid,                         # site id
            'kwid': kwid,                          # keyword id, if exists, default: 0
            'urlid': uid,                          # url id, if exists, default: 0
            'atid': atid,                          # url id, if exists, default: 0
            'url': url,                            # base url
            'rate': rate,
            'status': status,                      # status, default: 0
            'save': save,                          # 保留的参数
            'queuetime': 0,                        # 入队时间
            'crawltime': 0,                        # 最近一次抓取时间
            'crawlinfo': None,                     # 最近十次抓取信息
            'plantime': 0,                         # 下一次入队时间
        }
        return self.taskdb.insert(task)

    def build_newtask_by_attachment(self):
        attachment = self.task.get("attachment")
        self.logger.debug("%s build_newtask_by_attachment attachment: %s" % (self.__class__.__name__, attachment))
        status = 1 if attachment['status'] == AttachmentDB.ATTACHMENT_STATUS_ACTIVE else 0
        count = self.taskdb.get_count(attachment['projectid'], {"atid": attachment['aid']}) or 0
        self.task['save']['parent_url'] = self.task['url']
        self._new_task(attachment['projectid'], attachment['siteid'], self.task['url'], attachment['rate'], count + 1, attachment['aid'], 0, status, self.task['save'])

    def build_newtask_by_keywords(self):
        project = self.task.get("project")
        if not project:
            raise CDSpiderHandlerError('No project')
        site = self.task.get("site")
        if not site:
            raise CDSpiderHandlerError('No site')
        keyword = self.task.get("keyword")
        if not keyword:
            raise CDSpiderHandlerError('No keyword')
        count = self.task.get_count(project['pid'], {"siteid": site['sid'], "kwid": keyword['kid']})
        if count:
            return
        self.logger.debug("%s build_newtask_by_urls urls: %s" % (self.__class__.__name__, urls))
        prate = project.get('rate', 0)
        srate = site.get('rate', 0)
        urate = urls.get('rate', 0)
        rate = urate if urate > srate else (srate if srate > prate else prate)
        status = 1 if project['status'] == ProjectDB.PROJECT_STATUS_ACTIVE and site['status'] == SiteDB.SITE_STATUS_ACTIVE and keyword['status'] == KeywordsDB.KEYWORDS_STATUS_ACTIVE else 0
        self._new_task(project['pid'], site['sid'], site['url'], rate, 0, 0, keyword['kid'], status)

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
        status = 1 if project['status'] == ProjectDB.PROJECT_STATUS_ACTIVE and site['status'] == SiteDB.SITE_STATUS_ACTIVE and urls['status'] == UrlsDB.URLS_STATUS_ACTIVE else 0
        self._new_task(project['pid'], site['sid'], urls['url'], rate, urls['uid'], 0, 0, status)

class ResultTrait(object):

    def _build_crawl_info(self, final_url, createtime):
        return {
            str(self.crawl_id): {
                "task": self.task.get("tid"),
                "project": self.task.get("projectid"),
                "site": self.task.get("siteid"),
                "urls": self.task.get("urlid", 0),
                "keywords": self.task.get("kwid", 0),
                "url": final_url,
                "crawltime": createtime,
            }
        }

    def _build_result_info(self, **kwargs):
        result = kwargs.get('result', {})
        nocreated = kwargs.get('nocreated', False)
        update = kwargs.get('update', False)
        created = result.pop('created', 0)
        if created:
            created = TimeParser.timeformat(str(created))
        if not created and not nocreated:
            created = int(time.time())
        r = {
                'crawl_id': self.crawl_id,                        # 抓取id, 与siteid一起标识同一站点的同一批次的结果
                'status': kwargs.get('status', ResultDB.RESULT_STATUS_INIT),            # 状态
                'url': kwargs['final_url'],
                'domain': kwargs.get("typeinfo", {}).get('domain', None),   # 站点域名
                'sitetype': kwargs.get("typeinfo", {}).get('type', None),   # 站点类型
                'title': result.pop('title', result.pop('title', None)),               # 标题
                'author': result.pop('author', result.pop('author', None)),             # 作者
                'created': created,                               # 发布时间
                'summary': result.pop('summary', None),           # 摘要
                'content': result.pop('content', None) if "content" in result else str(result),           # 详情
                'crawlinfo': kwargs['crawlinfo'],                 # 抓取信息 [{"project":projectid,"task":taskid,"urls":urlid,"keywords":keywordid,"crawltime":crawltime},..]
                'source': kwargs.get('source', None),             # 抓到的源码
            }
        if not update:
            r.update({
                'unid': kwargs['unid'],                           # unique str
                "projectid": self.task.get("projectid"),
                "siteid": self.task.get("siteid"),
                "urlid": self.task.get("urlid", 0),
                "atid": self.task.get("atid", 0),
                "kwid": self.task.get("kwid", 0),
                'createtime': kwargs.get('createtime', int(time.time())),
            })
        r['result'] = result or None
        return r

    def list_to_item(self, data, task = None, unique = True):
        self.spider.set_handler(self)
        base_url = self.task.get('url')
        createtime = int(time.time())
        parentid = task.get('save', {}).get('parentid', '0')
        data = utils.filter_list_result(data)
        if not data:
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            for item in data:
                if not 'url' in item or not item['url']:
                    raise CDSpiderError("url no exists: %s @ %s" % (str(item), str(task)))
                item['url'] = urljoin(base_url, item['url'])
                inserted = True
                if unique:
                    inserted, unid = self.uniquedb.insert(self.get_unique_setting(item['url'], item), self.task.get("projectid"), self.task.get("siteid"), self.task.get("urlid"), self.task.get("atid"), self.task.get("kwid"), createtime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    newtask = {
                        "mode": self.MODE_ITEM,
                        "project": copy.deepcopy(task.get("project")),
                        "site": copy.deepcopy(task.get("site")),
                        "projectid": self.task.get("projectid"),
                        "siteid": self.task.get("siteid"),
                        "urlid": self.task.get("urlid", 0),
                        "kwid": self.task.get("kwid", 0),
                        "url": item['url'],
                        "unid": unid,
                        "item": copy.deepcopy(item),
                        "save": {"parentid": parentid}
                    }
                    if self.spider:
                        self.spider.fetch(newtask)
                    else:
                        self.logger.error("%s Not Spider", self.__class__.__name__)
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                    self.crawl_info['crawl_count']['repet_count'] += 1
                    self.on_repetition()

    def list_to_work(self, data, task = None, unique = True):
        base_url = self.task.get('url')
        createtime = int(time.time())
        parentid = task.get('save', {}).get('parentid', '0')
        data = utils.filter_list_result(data)
        if not data:
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            for item in data:
                if not 'url' in item or not item['url']:
                    raise CDSpiderError("url no exists: %s @ %s" % (str(item), str(task)))
                if item['url'].startswith('javascript'):
                    continue
                item['url'] = urljoin(base_url, item['url'])
                inserted = True
                if unique:
                    inserted, unid = self.uniquedb.insert(self.get_unique_setting(item['url'], item), self.task.get("projectid"), self.task.get("siteid"), self.task.get("urlid"), self.task.get("atid"), self.task.get("kwid"), createtime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                crawlinfo = self._build_crawl_info(base_url, createtime)
                if inserted:
                    self.crawl_info['crawl_count']['new_count'] += 1
                    result = self._build_result_info(final_url=item['url'], result=item, crawlinfo=crawlinfo, nocreated=True, **unid)
                    result['parentid'] = parentid
                    result_id = self.resultdb.insert(result)
                    if result_id:
                        self.outqueue.put_nowait({"id": result_id, 'task': 1})
                    else:
                        raise CDSpiderDBError("Result insert failed")
                elif unid:
                    self.resultdb.add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repet_count'] += 1
                self.on_repetition()

    def list_to_result(self, final_url, data, typeinfo, task = None, page_source = None, unid = None):
        if unid:
            createtime = unid['createtime']
        else:
            createtime = int(time.time())

        parentid = task.get('save', {}).get('parentid', '0')
        data = utils.filter_list_result(data)
        if not data:
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            for item in data:
                inserted = False
                if not unid:
                    inserted, unid = self.uniquedb.insert(self.get_unique_setting(final_url, data), self.task.get("projectid"), self.task.get("siteid"), self.task.get("urlid"), self.task.get("atid"), self.task.get("kwid"), createtime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                crawlinfo = self._build_crawl_info(final_url, createtime)
                if inserted:
                    self.crawl_info['crawl_count']['new_count'] += 1
                    if 'url' in item and iten['url']:
                        item['url'] = urljoin(final_url, item['url'])
                    result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=item, crawlinfo=crawlinfo, **unid)
                    result['parentid'] = parentid
                    result_id = self.resultdb.insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                elif unid:
                    self.resultdb.add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repet_count'] += 1
                self.on_repetition()

    def item_to_worker(self, final_url, data, typeinfo, task, page_source = None, unid = None):
        if unid:
            createtime = unid['createtime']
        else:
            createtime = int(time.time())
        data = utils.filter_item_result(data)
        if not data:
            raise CDSpiderParserNoContent()
        inserted = True
        isfirst = True
        # 判断是否为详情页第一页
        incr_data = task.get('save', {}).get('incr_data', None)
        parentid = task.get('save', {}).get('parentid', '0')
        rid = task.get('rid', None)
        update = True if rid else False
        if incr_data:
            for item in incr_data:
                if not item['first']:
                    isfirst = False

        if not unid:
            inserted, unid = self.uniquedb.insert(self.get_unique_setting(final_url, data), self.task.get("projectid"), self.task.get("siteid"), self.task.get("urlid"), self.task.get("atid"), self.task.get("kwid"), createtime)
            if not isfirst:
                inserted = True
            self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                item = task.get('item', {})
                data = utils.dictjoin(data, item)
                self.crawl_info['crawl_count']['new_count'] += 1
                crawlinfo = self._build_crawl_info(final_url, createtime)
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ResultDB.RESULT_STATUS_PARSED, update=update, **unid)
                if rid:
                    self.resultdb.update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.resultdb.insert(result)
            else:
                result = self.resultdb.get_detail_by_unid(**unid)
                result_id = result['rid']
                content = result['content']
                if 'content' in data and data['content']:
                    content = '%s\r\n\r\n%s' % (content, data['content'])
                self.resultdb.update(result_id, {"content": content})
            if result_id:
                self.outqueue.put_nowait({"id": result_id})
            else:
                raise CDSpiderDBError("Result insert failed")
            return result_id
        elif unid:
            crawlinfo = self._build_crawl_info(final_url, createtime)
            return self.resultdb.add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)

    def item_to_attachment(self, rtid, final_url, attachment, data):
        message = {
            "url": final_url,
            "atid": attachment['aid'],
            "save": {"parentid": rtid}
        }
        view_data = data.pop("data_attach_%s" % attachment['aid'], None)
        if view_data:
            message['save']['view_data'] = view_data
        self.requeue.put_nowait(message)

    def item_to_result(self, final_url, data, typeinfo, task, page_source=None, unid=None):
        if unid:
            createtime = unid['createtime']
        else:
            createtime = int(time.time())
        data = utils.filter_item_result(data)
        if not data:
            raise CDSpiderParserNoContent()
        inserted = True
        isfirst = True
        # 判断是否为详情页第一页
        incr_data = task.get('save', {}).get('incr_data', None)
        parentid = task.get('save', {}).get('parentid', '0')
        rid = task.get('rid', None)
        update = True if rid else False
        if incr_data:
            for item in incr_data:
                if not item['first']:
                    isfirst = False
        if not unid:
            inserted, unid = self.uniquedb.insert(self.get_unique_setting(final_url, data), self.task.get("projectid"), self.task.get("siteid"), self.task.get("urlid"), self.task.get("atid"), self.task.get("kwid"), createtime)
            self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                self.crawl_info['crawl_count']['new_count'] += 1
                crawlinfo = self._build_crawl_info(final_url, createtime)
                item = task.get('item', {})
                data = utils.dictjoin(data, item)
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ResultDB.RESULT_STATUS_PARSED, update=update, **unid)
                if rid:
                    self.resultdb.update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.resultdb.insert(result)
            else:
                result = self.resultdb.get_detail_by_unid(**unid)
                result_id = result['rid']
                content = result['content']
                if 'content' in data and data['content']:
                    content = '%s\r\n\r\n%s' % (content, data['content'])
                self.resultdb.update(result_id, {"content": content})
            if not result_id:
                raise CDSpiderDBError("Result insert failed")
            return result_id


from .SearchHandler import SearchHandler
from .GeneralHandler import GeneralHandler
from .AttachHandler import AttachHandler
