#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 23:26:18
"""
import time
import copy
from cdspider import DEFAULT_URLS_SCRIPTS
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.parser.lib.time_parser import Parser as TimeParser

class ResultTrait(object):

    def _build_crawl_info(self, final_url):
        return {
                "tid": self.task.get("tid"),
                "pid": self.task.get("pid"),
                "sid": self.task.get("sid"),
                "uid": self.task.get("uid", 0),
                "kwid": self.task.get("kwid", 0),
                "url": final_url,
                "crawl_id": self.crawl_id,
        }

    def _build_result_info(self, **kwargs):
        result = kwargs.get('result', {})
        update = kwargs.get('update', False)
        if self.mode == self.MODE_ITEM:
            pubtime = result.pop('pubtime', 0)
            if pubtime:
                pubtime = TimeParser.timeformat(str(pubtime))
            if update:
                src = kwargs.get('src', {})
                #TODO 更新列表页抓取任务的crawlinfo
                pubtime = pubtime or src.get('pubtime', None)
                now = src.get('ctime')
            else:
                src = {}
                now = int(time.time())
            if not pubtime or pubtime > now:
                pubtime = now
            r = {
                'status': kwargs.get('status', ArticlesDB.STATUS_INIT),            # 状态
                'url': kwargs['final_url'],
                'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
                'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
                'media_type': self.parse_media_type(kwargs['final_url']),          # 媒体类型
                'title': result.pop('title', None),                                # 标题
                'author': src.get('author', None) or result.pop('author', None),                              # 作者
                'pubtime': pubtime,                                                # 发布时间
                'content': result.pop('content', None),                            # 详情
                'channel': src.get('channel', None) or result.pop('channel', None),                            # 频道信息
                'crawlinfo': kwargs.get('crawlinfo')
                }
            r['result'] = result or None
            if not r['title']:
                r['status'] = ArticlesDB.STATUS_DELETED
                self.no_sync = True
            r = self.result_prepare(r)
        else:
            now = int(time.time())
            pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
            if pubtime and pubtime > now:
                pubtime = now
            r = {
                "status": kwargs.get('status', ArticlesDB.STATUS_INIT),
                'url': kwargs['final_url'],
                'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
                'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
                'title': result.pop('title', None),                                # 标题
                'author': result.pop('author', None),                              # 作者
                'pubtime': pubtime,                            # 发布时间
                'channel': result.pop('channel', None),                            # 频道信息
                'crawlinfo': kwargs.get('crawlinfo')
            }
        if not update:
            r.update({
                'acid': kwargs['unid'],                                            # unique str
                'ctime': kwargs.get('ctime', int(time.time())),
            })
        return r

    def build_item_task(self, rid):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': 'item',
            'pid': self.task.get('pid'),
            'rid': rid,
        }
        self.queue['scheduler2spider'].put_nowait(message)

    def channel_to_list(self, final_url, data, typeinfo, page_source, unique = True, return_result = False):
        """
        频道列表存储并生成任务
        """
        if not data:
            #TODO 与管理平台互动 提醒修改解析规则
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            formated = self.build_url_by_rule(data, final_url)
            self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(formated)))
            if return_result:
                return formated
            for item in formated:
                item['rate'] = self.task.get('site', {}).get('rate', 8)
                item['sid'] = self.task.get("sid")
                item['pid'] = self.task.get("pid")
                item['sub_process'] = None
                item['unique'] = {"url": None, "query": None, "data": None}
                item['scripts'] = DEFAULT_URLS_SCRIPTS.format(projectname = "Project%s" % self.task.get("pid"))
                item['ctime'] = int(time.time())
                item['utime'] = int(time.time())
                item['creator'] = 1
                item['status'] = 1
                uid = self.db['UrlsDB'].insert(item)
                if uid:
                    self.queue['newtask_queue'].put_nowait({"uid": uid})
                    self.crawl_info['crawl_count']['new_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_count'] += 1
                self.on_repetition()

    def list_to_item(self, final_url, data, typeinfo, page_source = None, unid = None, return_result = False):
        """
        列表数据生成详情任务
        """
        ctime = int(time.time())
        if not data:
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            formated = self.build_url_by_rule(data, final_url)
            self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(formated)))
            if return_result:
                return formated
            for item in formated:
                inserted = False
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    crawlinfo =  self._build_crawl_info(final_url)
                    if self.task.get('site', {}).get('type', SitesDB.TYPE_SEARCH) == SitesDB.TYPE_SEARCH:
                        typeinfo = self._typeinfo(item['url'])
                    result = self._build_result_info(final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                    result_id = self.db['ArticlesDB'].insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                    self.build_item_task(result_id)
                elif unid:
                    article = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                    if article.get('crawlinfo', {}).get("pid", self.task.get("pid")) != self.task.get("pid"):
                        self.sync_result.add(article['rid'])
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_count'] += 1
                self.on_repetition()

    def item_to_result(self, final_url, data, typeinfo, page_source=None, unid=None, return_result = False):
        """
        详情存储
        """
        if unid:
            ctime = unid['ctime']
        else:
            ctime = int(time.time())
        if not data:
            raise CDSpiderParserNoContent()
        inserted = True
        isfirst = True
        # 判断是否为详情页第一页
        incr_data = self.task.get('save', {}).get('incr_data', None)
        parentid = self.task.get('save', {}).get('parentid', '0')
        rid = self.task.get('rid', None)
        self.last_result_id = rid
        self.sync_result.add(rid)
        update = True if rid else False
        if incr_data:
            for item in incr_data:
                if 'isfirst' in item and not item['isfirst']:
                    isfirst = False
                    break
        item = self.task.get('item', {})
        crawlinfo = self._build_crawl_info(final_url)
        if return_result:
            formated = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ArticlesDB.STATUS_PARSED, update=update, rid=rid, src=item, unid = self.db['UniqueDB'].build(self.get_unique_setting(final_url, data)), ctime = ctime)
            self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(formated)))
            return formated
        if not unid:
            inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(final_url, data), ctime)
            self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                self.crawl_info['crawl_count']['new_count'] += 1
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ArticlesDB.STATUS_PARSED, update=update, rid=rid, src=item, **unid)
                self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                if rid:
                    self.db['ArticlesDB'].update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.db['ArticlesDB'].insert(result)
                self.item_result_post(result, unid)
            else:
                result = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                result_id = result['rid']
                content = result['content']
                if 'content' in data and data['content']:
                    content = '%s\r\n\r\n%s' % (content, data['content'])
                self.db['ArticlesDB'].update(result_id, {"content": content})
            if not result_id:
                raise CDSpiderDBError("Result insert failed")

    def _build_attach_data_info(self, data):
        result = {
            "views": int(data.get('views') or 0),
            "like_num": int(data.get('like_num') or 0),
            "reposts_num": int(data.get('reposts_num') or 0),
            "comments_num": int(data.get('comments_num') or 0),
        }
        result = self.result_prepare(result)
        return result

    def _build_comments_info(self, data, rid):
        result = {
            ""
        }
        result = self.result_prepare(result)
        return result

    def attach_to_result(self, final_url, data, typeinfo, page_source, unid=None, return_result = False):
        attachment = self.task.get('attachment')
        rid = self.task.get('rid', None)
        if return_result:
            if attachment.get('type', AttachmentDB.TYPE_IMPACT) == AttachmentDB.TYPE_IMPACT:
                return self._build_attach_data_info(data)
            return [self._build_comments_info(item, rid) for item in data]
        if not rid:
            raise CDSpiderSettingError("rid not found")
        self.last_result_id = rid
        article = self.db['ArticlesDB'].get_detail(rid)
        if attachment.get('type', AttachmentDB.TYPE_IMPACT) == AttachmentDB.TYPE_IMPACT:
            '''
            阅读数、点赞数....数据存储
            '''
            result = self._build_attach_data_info(data)
            attach_data = self.db['AttachDataDB'].get_detail(rid)
            if attach_data:
                result = utils.dictunion(result, data)
                result['utime'] = int(time.time())
                self.db['AttachDataDB'].update(rid, result)
            else:
                unid, ctime = ArticlesDB.unbuild_id(rid)
                result['domain'] = article['domain']
                result['subdomain'] = article['subdomain']
                result['ctime'] = int(time.time())
                result['acid'] = article['acid']
                result['utime'] = 0
                result['rid'] = rid
                self.db['AttachDataDB'].insert(result)
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            for item in data:
                result = self._build_comments_info(item, rid)
                result['acid'] = article['acid']
                if unid:
                    ctime = unid['ctime']
                else:
                    ctime = int(time.time())
                inserted = True
                if not unid:
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(final_url, data), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result_id = self.db['CommentsDB'].insert(result)
                    if result_id:
                        self.crawl_info['crawl_count']['new_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_count'] += 1
                self.on_repetition()
