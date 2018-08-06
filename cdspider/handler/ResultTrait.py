#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 23:26:18
"""
import time
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.time_parser import Parser as TimeParser

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
        nocreated = kwargs.get('nocreated', False)
        update = kwargs.get('update', False)
        pubtime = result.pop('pubtime', 0)
        if pubtime:
            pubtime = TimeParser.timeformat(str(pubtime))
        if not pubtime and not nocreated:
            pubtime = self.crawl_id
        r = {
                'status': kwargs.get('status', ArticlesDB.STATUS_INIT),            # 状态
                'url': kwargs['final_url'],
                'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
                'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
                'title': result.pop('title'),                                      # 标题
                'author': result.pop('author'),                                    # 作者
                'pubtime': pubtime,                                                # 发布时间
                'content': result.pop('content', None) if "content" in result else str(result),  # 详情
                'channel': result.pop('channel', None),                            # 频道信息
                'source': kwargs.get('source', None),                              # 抓到的源码
                'crawlinfo': kwargs.get('crawlinfo')
            }
        if not update:
            r.update({
                'unid': kwargs['acid'],                                            # unique str
                'ctime': kwargs.get('ctime', int(time.time())),
            })
        r['result'] = result or None
        return r

    def url_prepare(self, url):
        """
        url预处理
        """
        return url

    def build_url_by_rule(self, data, base_url = None):
        if not base_url:
            base_url = self.task.get('url')
        if not self.process:
            self._init_process()
        urlrule = self.process.get('url', {})
        formated = []
        for item in data:
            if not 'url' in item or not item['url']:
                raise CDSpiderError("url no exists: %s @ %s" % (str(item), str(task)))
            if item['url'].startswith('javascript'):
                continue
            if urlrule:
                parsed = {urlrule['name']: self.url_prepare(item['url'])}
                item['url'] = utils.build_url_by_rule(urlrule, parsed)
            else:
                item['url'] = urljoin(base_url, item['url'])
            formated.append(item)
        return formated

    def build_item_task(self, data, parent_url, rid, unid):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': 'item',
            'tid': 0,
            'pid': self.task.get('pid'),
            'sid': self.task.get('sid'),
            'rid': rid,
            'url': data.pop('url'),
            'save': data,
            'unid': unid,
            'parent_url': parent_url,
        }
        self.queue['schedule2spider'].put_nowait(message)

    def channel_to_list(self, final_url, data, typeinfo, page_source, unique = True):
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
            for item in formated:
                uid = None
                #uid = self.db['urlsdn'].insert()
                #TODO 结果写入url表
                if uid:
                    self.queue['newtask_queue'].put_nowait({"uid": uid})
                    self.crawl_info['crawl_count']['new_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repet_count'] += 1
                self.on_repetition()

    def list_to_item(self, final_url, data, typeinfo, page_source = None, unid = None):
        """
        列表数据生成详情任务
        """
        ctime = self.crawl_id
        if not data:
            raise CDSpiderParserNoContent()
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            formated = self.build_url_by_rule(data, final_url)
            for item in formated:
                inserted = False
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    crawlinfo =  self._build_crawl_info(final_url)
                    typeinfo = self._domain_info(final_url)
                    result = self._build_result_info(final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                    result_id = self.db['ArticlesDB'].insert(result)
                    self.queue['result2kafka'].put_nowait({"rid": result_id})
                    self.last_result_id = result_id
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                    self.build_item_task(item, final_url, result_id, unid)
#                elif unid:
#                    self.db['ArticlesDB'].add_crwal_info(unid['unid'], unid['ctime'], crawlinfo=crawlinfo)
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repet_count'] += 1
                self.on_repetition()

    def item_to_result(self, final_url, data, typeinfo, page_source=None, unid=None):
        """
        详情存储
        """
        if unid:
            ctime = unid['ctime']
        else:
            ctime = self.crawl_id
        if not data:
            raise CDSpiderParserNoContent()
        inserted = True
        isfirst = True
        # 判断是否为详情页第一页
        incr_data = self.task.get('save', {}).get('incr_data', None)
        parentid = self.task.get('save', {}).get('parentid', '0')
        rid = self.task.get('rid', None)
        update = True if rid else False
        if incr_data:
            for item in incr_data:
                if not 'isfirst' in item or not item['isfirst']:
                    isfirst = False
                    break
        if not unid:
            inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(final_url, data), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), ctime)
            self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                self.crawl_info['crawl_count']['new_count'] += 1
                crawlinfo = self._build_crawl_info(final_url, ctime)
                item = self.task.get('item', {})
                data = utils.dictjoin(data, item)
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ArticlesDB.STATUS_PARSED, update=update, **unid)
                if rid:
                    self.db['ArticlesDB'].update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.db['ArticlesDB'].insert(result)
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
            "views": data.get('views', 0),
            "like_num": data.get('like_num', 0),
            "reposts_num": data.get('reposts_num', 0),
            "comments_num": data.get('comments_num', 0),
        }
        return result

    def _build_comments_info(self, data, rid):
        result = {
            ""
        }
        return result

    def attach_to_result(self, final_url, data, typeinfo, page_source, unid=None):
        attachment = self.task.get('attachment')
        rid = self.task.get('rid', None)
        if attachment.get('type', AttachmentDB.TYPE_IMPACT) == AttachmentDB.TYPE_IMPACT:
            '''
            阅读数、点赞数....数据存储
            '''
            if isinstance(data, dict):
                data = [data]
            result = self._build_attach_data_info(data[0])
            attach_data = self.db['AttachDataDB'].get_detail(rid)
            if attach_data:
                result['utime'] = self.crawl_id
                self.db['AttachDataDB'].update(rid, result)
            else:
                unid, ctime = ArticlesDB.unbuild_id(rid)
                result['ctime'] = self.crawl_id
                result['acid'] = unid
                result['utime'] = 0
                self.db['AttachDataDB'].insert(rid, result)
        else:
            self.crawl_info['crawl_count']['count'] += len(data)
            new_count = self.crawl_info['crawl_count']['new_count']
            for item in data:
                result = self._build_comments_info(item, rid)
                if unid:
                    ctime = unid['ctime']
                else:
                    ctime = self.crawl_id
                inserted = True
                if not unid:
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(final_url, data), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), ctime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result_id = self.db['CommentsDB'].insert(result)
                    if result_id:
                        self.crawl_info['crawl_count']['new_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repet_count'] += 1
                self.on_repetition()
