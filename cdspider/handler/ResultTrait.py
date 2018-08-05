#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 23:26:18
"""

class ResultTrait(object):

    def _build_crawl_info(self, final_url, createtime):
        return {
            str(self.crawl_id): {
                "task": self.task.get("tid"),
                "project": self.task.get("pid"),
                "site": self.task.get("sid"),
                "urls": self.task.get("uid", 0),
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
                'status': kwargs.get('status', ArticlesDB.STATUS_INIT),            # 状态
                'url': kwargs['final_url'],
                'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
                'title': result.pop('title', result.pop('title', None)),           # 标题
                'author': result.pop('author', result.pop('author', None)),        # 作者
                'created': created,                                                # 发布时间
                'summary': result.pop('summary', None),                            # 摘要
                'content': result.pop('content', None) if "content" in result else str(result),           # 详情
                'crawlinfo': kwargs['crawlinfo'],                                  # 抓取信息 [{"project":pid,"task":taskid,"urls":uid,"keywords":keywordid,"crawltime":crawltime},..]
                'source': kwargs.get('source', None),                              # 抓到的源码
            }
        if not update:
            r.update({
                'unid': kwargs['unid'],                           # unique str
                "pid": self.task.get("pid"),
                "sid": self.task.get("sid"),
                "uid": self.task.get("uid", 0),
                "aid": self.task.get("aid", 0),
                "kwid": self.task.get("kwid", 0),
                'createtime': kwargs.get('createtime', int(time.time())),
            })
        r['result'] = result or None
        return r

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
                    inserted, unid = self.db['uniquedb'].insert(self.get_unique_setting(item['url'], item), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), createtime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                crawlinfo = self._build_crawl_info(base_url, createtime)
                if inserted:
                    self.crawl_info['crawl_count']['new_count'] += 1
                    result = self._build_result_info(final_url=item['url'], result=item, crawlinfo=crawlinfo, nocreated=True, **unid)
                    result['parentid'] = parentid
                    result_id = self.db['articlesdb'].insert(result)
                    if result_id:
                        self.queue['schedule2spider'].put_nowait({"id": result_id, 'task': 1})
                    else:
                        raise CDSpiderDBError("Result insert failed")
                elif unid:
                    self.db['articlesdb'].add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)
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
                    inserted, unid = self.db['uniquedb'].insert(self.get_unique_setting(final_url, data), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), createtime)
                    self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                crawlinfo = self._build_crawl_info(final_url, createtime)
                if inserted:
                    self.crawl_info['crawl_count']['new_count'] += 1
                    if 'url' in item and iten['url']:
                        item['url'] = urljoin(final_url, item['url'])
                    result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=item, crawlinfo=crawlinfo, **unid)
                    result['parentid'] = parentid
                    result_id = self.db['articlesdb'].insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                elif unid:
                    self.db['articlesdb'].add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)
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
            inserted, unid = self.db['uniquedb'].insert(self.get_unique_setting(final_url, data), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), createtime)
            if not isfirst:
                inserted = True
            self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                item = task.get('item', {})
                data = utils.dictjoin(data, item)
                self.crawl_info['crawl_count']['new_count'] += 1
                crawlinfo = self._build_crawl_info(final_url, createtime)
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ArticlesDB.STATUS_PARSED, update=update, **unid)
                if rid:
                    self.db['articlesdb'].update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.db['articlesdb'].insert(result)
            else:
                result = self.db['articlesdb'].get_detail_by_unid(**unid)
                result_id = result['rid']
                content = result['content']
                if 'content' in data and data['content']:
                    content = '%s\r\n\r\n%s' % (content, data['content'])
                self.db['articlesdb'].update(result_id, {"content": content})
            if result_id:
                self.queue['schedule2spider'].put_nowait({"id": result_id})
            else:
                raise CDSpiderDBError("Result insert failed")
            return result_id
        elif unid:
            crawlinfo = self._build_crawl_info(final_url, createtime)
            return self.db['articlesdb'].add_crwal_info(unid['unid'], unid['createtime'], crawlinfo=crawlinfo)

    def item_to_attachment(self, rtid, final_url, attachment, data):
        message = {
            "url": final_url,
            "aid": attachment['aid'],
            "save": {"parentid": rtid}
        }
        view_data = data.pop("data_attach_%s" % attachment['aid'], None)
        if view_data:
            message['save']['view_data'] = view_data
        self.queue['newtask_queue'].put_nowait(message)

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
            inserted, unid = self.db['uniquedb'].insert(self.get_unique_setting(final_url, data), self.task.get("pid"), self.task.get("sid"), self.task.get("uid"), self.task.get("aid"), self.task.get("kwid"), createtime)
            self.logger.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
        if inserted:
            if isfirst:
                self.crawl_info['crawl_count']['new_count'] += 1
                crawlinfo = self._build_crawl_info(final_url, createtime)
                item = task.get('item', {})
                data = utils.dictjoin(data, item)
                result = self._build_result_info(final_url=final_url, typeinfo=typeinfo, result=data, crawlinfo=crawlinfo, source=utils.decode(page_source), status=ArticlesDB.STATUS_PARSED, update=update, **unid)
                if rid:
                    self.db['articlesdb'].update(rid, result)
                    result_id = rid
                else:
                    result['parentid'] = parentid
                    result_id = self.db['articlesdb'].insert(result)
            else:
                result = self.db['articlesdb'].get_detail_by_unid(**unid)
                result_id = result['rid']
                content = result['content']
                if 'content' in data and data['content']:
                    content = '%s\r\n\r\n%s' % (content, data['content'])
                self.db['articlesdb'].update(result_id, {"content": content})
            if not result_id:
                raise CDSpiderDBError("Result insert failed")
            return result_id

