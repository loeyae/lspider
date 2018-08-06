#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:27:08
"""
import time
from cdspider.database.base import TaskDB
from cdspider.worker import BaseWorker
from cdspider.parser import ItemParser
from cdspider.spider import Spider
from cdspider.libs import utils
from cdspider.libs.time_parser import Parser as TimeParser

class ResultWorker(BaseWorker):
    """
    结果处理
    """

    def get_task(self, data):
        task = {
            "mode": 'item',
            "projectid": data.get("projectid"),
            "status": TaskDB.TASK_STATUS_ACTIVE,
            "siteid": data.get("siteid"),
            "urlid": data.get("urlid", 0),
            "kwid": data.get("kwid", 0),
            "url": data.get('url'),
            "atid": data.get('atid', 0),
            "unid": {"unid": data.get('unid'), "createtime": data.get('createtime')},
            "rid": data.get('rid'),
            "item": {
                'title': data.get('title', None),
                'author': data.get('author', None),
                'created': data.get('created', None),
                'content': data.get('content', None),
                'summary': data.get('summary', None),
                },
            "save": {
                "base_url": data.get('url')
            }
        }
        return task

    def on_result(self, message):
        self.logger.debug("got message: %s" % message)
        result = self.get_result(message)
        if not result or result['status'] != self.resultdb.RESULT_STATUS_INIT:
            return
        data={}
        if message.get('task', 0) == 1:
            task = self.get_task(result)
            task['queue_message'] = message
            task['queue'] = self.inqueue
            inqueue=None
            outqueue=self.inqueue
            status_queue = None
            requeue = self.outqueue
            excqueue=self.excqueue
            ProjectsDB=self.ProjectsDB
            sitetypedb = self.sitetypedb
            TaskDB = None
            SitesDB=self.SitesDB
            UniqueDB=self.UniqueDB
            UrlsDB=self.UrlsDB
            AttachmentDB=self.AttachmentDB
            KeywordsDB=self.KeywordsDB
            customdb=self.customdb
            resultdb=self.resultdb
            spider = Spider(inqueue=inqueue, outqueue=outqueue, status_queue=status_queue, requeue=requeue,
            excqueue=excqueue, ProjectsDB=ProjectsDB, sitetypedb=sitetypedb, TaskDB=TaskDB, SitesDB=SitesDB,
            resultdb=resultdb, customdb=customdb, UniqueDB=UniqueDB, UrlsDB=UrlsDB, KeywordsDB=KeywordsDB,
            AttachmentDB=AttachmentDB, handler=None, proxy=self.proxy, log_level=self.log_level)
            task = spider.get_task({'pid': result.get('projectid')}, task)
            spider.fetch(task)
        else:
            parser = ItemParser(source=result['source'], ruleset=None)
            data = parser.parse()
            if data and 'item' in data and data['item']:
                data['item'] = utils.dictjoin(data['item'], result)
                created = data['item'].pop('created', None)
                if created:
                    created = TimeParser.timeformat(TimeParser.parser_time(str(created)))
                if not created:
                    created = int(time.time())
                update = {}
                update['title'] = data['item'].pop('title', None)
                update['author'] = data['item'].pop('author', None)
                update['summary'] = data['item'].pop('summary', None)
                update['content'] = data['item'].pop('content', None)
                update['created'] = created
                update['status'] = self.resultdb.RESULT_STATUS_PARSED
                update['updatetime'] = int(time.time())
                if not result['item']:
                    del result['item']
                update['result'] = result or None
                self.resultdb.update(message['id'], update)

    def get_result(self, message):
        if not 'id' in message or not message['id']:
            return None
        id = message['id']
        return self.resultdb.get_detail(id)
