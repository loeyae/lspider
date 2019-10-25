# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019/10/23 10:50
"""
import os
import datetime
import requests
import mimetypes
from cdspider.worker import BaseWorker
from cdspider.libs.constants import *
from cdspider.crawler import BaseCrawler
from cdspider.libs import utils

class DownloadWorker(BaseWorker):
    """
    download_worker
    """

    inqueue_key = QUEUE_NAME_SPIDER_TO_DOWNLOAD
    
    def __init__(self, context):
        super(DownloadWorker, self).__init__(context)
        self.runtime_dir = self.g.get('runtime_dir')

    def on_result(self, message):
        self.debug("got message: %s" % message)
        if 'rid' not in message or not message['rid']:
            raise CDSpiderError("rid not in message")
        rid = message['rid']
        article = self.db['ArticlesDB'].get_detail(rid)
        if not article:
            raise CDSpiderDBDataNotFound("article: %s not exists" % rid)
        self.debug("got article: %s" % article)
        field = message['field']
        file_urls = article.get(field)
        self.debug("got urls: %s" % file_urls)
        currentday = datetime.datetime.now()
        current_month = currentday.strftime('%m')
        current_year = currentday.strftime('%Y')
        sd = os.path.join("download", current_year, current_month)
        d = os.path.join(self.runtime_dir, sd)
        if not os.path.exists(d):
            os.makedirs(d)
        download_info = self.download(d, sd, file_urls)
        self.db['ArticlesDB'].update(rid, {"downloadInfo": download_info})

    def download(self, d, sd, file_urls):
        self.debug("download urls: %s" % file_urls)
        if isinstance(file_urls, (list, tuple)):
            return [self.download(d, sd, item) for item in file_urls]
        elif isinstance(file_urls, dict):
            return [self.download(d, sd, item) for _,item in file_urls.items()]
        else:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36"}
            end = False
            retry = 0
            while end is False:
                try:
                    res = requests.get(file_urls, headers=headers, timeout=(30,30))
                    end = True
                except requests.exceptions.ConnectionError:
                    retry += 1
                    if retry > 5:
                        end = True
                except Exception:
                    end = True

            if res.status_code == BaseCrawler.STATUS_CODE_OK:
                ext = mimetypes.guess_extension(res.headers["Content-Type"])
                if ext is None:
                    ext = mimetypes.guess_extension(res.headers["Content-Type"], strict=False)
                file_name = "%s%s" % (utils.md5(file_urls), ext)
                full_file_name = os.path.join(d, file_name)
                short_file_name = os.path.join(sd, file_name)
                with open(full_file_name, "wb") as fp:
                    fp.write(res.content)
                    fp.close()
                res.close()
                return {"src": file_urls, "file": short_file_name}
            res.close()
            return {"src": file_urls, "file": ""}
