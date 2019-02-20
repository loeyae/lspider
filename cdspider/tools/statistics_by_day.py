#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-22 14:31:27
"""
import time
import datetime
import traceback
from cdspider.libs import utils
from cdspider.tools import Base
from cdspider.mailer import BaseSender
from cdspider.exceptions import *

class statistics_by_day(Base):
    """
    按天统计文章数量，并发送给指定的人员
    """
    inqueue_key = None

    def __init__(self, context, daemon = False):
        super(statistics_by_day, self).__init__(context, daemon)
        self.db = self.g['db']
        self.config = self.g['app_config']
        self.running = False
        self._run_once = False

    def process(self, *args):
        running_time = int(args[0]) if len(args) > 0 else None
        '''
        运行时间
        '''
        if running_time != None:
            h = int(time.strftime('%H'))
            if h == running_time:
                self.running = True
            else:
                self.running = False
            if h > running_time and self._run_once == True:
                self._run_once = False
        if self.running == False or self._run_once == True:
            self.debug("statistics sleep")
            time.sleep(60)
            return
        self.debug("statistics starting")
        config = self.config['mail']
        mailer = config['mailer']
        sender = config['sender']
        receiver = self.config['statistics_receiver']
        self.mailer = utils.load_mailer(mailer, sender=sender, receiver=receiver)
        if self.mailer and isinstance(self.mailer, BaseSender):
            try:
                t = int(time.mktime(time.strptime(str(datetime.datetime.today().date() - datetime.timedelta(days=1)), "%Y-%m-%d")))
                self.debug("statistics time: %s" % t)
                total = self.db['ArticlesDB'].get_count(t, where={"ctime": {"$gte": t, "$lt": t + 24 * 3600}})

                self.debug("statistics total: %(total)s" % {"total": total})
                subject = "数据统计-%s" % str(datetime.datetime.today().date() - datetime.timedelta(days=1))
                message = """<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>{subject}</title>
    </head>
    <body>
        <table border=3>
            <tr>
                <td></td><td>总计</td>
            </tr>
            <tr>
                <td>数量</td><td>{total}</td>
            </tr>
        </table>
    </body>
</html>
"""
                message = message.format(subject=subject, total=total)
                self.mailer.send(subject=subject, message=message, type='html')
                self.run_once = True
            except:
                self.error(traceback.format_exc())
        else:
            self.error("mailer not exists")
        self.debug("statistics end")
