#!/usr/bin/python
# -*- coding: UTF-8 -*-
# date: 2017/11/10
# author: Mr wang
import time
import re
import datetime
from cdspider.libs.utils import decode

class Parser(object):

    @staticmethod
    def get_timestamp(timestring, mode):
        ts = ''
        timeformats = {
            'local': {
                'ym': '%Y年%m月',
                'date': '%Y年%m月%d日',
                'datetime': '%Y年%m月%d日 %H:%M:%S',
                'datetimes': '%Y年%m月%d日 %H:%M',
            },
            'global1': {
                'ym': '%Y-%m',
                'date': '%Y-%m-%d',
                'datetime': '%Y-%m-%d %H:%M:%S',
                'datetimes': '%Y-%m-%d %H:%M',
            },
            'global2': {
                'ym': '%Y/%m',
                'date': '%Y/%m/%d',
                'datetime': '%Y/%m/%d %H:%M:%S',
                'datetimes': '%Y/%m/%d %H:%M',
            },
            'global3': {
                'ym': '%Y.%m',
                'date': '%Y.%m.%d',
                'datetime': '%Y.%m.%d %H:%M:%S',
                'datetimes': '%Y.%m.%d %H:%M',
            },
        }
        if re.findall(r':', timestring):
            try:
                ts = int(time.mktime(time.strptime(timestring, timeformats[mode]['datetimes'])))
            except:
                ts = int(time.mktime(time.strptime(timestring, timeformats[mode]['datetime'])))
            finally:
                return ts
        try:
            ts = int(time.mktime(time.strptime(timestring, timeformats[mode]['date'])))
        except:
            ts = int(time.mktime(time.strptime(timestring, timeformats[mode]['ym'])))
        finally:
            if not ts:
                ts = int(time.time())
            return ts

    @staticmethod
    def timeformat(timestring):
        if not timestring:
            return None
        if re.findall(r'^\d{1,2}月\d{1,2}日', timestring):
            timestring = "%s年%s" % (datetime.datetime.now().year, timestring)
        elif re.findall(r'^\d{1,2}-\d{1,2}[^-]*', timestring):
            timestring = "%s-%s" % (datetime.datetime.now().year, timestring)
        elif re.findall(r'^\d{1,2}\/\d{1,2}[^\/]*', timestring):
            timestring = "%s/%s" % (datetime.datetime.now().year, timestring)
        elif re.findall(r'^\d{1,2}\.\d{1,2}[^\.]*', timestring):
            timestring = "%s.%s" % (datetime.datetime.now().year, timestring)
        if re.findall(r'年',timestring):
            if re.findall(r'^\d{2}年', timestring):
                timestring = "%s%s" % (20, timestring)
            return Parser.get_timestamp(timestring, 'local')
        elif re.findall(r'\-',timestring):
            if re.findall(r'^\d{2}-\d{1,2}-\d{1,2}', timestring):
                timestring = "%s%s" % (20, timestring)
            timestring = re.sub("\+\d{2}(:\d+)?", "", re.sub("T", " ", timestring))
            return Parser.get_timestamp(timestring, 'global1')
        elif re.findall(r'\/',timestring):
            if re.findall(r'^\d{2}\/\d{1,2}\/\d{1,2}', timestring):
                timestring = "%s%s" % (20, timestring)
            timestring = re.sub("\+\d{2}(:\d+)?", "", re.sub("T", " ", timestring))
            return Parser.get_timestamp(timestring, 'global2')
        elif re.findall(r'\.', timestring):
            if re.findall(r'^\d{2}\.\d{1,2}\.\d{1,2}', timestring):
                timestring = "%s%s" % (20, timestring)
            timestring = re.sub("\+\d{2}(:\d+)?", "", re.sub("T", " ", timestring))
            return Parser.get_timestamp(timestring, 'global3')
        elif re.findall(r':',timestring):
            timestring = "%s %s" % (time.strftime("%Y-%m-%d"), timestring)
            return Parser.get_timestamp(timestring, 'global1')
        elif re.findall(r'秒前',timestring):
            dd=re.match(r'\d',timestring)
            w=int(dd.group())
            d = (datetime.date.today() - datetime.timedelta(seconds = w)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'分钟前',timestring):
            dd=re.match(r'\d',timestring)
            w=int(dd.group())
            d = (datetime.date.today() - datetime.timedelta(minutes = w)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'小时前',timestring):
            dd=re.match(r'\d',timestring)
            w=int(dd.group())
            d = (datetime.date.today() - datetime.timedelta(hours = w)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'天前',timestring):
            dd=re.match(r'\d',timestring)
            w=int(dd.group())
            d = (datetime.date.today() - datetime.timedelta(days = w)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
            return timeStamp
        elif re.findall(r'周前',timestring):
            dd=re.match(r'\d',timestring)
            w=int(dd.group())
            d = (datetime.date.today() - datetime.timedelta(weeks = w)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'今天',timestring):
            t = time.localtime(time.time())
            return int(time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'昨天',timestring):
            d = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        elif re.findall(r'前天',timestring):
            d = (datetime.date.today() - datetime.timedelta(days = 2)).strftime("%Y-%m-%d 00:00:00")
            return int(time.mktime(time.strptime(d,'%Y-%m-%d %H:%M:%S')))
        else:
            try:
                return int(timestring)
            except:
                return None

    @staticmethod
    def parser_time_from_url(url):
        d = re.findall(r'20[012]-\d{1,2}-\d{1,2}', url)
        if d:
            return int(time.mktime(time.strptime(d[0],'%Y-%m-%d')))
        d = re.findall(r'20[012]\d[01]\d[0123]\d', url)
        if d:
            return int(time.mktime(time.strptime(d[0],'%Y%m%d')))
        r = '20[012]\d(?:%s)(?:%s)' % ('|'.join([str(i) for i in range(1, 13)]), '|'.join([str(i) for i in range(1, 32)]))
        d = re.findall(r, url)
        if d:
            return int(time.mktime(time.strptime(d[0],'%Y%m%d')))
        return None

    @staticmethod
    def parser_time(html, now = False):
        rule1='((?:(?:(?:(?:20)?[012]\d)|(?:19)?[98]\d)年\d{1,2}月\d{1,2}日?(?:\D*\d{2}:\d{2}(?::\d{2})?)?)|(?:(?:(?:(?:20)?[012]\d)|(?:19)?[9]\d)[\-\/]\d{1,2}[\-\/]\d{1,2}(?:\D*\d{1,2}:\d{2}(?::\d{2})?)?))'
        rule11 = '\>\040*'+ rule1 +'[\040&\<]'
        rule12 = '[\>\040;](?:发表于|发布(?:时间|日期)|日期)\040*(?:\:|：)?\040*'+ rule1
        rule13 = rule1 +'\s*发布'
        rule14 = '[\040\<;]' + rule1 +'\040*\<'
        if isinstance(html, bytes):
            try:
                html = decode(html)
            except:
                html = str(html)
        if html:
            g1 = re.findall(rule12, html) or re.findall(rule13, html) or re.findall(rule11, html) or re.findall(rule14, html)
            for item in g1:
                if item:
                    return item
        if now:
            return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        return None
