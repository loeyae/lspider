#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: utils.py 2233 2018-07-06 01:26:22Z zhangyi $
import sys
import os
import re
import chardet
import logging
import traceback
import base64
import json
import hashlib
import datetime
from urllib import parse
from tld import get_tld
from types import *
import subprocess
import random
import types
import string
import six
from threading import Thread
from multiprocessing import Process
import stevedore
import datetime

def build_url_by_rule(rule, params):
    url = rule.get('base')
    if rule.get('mode', 'get') == 'format':
        return url.format(**params)
    return build_query(url, params)

def parse_domain(url):
    try:
        presult = parse.urlparse(url)
        domain = get_tld(url)
        hostname = presult.hostname
        end = hostname.find(domain)-1
        subdomain = hostname[0:end] or ''
        return subdomain, domain
    except:
        return None, None

def remove_punctuation(content):
    if not isinstance(content, str):
        content = content.decode('utf-8')
    tbl = dict.fromkeys(ord(x) for x in string.punctuation)
    return content.translate(tbl)

def remove_whtiespace_plus(content):
    if not isinstance(content, str):
        content = content.decode('utf-8')
    tbl = dict.fromkeys(ord(x) for x in string.whitespace)
    return content.translate(tbl)

def remove_whitespace(content):
    if not content:
        return None
    if isinstance(content, bytes):
        content = decode(content)
    elif not isinstance(content, str):
        content = str(content)
    tbl = dict.fromkeys(ord(x) for x in '\t\v\f')
    content = content.translate(tbl)
    return re.sub('((?:\r\n\s*)+|(?:\r\s*)+|(?:\n\s*)+)', '\r\n', content)

def decode(data, errors="ignore"):
    if isinstance(data, bytes):
        encoding =  chardet.detect(data)
        u = encoding['encoding']
        if not u:
            find_charset = re.compile(
                br'<meta.*?charset=["\']*([a-z0-9\-_]+?) *?["\'>]', flags=re.I
            ).findall
            encoding = [item.decode('utf-8') for item in find_charset(data)]
            u = encoding and encoding[0] or None
        if u:
            return data.decode(u, errors=errors)
        try:
            return data.decode("gb2312")
        except:
            return data.decode("gbk")
        return data.decode("utf-8")
    return data

def mgkeyconvert(data, restore = False):
    cdata = {}
    if restore:
        trantab = str.maketrans('\0\1\2\3\4\5\6\7\10\11', '/."$*<>:|?')
    else:
        trantab = str.maketrans('/."$*<>:|?', '\0\1\2\3\4\5\6\7\10\11')
    for k, v in data.items():
        k = k.translate(trantab)
        if isinstance(v, dict):
            cdata[k] = mgconvert(v, restore)
        else:
            cdata[k] = v
    return cdata


def filter_list_result(data):
    if not data:
        return None
    rest = []
    for k, item in data.items():
        if not item:
            continue
        if not isinstance(item, (list, tuple)):
           item = [item]
        rest.extend(item)
    return rest

def filter_item_result(data):
    if not data:
        return None
    rest = {}
    isauto = False
    if 'item' in data and isinstance(data['item'], dict):
        if 'raw_content' in data['item']:
            isauto = True
    for key, item in data.items():
        item_len = len(item)
        if isinstance(item, dict):
            item = dict((k, v) for k, v in item.items() if not v is None and v != '')
            if isauto and key != 'item':
                item.pop('title', None)
                item.pop('created', None)
                item.pop('author', None)
                item.pop('content', None)
                item.pop('raw_content', None)
                item.pop('raw_title', None)
            if not item:
                return None
            if isauto:
                if item_len > 5 and len(item) < 6:
                    return None
                elif len(item) < 4:
                    return None
            rest.update(item)
        else:
            rest.update({key: item})
    return rest

def get_current_month_days():
    currentday = datetime.datetime.now()
    currentMonth = currentday.strftime('%m')
    currentYear = currentday.strftime('%Y')
    d1 = datetime.datetime(int(currentYear),int(currentMonth),1)
    d2 = datetime.datetime(int(currentYear),int(currentMonth)+1,1)
    days = d2 - d1
    day = days.days
    return day

def patch_result(data, rule, callback=None):
        prefix = rule.get('prefix', '')
        suffix = rule.get('suffix', '')
        patch = rule.get('patch', None)
        if isinstance(data, list):
            if patch:
                return [re.sub('\[\w+\]', str(d), patch) if not isinstance(d, (list, dict)) and d else d for d in data]
            return ["%s%s%s" %(prefix, callback_result(callback, d), suffix) if not isinstance(d, (list, dict)) and d else d for d in data]
        elif isinstance(data, dict):
            if patch:
                return dict((k, re.sub('\[\w+\]', str(d), patch)) if not isinstance(d, (list, dict)) and d else d for k,d in data.items())
            return dict((k, "%s%s%s" %(prefix, callback_result(callback, d), suffix)) if not isinstance(d, (list, dict)) else d and d for k,d in data.items())
        else:
            rst = callback_result(callback, data)
            if patch:
                return re.sub('\[\w+\]', str(rst), patch) if not isinstance(rst, (list, dict)) and rst else rst
            return "%s%s%s" %(prefix, rst, suffix) if not isinstance(rst, (list, dict)) and rst else rst

def load_config(f):
    """
    load配置文件
    """
    if not f or not os.path.isfile(f):
        return {}

    def underline_dict(d):
        if not isinstance(d, dict):
            return d
        return dict((k.replace('-', '_'), underline_dict(v)) for k, v in six.iteritems(d))

    config = underline_dict(json.load(open(f, encoding='utf-8')))
    return config

def query2dict(value):
    if not value:
        return {}
    return dict([(k, v[0]) for k, v in parse.parse_qs(value).items()])

def parse_arg(ctx, param, value):
    return query2dict(value)

def rule2pattern(rule):
    """
    将页面配置转化为正则表达式
    """
    p1 = re.compile(r'([^\[]*)((?:\[\*\])?)(.*)\[r\=(\w+)\](.+)\[\/r\]([^\[]*)((?:\[\*\])?)(.*)', re.M|re.I)
    g1 = p1.search(rule)
    key = None
    if g1:
        rule = '%s%s%s(?P<%s>%s)%s%s%s' % (g1.group(1), '.*?' if g1.group(2) else '', g1.group(3), g1.group(4), '.+?' if g1.group(5) == "[*]" else g1.group(5), g1.group(6), '.*?' if g1.group(7) else '', g1.group(8) )
        key = g1.group(4)
    else:
        p2 = re.compile(r'([^\[]*)((?:\[\*\])?)(.*)\[(\w+)\]([^\[]*)((?:\[\*\])?)(.*)', re.M|re.I)
        g2 = p2.search(rule)
        if g2:
            rule = '%s%s%s(?P<%s>.+?)%s%s%s' % (g2.group(1), '.*?' if g2.group(2) else '', g2.group(3), g2.group(4), g2.group(5), '.*?' if g2.group(6) else '', g2.group(7))
            key = g2.group(4)
        else:
            return (None, None)
    return (rule, key)

def rule2subitem(rule, subject):
    """
    将页面配置转化为正则表达式
    """
    p1 = re.compile(r'([^\[]*)((?:\[\*\])?)(.*)\[r\=(\w+)\](.+)\[\/r\]([^\[]*)((?:\[\*\])?)(.*)', re.M|re.I)
    g1 = p1.search(rule)
    if g1:
        pt1 = '%s%s%s' % (g1.group(1), '.*?' if g1.group(2) else '', g1.group(3))
        pt2 = '%s%s%s' % (g1.group(6), '.*?' if g1.group(7) else '', g1.group(8))
        rule = '%s%s%s' % ('(%s)' % pt1 if pt1 else '', '.+?' if g1.group(5) == "[*]" else g1.group(5), '(%s)' % pt2 if pt2 else '')
        subject = '%s%s%s' % ('\\1' if pt1 else '' , subject, '\\2' if pt1 and pt2 else ('\\1' if pt2 else ''))
    else:
        p2 = re.compile(r'([^\[]*)((?:\[\*\])?)(.*)\[(\w+)\]([^\[]*)((?:\[\*\])?)(.*)', re.M|re.I)
        g2 = p2.search(rule)
        if g2:
            pt1 = '%s%s%s' % (g2.group(1), '.*?' if g2.group(2) else '', g2.group(3))
            pt2 = '%s%s%s' % (g2.group(5), '.*?' if g2.group(6) else '', g2.group(7))
            rule = '%s.+?%s' % ('(%s)' % pt1 if pt1 else '', '(%s)' % pt2 if pt2 else '')
            subject = '%s%s%s' % ('\\1' if pt1 else '' , subject, '\\2' if pt1 and pt2 else ('\\1' if pt2 else ''))
    return (rule, subject)

def load_crawler(crawler, *args, **kwargs):
    """
    以插件模式加载crawler
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.crawler',
        name=crawler,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def load_parser(parser, *args, **kwargs):
    """
    以插件模式加载parser
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.parser',
        name=parser,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def load_queue(queue, *args, **kwargs):
    """
    以插件模式加载queue
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.queue',
        name=queue,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def load_mailer(mailer, *args, **kwargs):
    """
    以插件模式加载mailer
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.mailer',
        name=mailer,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def load_db(driver_name, *args, **kwargs):
    """
    以插件模式加载db
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.db',
        name=driver_name,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def load_handler(name, *args, **kwargs):
    """
    以插件模式加载handler
    """
    mgr = stevedore.driver.DriverManager(
        namespace='cdspider.handler',
        name=name,
        invoke_args = args,
        invoke_kwds = kwargs,
        invoke_on_load=True,
    )
    return mgr.driver

def run_in_thread(func, *args, **kwargs):
    """Run function in thread, return a Thread object"""
    thread = Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread


def run_in_subprocess(func, *args, **kwargs):
    """Run function in subprocess, return a Process object"""
    thread = Process(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread

def run_cmd(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout.read()

def url_encode(params):
    g_encode_params = {}

    def _encode_params(params, p_key=None):
        encode_params = {}
        if isinstance(params, dict):
            for key in params:
                encode_key = '{}[{}]'.format(p_key,key)
                encode_params[encode_key] = params[key]
        elif isinstance(params, (list, tuple)):
            for offset,value in enumerate(params):
                encode_key = '{}[{}]'.format(p_key, offset)
                encode_params[encode_key] = value
        else:
            g_encode_params[p_key] = params

        for key in encode_params:
            value = encode_params[key]
            _encode_params(value, key)

    if isinstance(params, dict):
        for key in params:
            _encode_params(params[key], key)

    return parse.urlencode(g_encode_params)

def build_query(url, query):
    arr = parse.urlparse(url)
    if arr.query:
        query1 = parse.parse_qs(arr.query)
    else:
        query1 = {}
    if isinstance(query, str):
        query = parse.parse_qs(query)
    query1.update(query)
    query2 = {}
    for i,v in query1.items():
        if v:
            if isinstance(v, (list, tuple)):
               query2[i] = v[0] or ""
            else:
                query2[i] = str(v)  or ""
    querystr = url_encode(query2)
    return parse.urlunparse((arr.scheme, arr.netloc, arr.path, arr.params, querystr, arr.fragment))

def build_filter_query(url, query):
    arr = parse.urlparse(url)
    if arr.query:
        query1 = parse.parse_qs(arr.query)
    else:
        query1 = {}
    query2 = {}
    for i,v in query1.items():
        if i in query and v:
            if isinstance(v, (list, tuple)):
               query2[i] = v[0] or ""
            else:
                query2[i] = str(v)  or ""
    querystr = url_encode(query2)
    return parse.urlunparse((arr.scheme, arr.netloc, arr.path, arr.params, querystr, arr.fragment))

def base64encode(text):
    return base64.b64encode(text.encode('utf-8')).decode('utf-8')

def base64decode(text):
    return base64.b64decode(text.encode('utf-8')).decode('utf-8')

def randomint(wait):
    if isinstance(wait, list):
        max = int(wait.pop())
        if wait:
            min = int(wait.pop())
        else:
            min = int(max / 2)
    elif isinstance(wait, dict):
         max = int(wait.get("max", 0))
         min = int(wait.get("min", 0))
    else:
        max =  int(wait)
        min = int(max / 2)
    return random.randint(min, max)

def map_build(data, mapping, clear = False):
    if isinstance(data, dict):
        for name,value in data.items():
            if name in mapping:
                data[mapping[name]] = value
                if clear:
                    del data[name]
    return data

def desc2date(desc):
    today = datetime.date.today()
    if desc == '前天':
        return str(today - datetime.timedelta(days=2))
    if desc == '昨天':
        return str(today - datetime.timedelta(days=1))
    return desc

def md5(key):
    return hashlib.md5(str(key).encode("utf-8")).hexdigest()

def str2json(json_str):
    data = json.loads(json_str)
    return data

def merge(data1, data2):
    if isinstance(data1, list) and  isinstance(data2, list):
        l = len(data2)
        for i in range(len(data1)):
            if i < l:
                data1[i] = merge(data1[i], data2[i])
        return data1
    elif isinstance(data1, dict) and  isinstance(data2, dict):
        data1.update(data2)
        return data1
    else:
        return str(data1) + str(data2)

def list_column(list1, key, column = None):
    if column:
        return dict((item[key],item[column]) for item in list1 if key in item and column in item)
    return dict((item[key],item) for item in list1 if key in item)

def filter(data):
    if isinstance(data, list):
        return [item for item in data if item]
    elif isinstance(data, dict):
        return dict((k, v) for k, v in data.items() if v)
    return data

def iterator2list(iterator):
    """
    迭代器转化为list
    :params:
        - iterator 迭代器
    """
    return list(iterator)

def list2dict(keylist, valuelist):
    """
    返回以列表keylist的值为key,对应valuelist位置的值为value的字典，如果valuelist的长度小于keylist的长度，则返回 None
    """
    if len(valuelist) < len(keylist):
        return None
    return dict((keylist[idx],valuelist[idx]) for idx in range(len(keylist)))

def dictjoin(dict1, dict2):
    """
    返回出现在dict1或dict2中key对应的字典，如果dict1和dict2中都存在该key，并且都不为空，则保留dict1中的值
    """
    for key in dict1:
        val = dict2.pop(key, None)
        if (dict1[key] == None or dict1[key] == '') and val != None and val != '':
            dict1[key] = val
    dict1.update(dict2)
    return dict1

def dictunion(dict1, dict2):
    """
    返回key出现在dict1中同时出现在dict2的对应字典
    """
    return dict((x,y) for x,y in dict1.items() if x in dict2)

def table2kvlist(data):
    """
    将table形式数据转换为键值对列表
    :params
        - data dict
    :ex:
        data:
            {
                "title": ["t1","t2","t3",..]
                "url": ["u1","u2","u3",..]
                ...
            }
        set:
            [
                {"title": "t1", "url": "u1",..}
                {"title": "t2", "url": "u2",..}
                {"title": "t3", "url": "u3",..}
                .
                .
                .
            ]
    """
    keys = list(data.keys())
    values = list(data.values())
    d = []
    l = len(keys)
    while True:
        item = []
        for i in range(l):
            if len(values[i]) <= 0:
                return d
            item.append(values[i].pop(0))
        d.append(dict(zip(keys,item)))
    return d

def list2kv(data):
    """"
    返回以list第一行为key，其他行为value的数据集
    :params:
        - data list
    :ex:
        data:
            name,value
            aaa,bbb
            ccc,ddd
        set:
            [
                {'name':'aaa', 'value':'bbb'},
                {'name':'ccc', 'value':'ddd'},
            ]
    """
    keys = data[0]
    values = data[1:]
    return [dict((keys[i],item[i]) for i in range(len(item))) for item in values]

def utf8(string):
    """
    Make sure string is utf8 encoded bytes.

    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if isinstance(string, six.text_type):
        return string.encode('utf8')
    elif isinstance(string, six.binary_type):
        return string
    else:
        return six.text_type(string).encode('utf8')


def text(string, encoding='utf8'):
    """
    Make sure string is unicode type, decode with given encoding if it's not.

    If parameter is a object, object.__str__ will been called
    """
    if isinstance(string, six.text_type):
        return string
    elif isinstance(string, six.binary_type):
        return string.decode(encoding)
    else:
        return six.text_type(string)

def get_object(object_string):
    """
    动态返回module中的对象
    """
    if "." not in object_string:
        raise Exception("get method need module.method or module.object.method")
    module_name, class_name = object_string.rsplit(".", 1)
    try:
        if six.PY2:
            module = __import__(module_name, globals(), locals(), [utf8(class_name)], -1)
        else:
            module = __import__(module_name, globals(), locals(), [class_name])
    except:
        importstring = "import %s" % module_name
        exec(importstring)
        module = eval(module_name)
    obj = getattr(module, class_name)
    if isinstance(obj, types.ModuleType):
        return getattr(obj, class_name)
    return obj

def get_method(method_string, *args, **kwargs):
    """
    动态返回对象
    """
    if "." not in method_string:
        raise Exception("get method need module.method or module.object.method")
    object_string, method_name = method_string.rsplit(".", 1)
    o = get_object(object_string)(*args, **kwargs)
    return getattr(o, method_name)

def callback_result(func, data):
    """
    返回回调函数对数据处理的结果
    """
    if not func:
        return data
    try:
        rst = get_method(func, data)
        return rst
    except:
        logging.error(traceback.format_exc())
        return data

def pcre2re(pattern, m = None):
    """
    pecre模式正则表达式转换为re模式
    """
    mode = {'i':'I','m':'M','s':'S','u':'U'}
    if pattern[0:1] == '#':
        ids = 1
        idx = pattern[1:].index('#') + 1
    elif pattern[0:1] == '/':
        ids = 1
        idx = pattern[1:].index('/') + 1
    else:
        ids = 0
        idx = len(pattern)
    ptr =  pattern[ids:idx]
    md = pattern[idx+1:]
    arg = []
    arg.append(ptr)
    if m:
        arg.append(m)
    if md:
        for i in md:
            if i in mode:
                arg.append(getattr(re, mode[i]))
    return getattr(re,'compile')(*arg)

def find_str(string, sub_str, find_cnt):
    """
    查找字符串
    """
    str_list = string.split(sub_str,find_cnt)
    if len(str_list) <= find_cnt:
        return -1
    return len(string)-len(str_list[-1])-len(sub_str)

class __redirection__:

    def __init__(self):
        self.buff=''
        self.__console__=sys.stdout

    def write(self, output_stream):
        self.buff += output_stream

    def to_console(self):
        sys.stdout=self.__console__
        print(self.buff)

    def to_file(self, file_path):
        f=open(file_path,'w')
        sys.stdout=f
        f.close()

    def flush(self):
        self.buff=''

    def read(self):
        return self.buff

    def reset(self):
        sys.stdout=self.__console__
