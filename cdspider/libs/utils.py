#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import sys
import os
import re
import logging
import traceback
import base64
import json
import hashlib
import datetime
import tldextract
import subprocess
import random
import types
import string
import six
import stevedore
import datetime
from threading import Thread
from multiprocessing import Process
from chardet.universaldetector import UniversalDetector
from urllib import parse
from types import *
from lxml import etree as le

def url_is_from_any_domain(url, domains):
    """Return True if the url belongs to any of the given domains"""
    host = parse.urlparse(url).netloc.lower()
    if not host:
        return False
    domains = [d.lower() for d in domains]
    return any((host == d) or (host.endswith('.%s' % d)) for d in domains)

def format_(data, params):
    keylist = re.findall('\{(\w+)\}', url)
    format_params = {}
    for key in keylist:
        if key in params:
            format_params[key] = params[key]
        else:
            format_params[key] = '{%s}' % key
    return data.format(**format_params)

def build_url_by_rule(rule, params):
    url = rule.get('base')
    if not params:
        return url
    mode = rule.get('mode', 'get')
    if mode == 'format':
        keylist = re.findall('\{(\w+)\}', url)
        format_params = {}
        for key in keylist:
            if key in params:
                if isinstance(params[key], bytes):
                    format_params[key] = parse.quote_plus(params[key])
                elif isinstance(params[key], str):
                    format_params[key] = parse.quote_plus(params[key].encode())
                else:
                    format_params[key] = params[key]
            else:
                format_params[key] = '{%s}' % key
        return url.format(**format_params)
    elif mode == 'replace':
        for key, value in params.items():
            replace, subject = rule2subitem(str(key), str(value))
            url = re.sub(replace, subject, url)
    elif mode == 'get':
        return build_query(url, params)
    return url

def parse_domain(url):
    try:
        extracted = tldextract.extract(url)
        return extracted.subdomain, "%s.%s" % (extracted.domain, extracted.suffix)
    except:
        return None, None

def domain_info(url):
    subdomain, domain = parse_domain(url)
    if not subdomain:
        subdomain = 'www'
    return "%s.%s" % (subdomain, domain), domain

def typeinfo(url):
    subdomain, domain = domain_info(url)
    return {"domain": domain, "subdomain": subdomain}

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
    return re.sub('((?:\r\n\s*)+|(?:\r\s*)+|(?:\n\s*)+|(?:<style>).+?(?:</style>))', '\r\n', content)

def decode(data, errors="ignore"):
    if isinstance(data, bytes):
        detector = UniversalDetector()
        hlist = filter(re.findall(b'[\x7f-\xf7]+', data))
        line = ''
        for item in hlist:
            if len(item) > len(line):
                line = item
            if len(line) > 100:
                break
        if line:
            detector.feed(line)
        detector.close()
        u = detector.result['encoding']
        if not u or u in ('ascii', 'ISO-8859-1', 'latin-1'):
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
    if isinstance(data, (list, tuple)):
        if patch:
            return [re.sub('\[\w+\]', str(callback_result(callback, d)), patch) if not isinstance(d, (list, dict)) and d else d for d in data]
        return ["%s%s%s" %(prefix, callback_result(callback, d), suffix) if not isinstance(d, (list, dict)) and d else d for d in data]
    elif isinstance(data, dict):
        if patch:
            return dict((k, re.sub('\[\w+\]', str(callback_result(callback, d)), patch)) if not isinstance(d, (list, dict)) and d else d for k,d in data.items())
        return dict((k, "%s%s%s" %(prefix, callback_result(callback, d), suffix)) if not isinstance(d, (list, dict)) else d and d for k,d in data.items())
    else:
        rst = callback_result(callback, data)
        if patch:
            return re.sub('\[\w+\]', str(rst), patch) if not isinstance(rst, (list, dict)) and rst else rst
        return "%s%s%s" %(prefix, rst, suffix) if not isinstance(rst, (list, dict)) and rst else rst

def preg(data, rule):
    pattern, key = rule2pattern(rule)
    r = re.search(pattern, str(data), re.S|re.I)
    if r:
        matched = r.group(key)
        if matched:
            matched = matched.strip()
        return matched
    return None

def extract_result(data, rule, callback=None):

    extract = rule.get('extract', None)
    if not extract:
        return data
    if isinstance(data, (list, tuple)):
        return [preg(callback_result(callback, d), extract) if not isinstance(d, (list, dict)) and d else d for d in data]
    elif isinstance(data, dict):
        return dict((k, preg(callback_result(callback, d), extract)) if not isinstance(d, (list, dict)) and d else d for k,d in data.items())
    else:
        return preg(callback_result(callback, data), extract)

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

def quertstr2dict(value):
    if not value:
        return {}
    d = [parse.unquote(item).split('=') for item in value.split("&")]
    return dict([(i[0], i[1]) for i in d])

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

    t = sorted(g_encode_params.items(), key=lambda item: item[0])
    query_str = []
    for item in t:
        query_str.append("%s=%s" % (item[0], parse.quote_plus(item[1])))
    return "&". join(query_str)

def build_query(url, query):
    if not query:
        return url
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

def build_filter_query(url, query = [], exclude = []):
    arr = parse.urlparse(url)
    if arr.query:
        query1 = parse.parse_qs(arr.query)
    else:
        query1 = {}
    query2 = {}
    if query:
        for i,v in query1.items():
            if i in query and v:
                if isinstance(v, (list, tuple)):
                   query2[i] = v[0] or ""
                else:
                    query2[i] = str(v)  or ""
    elif exclude:
        for i,v in query1.items():
            if not i in exclude and v:
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

def quote_chinese(url, encodeing="utf-8"):
    """Quote non-ascii characters"""
    if isinstance(url, six.text_type):
        return quote_chinese(url.encode(encodeing))
    if six.PY3:
        res = [six.int2byte(b).decode('latin-1') if b < 128 else '%%%02X' % b for b in url]
    else:
        res = [b if ord(b) < 128 else '%%%02X' % ord(b) for b in url]
    return "".join(res)

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

def dictjoin(dict1, dict2, replace = False):
    """
    返回出现在dict1或dict2中key对应的字典，如果dict1和dict2中都存在该key，并且都不为空，则保留dict1中的值
    """
    for key in dict1:
        val = dict2.pop(key, None)
        if replace:
            if val != None and val != '':
                dict1[key] = val
        else:
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
    vlen = [len(item) if isinstance(item, (list, tuple)) else 0 for item in values]
    vl = max(vlen)
    d = []
    l = len(keys)
    if l > 0:
        for j in range(vl):
            item = []
            for i in range(l):
                v = None
                if values[i] and isinstance(values[i], (list, tuple)) and len(values[i]) > j:
                    v = values[i][j]
                item.append(v)
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

class xml_tool(object):

    def __init__(self, x):
        if os.path.isfile(x):
            self.root = le.parse(x).getroot()
        else:
            if isinstance(x, bytes):
                self.root = le.fromstring(x)
            else:
                self.root = le.fromstring(x.encode())

    def css_select(self, selector):
        return self.root.cssselect(selector)

    def get_elements_by_tags(self, tag):
        return self.css_select(tag)

    def add_children(self, element, parent = None):
        if parent is None:
            parent = self.root
        if isinstance(parent, list):
            parent = parent[0]
        parent.append(element)

    def remove(self, node):
        parent = node.getparent()
        if parent is not None:
            if node.tail:
                prev = node.getprevious()
                if prev is None:
                    if not parent.text:
                        parent.text = ''
                    parent.text += ' ' + node.tail
                else:
                    if not prev.tail:
                        prev.tail = ''
                    prev.tail += ' ' + node.tail
            node.clear()
            parent.remove(node)

    def create_element(self, tag, text = None, tail = '\n'):
        e = le.Element(tag)
        if text:
            e.text =text
        e.tail = tail
        return e

    def get_element(self, expression, index = 0, mode = 'xpath'):
        if mode == 'css':
            items = self.css_select(expression)
        else:
            items = self.xpath(expression)
        if items:
            return items[index]
        raise Exception('not element found')

    def xpath_re(self, expression):
        regexp_namespace = "http://exslt.org/regular-expressions"
        items = self.root.xpath(expression, namespaces={'re': regexp_namespace})
        return items

    def xpath(self, expression):
        items = self.root.xpath(expression)
        return items

    def get_elements_by_tag(self, tag=None, attr=None, value=None, childs=False):
        selector = 'descendant-or-self::%s' % (tag or '*')
        if attr and value:
            selector = '%s[re:test(@%s, "%s", "i")]' % (selector, attr, value)
        elems = self.xpath_re(selector)
        if self.root in elems and (tag or childs):
            elems.remove(self.root)
        return elems

    def to_string(self):
        return le.tostring(self.root)

    def save_file(self, filename):
        return le.ElementTree(self.root).write(filename, pretty_print=True, xml_declaration=True, encoding='utf-8')

def array2rule(rule, final_url):
    def build_rule(item, final_url):
        key = item.pop('key')
        if key and item['filter']:
            if item['filter'] == '@value:parent_url':
                '''
                规则为获取父级url时，将详情页url赋给规则
                '''
                item['filter'] = '@value:%s' % final_url
            return {key: item}
        return None
    #格式化解析规则
    parse = {}
    if isinstance(rule, (list, tuple)):
        for item in rule:
            ret = build_rule(item, final_url)
            if ret:
                parse.update(ret)
    elif isinstance(rule, dict):
        for item in rule.values():
            ret = build_rule(item, final_url)
            if ret:
                parse.update(ret)
    return parse

def rule2parse(parser_cls, source, final_url, rule, log_level):
    """
    根据规则解析出结果
    """
    if not rule:
        return {}
    parser = parser_cls(source=source, ruleset=rule, log_level=log_level, url=final_url)
    parsed = parser.parse()
    return filter(parsed)

def attach_preparse(parser_cls, source, final_url, rule, log_level):
    """
    附加任务url生成规则参数获取
    """
    parsed = rule2parse(parser_cls, source, final_url, rule, log_level)
    if parsed.keys() != rule.keys():
        '''
        数据未完全解析到，则任务匹配失败
        '''
        return False
    return parsed

def build_attach_url(parser_cls, source, final_url, rule, log_level):
        """
        根据规则构造附加任务url
        :param rule 附加任务url生成规则
        """
        if 'preparse' in rule and rule['preparse']:
            #根据解析规则匹配解析内容
            parse = rule['preparse'].get('parse', None)
            parsed = {}
            if parse:
                _rule = array2rule(parse, final_url)
                parsed = attach_preparse(parser_cls, source, final_url, _rule, log_level)
                if parsed == False:
                    return (None, None)
            hard_code = []
            params = {}
            for k, r in parsed.items():
                if 'mode' in _rule[k]:
                    hard_code.append({"mode": rule[k]['mode'], "name": k, "value": r})
                else:
                    params[k] = r
            urlrule = rule['preparse'].get('url', {})
            if urlrule:
                #格式化url设置，将parent_rul替换为详情页url
                if urlrule['base'] == 'parent_url':
                    urlrule['base'] = final_url
            return (build_url_by_rule(urlrule, params), hard_code)
        return (None, None)
