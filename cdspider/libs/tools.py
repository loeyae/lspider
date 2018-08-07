#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import sys
import imp
import inspect
import linecache
import six
import click
import json
import logging
import redis

from cdspider.exceptions import *
from cdspider.libs import utils

BROKEN_EXCEPTIONS = {
    'base': CDSpiderCrawlerBroken,
    'exists': CDSpiderCrawlerNoExists,
    'login': CDSpiderCrawlerDoLogin,
    'source': CDSpiderCrawlerNoSource,
    'page': CDSpiderCrawlerNoNextPage,
    'max': CDSpiderCrawlerMoreThanMaximum,
}

TIMEOUT_EXCEPTIONS = {
    'base': CDSpiderCrawlerTimeout,
    'connect': CDSpiderCrawlerConnectTimeout,
    'read': CDSpiderCrawlerReadTimeout,
}

PROXY_EXCEPTIONS = {
    'base': CDSpiderCrawlerProxyError,
    'expired': CDSpiderCrawlerProxyExpired,
}


class Redis():
    """
    redis连接库
    """
    def __init__(self, *args, **kwargs):
        if len(args) > 0:
            kwargs['host'] = args[0]
        if len(args) > 1:
            kwargs['port'] = args[1]
        if len(args) > 2:
            kwargs['pass'] = args[2]
        if len(args) > 3:
            kwargs['db'] = args[3]

        assert 'host' in kwargs and kwargs['host'], "invalid host"
        kwargs.setdefault('port', 6379)
        kwargs.setdefault('pass', None)
        kwargs.setdefault('db', 0)
        self.setting = kwargs
        self.connect()

    def connect(self):
        if not 'pool' in self.setting:
            self.setting['pool'] = redis.ConnectionPool(host=self.setting['host'], port=self.setting['port'], db=self.setting['db'], password=self.setting['pass'])
        self._conn = redis.Redis(connection_pool=self.setting['pool'])

    def __getattr__(self, name):
        try:
            return getattr(self._conn, name)
        except Exception as e:
            raise CDSpiderRedisError(e)

def read_config(ctx, param, value):
    """
    load配置文件
    """
    if not value:
        return {}

    def underline_dict(d):
        if not isinstance(d, dict):
            return d
        return dict((k.replace('-', '_'), underline_dict(v)) for k, v in six.iteritems(d))

    config = underline_dict(json.load(value))
    ctx.default_map = config
    return config

def load_crawler(ctx, param, value):
    """
    加载crawler
    """
    if isinstance(value, six.string_types):
        log_level = logging.WARN
        if ctx.obj.get("debug", False):
            log_level = logging.DEBUG
        return utils.load_crawler(value, log_level = log_level)
    return value

def load_parser(ctx, param, value):
    """
    加载parser
    """
    if isinstance(value, six.string_types):
        log_level = logging.WARN
        if ctx.obj.get("debug", False):
            log_level = logging.DEBUG
        return utils.load_parser(value, log_level = log_level)
    return value

def connect_message_queue(ctx, param, value):
    """
    连接queue
    """
    if not value:
        return
    protocol = value.get('protocol', 'amqp')
    prefix = value.get('queue_prefix', '')
    if isinstance(param, click.core.Option):
        name = value.get('name', param.name)
    else:
        name =  value.get('name', param)
    if prefix:
        name = "%s_%s" % (prefix, name)
    host = value.get('host', 'localhost')
    port = value.get('port', 5672)
    user = value.get('user', 'guest')
    password = value.get('password', 'guest')
    maxsize = value.get('maxsize', ctx.params.get('queue_maxsize', 100))
    lazy_limit = value.get('lazy_limit', True)
    log_level = logging.WARN
    if ctx.params.get("debug", ctx.obj.get('debug') if ctx.obj else False):
        log_level = logging.DEBUG
    qo = utils.load_queue(protocol, name = name, host = host, port = port,
        user = user, password = password, maxsize = maxsize,
        lazy_limit = lazy_limit, log_level = log_level)
    return qo

def connect_db(ctx, param, value):
    """
    连接数据库
    """
    if not value:
        return
    protocol = value.get('protocol', 'sqlite')
    host = value.get('host', 'localhost')
    port = value.get('port', 0)
    db = value.get('db')
    user = value.get('user')
    password = value.get('password')
    log_level = logging.WARN
    if ctx.params.get("debug", ctx.obj.get('debug') if ctx.obj else False):
        log_level = logging.DEBUG
    logger = logging.getLogger("db")
    logger.setLevel(log_level)
    kwargs = {}
    if protocol == 'hive':
        config = ctx.params.get('config').get('redis')
        kwargs['redis'] = Redis(**config)
    connector =  utils.load_db(protocol, host = host, port = port, db = db, user = user,
        password = password, logger=logger, log_level = log_level, **kwargs)
    return connector

def connect_rpc(ctx, param, value):
    if not value:
        return
    from xmlrpc import client as xmlrpc_client
    return xmlrpc_client.ServerProxy(value, allow_none=True)

class ModulerLoader(object):
    """
    组件
    """

    def __init__(self, moduler, mod=None):
        self.moduler = moduler
        self.name = moduler['name']
        self.mod = mod

    def load_module(self):
        if self.mod is None:
            self.mod = mod = imp.new_module(self.name)
        else:
            mod = self.mod
        mod.__file__ = '<%s>' % self.name
        mod.__loader__ = self
        mod.__moduler__ = self.moduler
        mod.__package__ = 'cdspider'
        code = self.get_code()
        six.exec_(code, mod.__dict__)
        linecache.clearcache()
        if sys.version_info[:1] == (3,):
            sys.modules[self.name] = mod
        return mod

    def is_package(self):
        return False

    def get_code(self):
        return compile(self.get_source(), '<%s>' % self.name, 'exec')

    def get_source(self):
        script = self.moduler['scripts']
        if isinstance(script, six.text_type):
            return script.encode('utf8')
        return script

class ProjectLoader(ModulerLoader):
    """
    项目
    """
    def __init__(self, project, mod=None):
        super(ProjectLoader, self).__init__(project, mod)
        self.name = 'cdspider.handler.custom.%s' % project['name']

class TaskLoader(ModulerLoader):
    """
    任务
    """
    def __init__(self, task, mod=None):
        task['name'] = task['project']['name']
        super(TaskLoader, self).__init__(task, mod)
        self.name = 'cdspider.handler.custom.%s' % task['name']

def load_handler(task, **kwargs):
    """
    动态加载handler
    如果task中有定义，则使用task中的handler。如果project也有定义，task中定义的handler需继承自project中定义的handler
    如果project中有定义，则使用project中的handler
    否则，根据项目类型，使用默认的handler
    """
    from cdspider.handler import BaseHandler, AttachHandler, GeneralHandler, ProjectBaseHandler, SearchHandler
    mod = None
    project = task.get("project", None)
    site = task.get("site", None)
    urls = task.get("urls", None)
    if 'pid' in project and project['pid']:
        project['name'] = 'Project%s' % project['pid']
    if project and "scripts" in project and project['scripts']:
        mod = ProjectLoader(project).load_module()
    if site and "scripts" in site and site['scripts']:
        site['project'] = {"name": project['name']}
        mod = TaskLoader(site, mod).load_module()
    if urls and "scripts" in urls and urls['scripts']:
        urls['project'] = {"name": project['name']}
        mod = TaskLoader(urls, mod).load_module()
    if mod:
        _class_list = []
        for each in list(six.itervalues(mod.__dict__)):
            if inspect.isclass(each) and each is not BaseHandler \
                            and each is not ProjectBaseHandler and each is not AttachHandler \
                            and each is not GeneralHandler and each is not SearchHandler \
                            and issubclass(each, BaseHandler):
                _class_list.append(each)
        l = len(_class_list)
        logging.info("matched handler: %s" % _class_list)
        if l > 0:
            _class = None
            for each in _class_list:
                if not _class:
                    _class = each
                else:
                    if issubclass(each, _class):
                        _class = each
            logging.info("selected handler: %s" % _class)
            if _class:
                return _class(task = task, **kwargs)
    raise CDSpiderHandlerError("HandlerLoader no handler selected")

def load_cls(ctx, param, value):
    if isinstance(value, six.string_types):
        return utils.get_object(value)
    return value
