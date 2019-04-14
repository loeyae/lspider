# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import sys
import copy
import importlib.util
import linecache
import six
import click
import json
import logging
import redis
import collections

from cdspider.exceptions import *
from cdspider.libs import utils


class Redis():
    """
    redis连接库
    """
    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            kwargs['url'] = args[0]
        elif len(args) > 1:
            kwargs['host'] = args[0]
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
        if 'pool' not in self.setting:
            if 'url' in self.setting:
                self.setting['pool'] = redis.ConnectionPool.from_url(url=self.setting['url'], db=self.setting['db'])
            else:
                self.setting['pool'] = redis.ConnectionPool(host=self.setting['host'], port=self.setting['port'], db=self.setting['db'], password=self.setting['pass'])
        self._conn = redis.Redis(connection_pool=self.setting['pool'])

    def __getattr__(self, name):
        try:
            return getattr(self._conn, name)
        except Exception as e:
            raise CDSpiderRedisError(str(e))


class db_wrapper(collections.UserDict):
    """
    db wrapper
    """
    connector = None
    protocol = None

    def __init__(self, connector, protocol, log_level = logging.WARN):
        self.connector = connector
        self.protocol = protocol
        self.log_level = log_level
        super(db_wrapper, self).__init__()

    def __getitem__(self, key):
        if key in self.data:
            return super(db_wrapper, self).__getitem__(key)
        try:
            cls = utils.load_dao(self.protocol, key, connector=self.connector, log_level=self.log_level)
            self.data[key] = cls
            return cls
        except (ImportError, AttributeError):
            cls = utils.load_dao(self.protocol, 'Base', connector=self.connector, log_level=self.log_level)
            self.data[key] = cls
            return cls


class queue_wrapper(collections.UserDict):
    """
    db wrapper
    """
    context = None
    queue_setting = None

    def __init__(self, context, queue_setting):
        self.context = context
        self.queue_setting = queue_setting
        super(queue_wrapper, self).__init__()

    def __getitem__(self, key):
        if key in self.data:
            return super(queue_wrapper, self).__getitem__(key)
        queue_setting = copy.deepcopy(self.queue_setting)
        cls = connect_message_queue(self.context, key, queue_setting)
        self.data[key] = cls
        return cls

def underline_dict(d):
    if not isinstance(d, dict):
        return d
    return dict((k.replace('-', '_'), underline_dict(v)) for k, v in six.iteritems(d))

def read_config(ctx, param, value):
    """
    load配置文件
    """
    if not value:
        return {}

    config = underline_dict(json.load(value))
    ctx.default_map = config
    return config

def load_config(ctx, param, value):
    if value is None:
        return None
    if not value:
        return {}
    return underline_dict(json.load(value))

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
    if isinstance(value, six.string_types):
        from xmlrpc import client as xmlrpc_client
        return xmlrpc_client.ServerProxy(value, allow_none=True)
    return value


class ModulerLoader(object):
    """
    组件
    """

    def __init__(self, moduler, mod=None):
        self.moduler = moduler
        self.name = moduler['name']
        self.mod = mod

    def load_module(self, handler, handler_params):
        if self.mod is None:
            self.mod = mod = importlib.util.module_from_spec(self.name)
        else:
            mod = self.mod
        mod.__file__ = '<%s>' % self.name
        mod.__loader__ = self
        mod.__moduler__ = self.moduler
        mod.__package__ = 'cdspider'
        mod.handler = handler
        mod.handler_params = handler_params
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

def load_cls(ctx, param, value):
    if isinstance(value, six.string_types):
        return utils.get_object(value)
    return value
