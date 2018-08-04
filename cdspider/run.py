#! /usr/bin/python
#-*- coding: UTF-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: run.py 2299 2018-07-06 08:30:54Z zhangyi $

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2018-1-9 18:04:41$"

import sys
import os
import traceback
import click
import logging.config
import six
from cdspider.libs import utils
from cdspider.libs.tools import *
from cdspider.database.base import *

cpath = os.path.dirname(__file__)

@click.group(invoke_without_command=True)
@click.option('-c', '--config', default=os.path.join(cpath, "config", "main.json"),
              callback=read_config, type=click.File(mode='r', encoding='utf-8'),
              help='json配置文件. {"webui": {"port":5001}}', show_default=True)
@click.option('--logging-config', default=os.path.join(cpath, "config", "logging.conf"),
              help="日志配置文件", show_default=True)
@click.option('--debug', default=False, is_flag=True, help='debug模式', show_default=True)
@click.option('--projectdb', callback=connect_db, help='project数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: project}')
@click.option('--taskdb', callback=connect_db, help='task数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: task}')
@click.option('--sitedb', callback=connect_db, help='site数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: site}')
@click.option('--sitetypedb', callback=connect_db, help='site type数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: sitetype}')
@click.option('--urlsdb', callback=connect_db, help='url数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: url}')
@click.option('--attachmentdb', callback=connect_db, help='attachment数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: attachment}')
@click.option('--keywordsdb', callback=connect_db, help='keywordds数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: keywordds}')
@click.option('--uniquedb', callback=connect_db, help='unique数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: unique}')
@click.option('--resultdb', callback=connect_db, help='result数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: result}')
@click.option('--custom-db', help='自定义数据库设置, default: '
              'custom1: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: custom}')
@click.option('--proxy', default=None, help='代理设置', show_default=True)
@click.option('--queue-maxsize', default=1000, help='queue最大阈值', show_default=True)
@click.option('--queue-prefix', default=None, help='queue的前缀', show_default=True)
@click.option('--message-queue', help='queue设置, default: '
              'message_queue: {protocol: amqp, host: host, port: 5672, user: guest, password: guest}')
@click.option('--add-sys-path/--not-add-sys-path', default=True, is_flag=True,
              help='增加当前文件夹路径到系统路径', show_default=True)
@click.option('--testing-mode', default=False, is_flag=True, help='debug mode', show_default=True)
@click.pass_context
def cli(ctx, **kwargs):
    if kwargs['add_sys_path']:
        sys.path.append(os.getcwd())
    logging.config.fileConfig(kwargs['logging_config'])
    kwargs['logger'] = logging.getLogger("root")
    if kwargs['debug']:
        kwargs['logger'].setLevel(logging.DEBUG)

    if kwargs.get("custom_db"):
        kwargs['custom_db'] = {}
        for n,v in kwargs.get("custom_db").items():
            kwargs['custom_db'][n] = connect_db(ctx, n, v)

    app_config = utils.load_config(os.path.join(cpath, "config", "app.json"))

    queue_setting = kwargs.get("message_queue")
    if queue_setting:
        queue_setting.setdefault('maxsize', kwargs.get('queue_maxsize'))
        queue_setting.setdefault('queue_prefix', kwargs.get('queue_prefix', ''))
        for q in app_config.get("queues", {}):
            _maxsize = queue_setting['maxsize']
            if q == 'spider2scheduler' or q == 'excinfo_queue':
                queue_setting['maxsize'] = 0
            kwargs[q] = connect_message_queue(ctx, q, queue_setting)
            if _maxsize != queue_setting['maxsize']:
                queue_setting['maxsize'] = _maxsize

    ctx.obj = {}
    ctx.obj.update(kwargs)
    ctx.obj['app_config'] = app_config
    ctx.obj.setdefault('rate_map', ctx.obj['app_config'].get('ratemap', {}))
    ctx.obj['instances'] = []
    if ctx.invoked_subcommand is None and not ctx.obj.get('testing_mode'):
        ctx.invoke(all)
    return ctx

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.Scheduler', callback=load_cls, help='scheduler name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.option('--xmlrpc/--no-xmlrpc', default=False)
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=23333, help="xmlrpc bind port")
@click.pass_context
def schedule(ctx, scheduler_cls, no_loop, xmlrpc, xmlrpc_host, xmlrpc_port, get_object=False):
    """
    Schedule: 根据project的设置生成任务
    """
    g = ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    newtask_queue = g.get('newtask_queue')
    inqueue = g.get('spider2scheduler')
    outqueue = g.get('scheduler2spider')
    status_queue = g.get('status_queue')
    search_work = g.get('search_work')
    projectdb = g.get('projectdb')
    taskdb = g.get('taskdb')
    sitedb = g.get('sitedb')
    urlsdb = g.get('urlsdb', None)
    attachmentdb = g.get('attachmentdb', None)
    keywordsdb = g.get('keywordsdb', None)
    customdb = g.get('customdb', None)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    scheduler = Scheduler(newtask_queue=newtask_queue, inqueue=inqueue, outqueue=outqueue, status_queue=status_queue, search_work=search_work, projectdb=projectdb, taskdb=taskdb, sitedb=sitedb, urlsdb=urlsdb, keywordsdb=keywordsdb, attachmentdb=attachmentdb, customdb=customdb, rate_map=rate_map, log_level=log_level)
    g['instances'].append(scheduler)
    if g.get('testing_mode') or get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        if xmlrpc:
            utils.run_in_thread(scheduler.xmlrpc_run, port=xmlrpc_port, bind=xmlrpc_host)
        scheduler.run()

@cli.command()
@click.option('--fetch-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.option('--xmlrpc/--no-xmlrpc', default=False)
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=24444, help="xmlrpc bind port")
@click.pass_context
def fetch(ctx, fetch_cls, no_loop, xmlrpc, xmlrpc_host, xmlrpc_port, get_object=False, no_input=False):
    """
    Fetch: 监听任务并执行抓取
    """
    g = ctx.obj
    Spider = load_cls(ctx, None, fetch_cls)
    if not no_input:
        inqueue = g.get("scheduler2spider")
        outqueue = g.get("spider2result")
        status_queue = g.get("status_queue")
        requeue = g.get("spider2scheduler")
        excqueue = g.get("excinfo_queue")
        resultdb = g.get('resultdb')
        taskdb = g.get('taskdb')
    else:
        inqueue = None
        outqueue = None
        status_queue = None
        requeue = None
        excqueue = None
        resultdb = None
        taskdb = None
    projectdb = g.get('projectdb')
    sitetypedb = g.get('sitetypedb', None)
    sitedb = g.get('sitedb')
    customdb = g.get('customdb', None)
    uniquedb = g.get('uniquedb')
    urlsdb = g.get('urlsdb', None)
    attachmentdb = g.get('attachmentdb', None)
    keywordsdb = g.get('keywordsdb', None)
    handler = None
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    attach_storage = g.get('app_config', {}).get('attach_storage', None)
    if attach_storage:
        attach_storage = os.path.realpath(os.path.join(cpath, attach_storage))
    if g.get("debug", False):
        log_level = logging.DEBUG

    spider = Spider(inqueue=inqueue, outqueue=outqueue, status_queue=status_queue, requeue=requeue,
            excqueue=excqueue, projectdb=projectdb, sitetypedb=sitetypedb, taskdb=taskdb, sitedb=sitedb,
            resultdb=resultdb, customdb=customdb, uniquedb=uniquedb, urlsdb=urlsdb, keywordsdb=keywordsdb,
            attachmentdb=attachmentdb, handler=handler, proxy=proxy, log_level=log_level, attach_storage = attach_storage)
    g['instances'].append(spider)
    if g.get('testing_mode') or get_object:
        return spider
    if no_loop:
        spider.run_once()
    else:
        if xmlrpc:
            utils.run_in_thread(spider.xmlrpc_run, port=xmlrpc_port, bind=xmlrpc_host)
        spider.run()

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.ResultWorker', callback=load_cls, help='worker name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def result_work(ctx, worker_cls, no_loop, get_object=False):
    """
    Result Wroker: 抓取结果处理
    """
    g = ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    inqueue = g.get('spider2result')
    outqueue = g.get('spider2scheduler')
    excqueue = g.get('excinfo_queue')
    resultdb = g.get('resultdb')
    projectdb = g.get('projectdb')
    sitetypedb = g.get('sitetypedb')
    sitedb = g.get('sitedb')
    customdb = g.get('customdb', None)
    uniquedb = g.get('uniquedb', None)
    urlsdb = g.get('urlsdb', None)
    attachmentdb = g.get('attachmentdb', None)
    keywordsdb = g.get('keywordsdb', None)
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    worker = Worker(inqueue=inqueue, outqueue=outqueue, excqueue=excqueue, projectdb=projectdb,
            sitedb=sitedb, uniquedb=uniquedb, urlsdb=urlsdb, keywordsdb=keywordsdb, customdb=customdb,
            attachmentdb=attachmentdb, resultdb=resultdb, sitetypedb=sitetypedb, proxy=proxy, log_level=log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.SearchWorker', callback=load_cls, help='worker name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def search_work(ctx, worker_cls, no_loop, get_object=False):
    """
    Result Wroker: 抓取结果处理
    """
    g = ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    inqueue = g.get('search_work')
    outqueue = g.get('newtask_queue')
    excqueue = g.get('excinfo_queue')
    resultdb = g.get('resultdb')
    projectdb = g.get('projectdb')
    sitetypedb = g.get('sitetypedb')
    sitedb = g.get('sitedb')
    customdb = g.get('customdb', None)
    uniquedb = g.get('uniquedb', None)
    urlsdb = g.get('urlsdb', None)
    attachmentdb = g.get('attachmentdb', None)
    keywordsdb = g.get('keywordsdb', None)
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    worker = Worker(inqueue=inqueue, outqueue=outqueue, excqueue=excqueue, projectdb=projectdb,
            sitedb=sitedb, uniquedb=uniquedb, urlsdb=urlsdb, keywordsdb=keywordsdb, customdb=customdb,
            attachmentdb=attachmentdb, resultdb=resultdb, sitetypedb=sitetypedb, proxy=proxy, log_level=log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.ExcWorker', callback=load_cls, help='worker name')
@click.option('--mailer', default=None, help='mailer name', show_default=True)
@click.option('--sender', default=None, show_default=True, help='发件人人设置'
              ' default: {host: host, port: 27017, user: guest, password: guest, from: admin@localhost.com}')
@click.option('--receiver', default=None, show_default=True, help='收件人设置'
              ' default: [{name: name, mail: a@b.com},...]')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def exc_work(ctx, worker_cls, mailer, sender, receiver, no_loop, get_object=False):
    """
    Error Wroker: 抓取异常处理
    """
    g = ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    inqueue = g.get('excinfo_queue')
    outqueue = g.get('spider2scheduler')
    excqueue = None
    resultdb = g.get('resultdb')
    projectdb = g.get('projectdb')
    sitetypedb = g.get('sitetypedb')
    sitedb = g.get('sitedb')
    customdb = g.get('customdb', None)
    uniquedb = g.get('uniquedb', None)
    urlsdb = g.get('urlsdb', None)
    attachmentdb = g.get('attachmentdb', None)
    keywordsdb = g.get('keywordsdb', None)
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    if mailer:
        mailer = utils.load_mailer(mailer, sender=sender, receiver=receiver)
    worker = Worker(inqueue=inqueue, outqueue=outqueue, excqueue=excqueue, projectdb=projectdb,
            sitedb=sitedb, uniquedb=uniquedb, urlsdb=urlsdb, keywordsdb=keywordsdb, customdb=customdb,
            attachmentdb=attachmentdb, resultdb=resultdb, sitetypedb=sitetypedb, proxy=proxy, mailer=mailer,
            log_level=log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('--webui-instance', default='cdspider.webui.app.app', callback=load_cls,
              help='webui Flask Application instance to be used.')
@click.option('--admindb', callback=connect_db, help='admin数据库设置,'
              ' default: {protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: admin}')
@click.option('--host', default='0.0.0.0', help='webui bind to host')
@click.option('--port', default=5000, help='webui bind to host')
@click.option('--scheduler-rpc', default=None, help='schedule rpc server')
@click.option('--spider-rpc', default=None, help='spider rpc server')
@click.option('--need-auth', is_flag=True, default=False, help='need username and password')
@click.option('--username', help='username of lock -ed projects')
@click.option('--password', help='password of lock -ed projects')
@click.option('--process-time-limit', default=30, help='script process time limit in debug')
@click.pass_context
def web(ctx, webui_instance, admindb, host, port, scheduler_rpc, spider_rpc, need_auth,
        username, password, process_time_limit, get_object=False):
    """
    爬虫可视化管理系统
    """
    app = load_cls(None, None, webui_instance)
    g = ctx.obj
    if username:
        app.config['webui_username'] = username
    if password:
        app.config['webui_password'] = password
    app.config['need_auth'] = need_auth
    app.config['process_time_limit'] = process_time_limit
    app.config['admindb'] = admindb
    app.config['rate_map'] = g.get('rate_map', {})
    app.config['app_config'] = g.get('app_config', {})
    app.config['taskdb'] = g.get("taskdb", None)
    app.config['projectdb'] = g.get("projectdb", None)
    app.config['resultdb'] = g.get("resultdb", None)
    app.config['sitetypedb'] = g.get("sitetypedb", None)
    app.config['sitedb'] = g.get("sitedb", None)
    app.config['urlsdb'] = g.get("urlsdb", None)
    app.config['attachmentdb'] = g.get("attachmentdb", None)
    app.config['keywordsdb'] = g.get("keywordsdb", None)
    app.config['customdb'] = g.get("customdb", None)

    if 'ratemap' in app.config['app_config'] and app.config['app_config']['ratemap']:
        app.config['app_config']['ratemap_sorted'] = sorted(app.config['app_config']['ratemap'].items(), key=lambda d: int(d[0]))

    if isinstance(scheduler_rpc, six.string_types):
        scheduler_rpc = connect_rpc(ctx, None, scheduler_rpc)
        app.config['scheduler_rpc'] = scheduler_rpc
        app.config['newtask'] = lambda x: scheduler_rpc.newtask(x).data
        app.config['status'] = lambda x: scheduler_rpc.status(x).data
        app.config['search_work'] = lambda x: scheduler_rpc.search_work(x).data
    else:
        webui_scheduler = ctx.invoke(schedule, get_object=True)
        def newtask(x):
            return webui_scheduler.newtask(x)
        app.config['newtask'] = lambda x: newtask(x)
        def status(x):
            return webui_scheduler.status(x)
        app.config['status'] = lambda x: status(x)

        def search_work(x):
            return webui_scheduler.search_work(x)
        app.config['search_work'] = lambda x: search_work(x)

    if isinstance(spider_rpc, six.string_types):
        import umsgpack
        spider_rpc = connect_rpc(ctx, None, spider_rpc)
        app.config['spider_rpc'] = spider_rpc
        app.config['fetch'] = lambda x: umsgpack.unpackb(spider_rpc.fetch(x).data)
        app.config['task'] = lambda x: umsgpack.unpackb(spider_rpc.task(x).data)
    else:
        webui_fetcher = ctx.invoke(fetch, get_object=True, no_input=True)
        def gfetch(x):
            ret = webui_fetcher.fetch(x, True)
            if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                result, broken_exc, last_source, final_url, save = ret[0]
                last_source = utils.decode(last_source)
                result = (result, broken_exc, last_source, final_url, save)
                return result
            else:
                g['logger'].error(str(ret))
                return ret
        app.config['fetch'] = lambda x: gfetch(x)
        def gtask(x):
            message, task = x
            task = webui_fetcher.get_task(message, task, no_check_status = True)
            return task
        app.config['task'] = lambda x: gtask(x)

    app.config['queues'] = {}
    app.config['queue_setting'] = g.get('message_queue')
    for q in g.get('app_config', {}).get("queues", {}):
        app.config['queues'][q] = g.get(q, None)

    app.debug = g.get("debug", False)
    g['instances'].append(app)
    if g.get('testing_mode') or get_object:
        return app

    app.run(host=host, port=port)

@cli.command()
@click.option('--created', default=None, help='拉取数据的时间', show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def rebuild_result(ctx, created, no_loop):
    import time
    g = ctx.obj
    outqueue = g.get('spider2result')
    resultdb = g.get('resultdb')
    createtime = 0
    lastcreatetime = createtime
    if not created:
        created = int(time.time())
    while True:
        if lastcreatetime == createtime:
            createtime += 1
        else:
            lastcreatetime = createtime
        g['logger'].debug("current createtime: %s" % createtime)
        data = resultdb.get_list(created, where = [("status", resultdb.RESULT_STATUS_INIT), ("createtime", "$gte", createtime)], select={"rid": 1, "url": 1, "createtime": 1}, hits=100)
        data = list(data)
        g['logger'].debug("got result: %s" % str(data))
        i = 0
        for item in data:
            if item['url'].startswith('javascript'):
                resultdb.update(item['rid'], {"status": resultdb.RESULT_STATUS_DELETED})
                continue
            outqueue.put_nowait({"id": item['rid'], "task": 1})
            if item['createtime'] > createtime:
                createtime = item['createtime']
            i += 1
        if i == 0:
            g['logger'].info("no rebuid result")
        if no_loop:
            break
        time.sleep(0.5)


@cli.command()
@click.option('--fetch-num', default=1, help='fetch实例个数')
@click.option('--result-work-num', default=1, help='result worker实例个数')
@click.option('--exc-work-num', default=1, help='exc worker实例个数')
@click.option('--run-in', default='subprocess', type=click.Choice(['subprocess', 'thread']),
              help='运行模式:subprocess or thread')
@click.pass_context
def all(ctx, fetch_num, result_work_num, exc_work_num, run_in):
    """
    集成运行整个系统
    """
    ctx.obj['debug'] = False
    g = ctx.obj
    if run_in == 'subprocess' and os.name != 'nt':
        run_in = utils.run_in_subprocess
    else:
        run_in = utils.run_in_thread

    threads = []

    try:

        #result worker
        result_worker_config = g['config'].get('result_work', {})
        for i in range(result_work_num):
            threads.append(run_in(ctx.invoke, result_work, **result_worker_config))

        #exc worker
        exc_worker_config = g['config'].get('exc_work', {})
        for i in range(exc_work_num):
            threads.append(run_in(ctx.invoke, exc_work, **exc_worker_config))

        #fetch
        fetcher_config = g['config'].get('fetche', {})
#        fetcher_config.setdefault('xmlrpc_host', '127.0.0.1')
        for i in range(fetch_num):
            threads.append(run_in(ctx.invoke, fetch, **fetcher_config))

        #schedule
        scheduler_config = g['config'].get('schedule', {})
#        scheduler_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, schedule, **scheduler_config))

        #webui
        webui_config = g['config'].get('web', {})
#        webui_config.setdefault('scheduler_rpc', 'http://127.0.0.1:%s/'
#                                % g.config.get('scheduler', {}).get('xmlrpc_port', 23333))
        ctx.invoke(web, **webui_config)
    except:
        g['logger'].error(traceback.format_exc())
    finally:
        for each in g["instances"]:
            each.quit()

        for each in threads:
            if not each.is_alive():
                continue
            if hasattr(each, 'terminate'):
                each.terminate()
            each.join()

def get_wsgi_app(config=os.path.join(cpath, "config", "main.json"), logging_config=os.path.join(cpath, "config", "logging.conf"), app_config=os.path.join(cpath, "config", "app.json"), scheduler_rpc = None, spider_rpc = None):
    """
    get web app
    """
    app = load_cls(None, None, "cdspider.webui.app.app")
    config = utils.load_config(config)
    debug = config.get('debug', False)
    logging.config.fileConfig(logging_config)
    username = config.get('web', {}).get('username', None)
    password = config.get('web', {}).get('password', None)
    if username:
        app.config['webui_username'] = username
    if password:
        app.config['webui_password'] = password
    app.config['need_auth'] = config.get('web', {}).get('need_auth', False)
    app.config['process_time_limit'] =  config.get('web', {}).get('process_time_limit', 30)
    logger = logging.getLogger('root')
    log_level = logging.WARN if not debug else logging.DEBUG
    logger.setLevel(log_level)
    dblogger = logging.getLogger('db')

    def cnct_db(config, name, logger, log_level):
        db_config = config.get(name, {})
        return utils.load_db(db_config.get('protocol', 'mysql'), name = name, host = db_config.get('host', None), port = db_config.get('port'), db = db_config.get('db'), user = db_config.get('user'), password = db_config.get('password'), table = db_config.get('table'), logger=logger, log_level = log_level) if db_config else None

    admindb_config = config.get('web', {})
    app.config['admindb'] = cnct_db(admindb_config, 'admindb', dblogger, log_level)
    app_config = utils.load_config(app_config)
    app.config['rate_map'] = app_config.get('rate_map', {})
    app.config['app_config'] = app_config
    app.config['taskdb'] = cnct_db(config, 'taskdb', dblogger, log_level)
    app.config['projectdb'] = cnct_db(config, 'projectdb', dblogger, log_level)
    app.config['resultdb'] = cnct_db(config, 'resultdb', dblogger, log_level)
    app.config['sitetypedb'] = cnct_db(config, 'sitetypedb', dblogger, log_level)
    app.config['sitedb'] = cnct_db(config, 'sitedb', dblogger, log_level)
    app.config['urlsdb'] = cnct_db(config, 'urlsdb', dblogger, log_level)
    app.config['attachmentdb'] = cnct_db(config, 'attachmentdb', dblogger, log_level)
    app.config['keywordsdb'] = cnct_db(config, 'keywordsdb', dblogger, log_level)
    app.config['customdb'] = cnct_db(config, 'customdb', dblogger, log_level)

    if 'ratemap' in app.config['app_config'] and app.config['app_config']['ratemap']:
        app.config['app_config']['ratemap_sorted'] = sorted(app.config['app_config']['ratemap'].items(), key=lambda d: int(d[0]))

    if isinstance(spider_rpc, six.string_types):
        import umsgpack
        spider_rpc = connect_rpc(None, None, spider_rpc)
        app.config['spider_rpc'] = spider_rpc
        app.config['fetch'] = lambda x: umsgpack.unpackb(spider_rpc.fetch(x).data)
        app.config['task'] = lambda x: umsgpack.unpackb(spider_rpc.task(x).data)
    else:
        Spider = load_cls(None, None, "cdspider.spider.Spider")
        inqueue = None
        outqueue = None
        status_queue = None
        requeue = None
        excqueue = None
        resultdb = None
        taskdb = None
        projectdb = app.config.get('projectdb')
        sitetypedb = app.config.get('sitetypedb', None)
        sitedb = app.config.get('sitedb')
        customdb = app.config.get('customdb', None)
        uniquedb = app.config.get('uniquedb')
        attachmentdb = app.config.get('attachmentdb', None)
        urlsdb = app.config.get('urlsdb', None)
        keywordsdb = app.config.get('keywordsdb', None)
        handler = None
        proxy = config.get('proxy', None)

        webui_fetcher = Spider(inqueue=inqueue, outqueue=outqueue, status_queue=status_queue, requeue=requeue,
                excqueue=excqueue, projectdb=projectdb, sitetypedb=sitetypedb, taskdb=taskdb, sitedb=sitedb,
                resultdb=resultdb, customdb=customdb, uniquedb=uniquedb, urlsdb=urlsdb, keywordsdb=keywordsdb,
                attachmentdb=attachmentdb, handler=handler, proxy=proxy, log_level=log_level)
        def gfetch(x):
            import chardet
            ret = webui_fetcher.fetch(x, True)
            if ret and isinstance(ret, (list, tuple)) and isinstance(ret[0], (list, tuple)):
                result, broken_exc, last_source, final_url, save = ret[0]
                if isinstance(last_source, bytes):
                    encoding = chardet.detect(last_source)
                    u = encoding['encoding']
                    if u.upper() == "GB2312":
                        u = "GBK";
                    elif u == 'Windows-1254':
                        u = 'UTF-8'
                    last_source = last_source.decode(u)
                result = (result, broken_exc, last_source, final_url, save)
                return result
            else:
                logger.error(str(ret))
                return ret

        app.config['fetch'] = lambda x: gfetch(x)
        def gtask(x):
            message, task = x
            task = webui_fetcher.get_task(message, task, no_check_status = True)
            return task
        app.config['task'] = lambda x: gtask(x)

    app.config['queues'] = {}
    queue_setting = config.get("message_queue")
    app.config['queue_setting'] = queue_setting
    if queue_setting:
        queue_setting.setdefault('maxsize', 0)
        queue_setting.setdefault('queue_prefix', '')
        def connect_queue(name, value, log_level):
            protocol = value.get('protocol', 'amqp')
            prefix = value.get('queue_prefix', '')
            name =  value.get('name', name)
            if prefix:
                name = "%s_%s" % (prefix, name)
            host = value.get('host', 'localhost')
            port = value.get('port', 5672)
            user = value.get('user', 'guest')
            password = value.get('password', 'guest')
            maxsize = value.get('maxsize', 0)
            lazy_limit = value.get('lazy_limit', True)
            qo = utils.load_queue(protocol, name = name, host = host, port = port,
                user = user, password = password, maxsize = maxsize,
                lazy_limit = lazy_limit, log_level = log_level)
            return qo
        for q in app_config.get("queues", {}):
            _maxsize = queue_setting['maxsize']
            if q == 'spider2scheduler' or q == 'excinfo_queue':
                queue_setting['maxsize'] = 0
            app.config['queues'][q] = connect_queue(q, queue_setting, log_level)
            if _maxsize != queue_setting['maxsize']:
                queue_setting['maxsize'] = _maxsize

    if isinstance(scheduler_rpc, six.string_types):
        scheduler_rpc = connect_rpc(None, None, scheduler_rpc)
        app.config['scheduler_rpc'] = scheduler_rpc
        app.config['newtask'] = lambda x: scheduler_rpc.newtask(x).data
        app.config['status'] = lambda x: scheduler_rpc.status(x).data
        app.config['search_work'] = lambda x: scheduler_rpc.search_work(x).data
    else:
        Scheduler = load_cls(None, None, "cdspider.scheduler.Scheduler")
        webui_scheduler = Scheduler(newtask_queue=app.config['queues'].get("newtask_queue", None), inqueue=app.config['queues'].get('spider2scheduler', None), outqueue=app.config['queues'].get("scheduler2spider", None), status_queue=app.config['queues'].get("status_queue", None), projectdb=app.config["projectdb"], taskdb=app.config["taskdb"], sitedb=app.config["sitedb"], urlsdb=app.config["urlsdb"], keywordsdb=app.config["keywordsdb"], attachmentdb=attachmentdb, customdb=app.config.get("customdb", None), rate_map=app.config.get("rate_map"), log_level=log_level)
        def newtask(x):
            return webui_scheduler.newtask(x)
        app.config['newtask'] = lambda x: newtask(x)
        def status(x):
            return webui_scheduler.status(x)
        app.config['status'] = lambda x: status(x)
        def search_work(x):
            return webui_scheduler.search_work(x)
        app.config['search_work'] = lambda x: search_work(x)


    app.debug = debug
    return app

def main():
    cli()


if __name__ == "__main__":
    main()
