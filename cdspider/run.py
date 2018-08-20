#! /usr/bin/python
#-*- coding: UTF-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

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
@click.option('--database', help='数据库设置, default: '
              '{protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: cdspider}')
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

    app_config = utils.load_config(os.path.join(cpath, "config", "app.json"))

    db_setting = kwargs.get('database')
    db_object = {}
    if db_setting:
        connector = connect_db(ctx, None, db_setting)
        db_object['base'] = load_cls(ctx, None, 'cdspider.database.{protocol}.Base'.format(protocol = db_setting.get('protocol')))(connector)
        for d in app_config.get("database", {}):
            db = 'cdspider.database.{protocol}.{db}'.format(protocol = db_setting.get('protocol'), db= d)
            db_object[d] = load_cls(ctx, None, db)(connector)
    kwargs['db'] = db_object

    queue_object = {}
    queue_setting = kwargs.get("message_queue")
    if queue_setting:
        queue_setting.setdefault('maxsize', kwargs.get('queue_maxsize'))
        queue_setting.setdefault('queue_prefix', kwargs.get('queue_prefix', ''))
        for q in app_config.get("queues", {}):
            _maxsize = queue_setting['maxsize']
            if q == 'spider2scheduler' or q == 'excinfo_queue':
                queue_setting['maxsize'] = 0
            queue_object[q] = connect_message_queue(ctx, q, queue_setting)
            if _maxsize != queue_setting['maxsize']:
                queue_setting['maxsize'] = _maxsize

    kwargs['queue'] = queue_object

    ctx.obj = {}
    ctx.obj.update(kwargs)
    ctx.obj['app_config'] = app_config
    ctx.obj.setdefault('rate_map', ctx.obj['app_config'].get('ratemap', {}))
    ctx.obj['instances'] = []
    if ctx.invoked_subcommand is None and not ctx.obj.get('testing_mode'):
        ctx.invoke(all)
    return ctx

@cli.command()
@click.option('--scheduler_cls', default='cdspider.scheduler.Scheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def schedule(ctx, scheduler_cls, no_loop,  get_object=False):
    """
    Schedule: 根据TaskDB往queue:scheduler2spider 里塞任务
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), rate_map=rate_map, log_level=log_level)
    g['instances'].append(scheduler)
    if g.get('testing_mode') or get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        scheduler.run()

@cli.command()
@click.option('--scheduler_cls', default='cdspider.scheduler.Scheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def newtask_schedule(ctx,scheduler_cls, no_loop,  get_object=False):
    """
    newTask_schedle: 根据queue:newTask2scheduler往TaskDB 里存入新的任务数据
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    Scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), rate_map=rate_map, log_level=log_level)
    g['instances'].append(Scheduler)
    if g.get('testing_mode') or get_object:
        return Scheduler
    if no_loop:
        Scheduler.newTask_run_once()
    else:
        Scheduler.newTask_run()

@cli.command()
@click.option('--scheduler_cls', default='cdspider.scheduler.Scheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def status_schedule(ctx,scheduler_cls,no_loop,  get_object=False):
    """
    newTask_schedle: 根据queue:status2scheduler往TaskDB 里更新数据状态
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    Scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), rate_map=rate_map, log_level=log_level)
    g['instances'].append(status_schedule)
    if g.get('testing_mode') or get_object:
        return Scheduler
    if no_loop:
        Scheduler.status_run_once()
    else:
        Scheduler.status_run()

@cli.command()
@click.option('--insert_kafka_worker_cls', default='cdspider.worker.insert_kafka_worker', callback=load_cls, help='worker name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def insert_kafka_worker(ctx,insert_kafka_worker_cls,no_loop,  get_object=False):
    """
    newTask_schedle: 根据queue:status2scheduler往TaskDB 里更新数据状态
    """
    g=ctx.obj
    Worker = load_cls(ctx, None, insert_kafka_worker_cls)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    worker = Worker(g.get('db'),g.get('queue'),g['config']['kafka'] ,log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()


@cli.command()
@click.option('--fetch-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def fetch(ctx, fetch_cls, no_loop, get_object=False, no_input=False):
    """
    Fetch: 监听任务并执行抓取
    """
    g = ctx.obj
    Spider = load_cls(ctx, None, fetch_cls)
    db = g.get('db')
    queue = g.get('queue')
    if no_input:
        queue = {}
        db['TaskDB'] = None
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    attach_storage = g.get('app_config', {}).get('attach_storage', None)
    if attach_storage:
        attach_storage = os.path.realpath(os.path.join(cpath, attach_storage))
    if g.get("debug", False):
        log_level = logging.DEBUG

    spider = Spider(db = db, queue = queue, proxy=proxy, log_level=log_level, attach_storage = attach_storage)
    g['instances'].append(spider)
    if g.get('testing_mode') or get_object:
        return spider
    if no_loop:
        spider.run_once()
    else:
        spider.run()

@cli.command()
@click.option('--spider-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=24444, help="xmlrpc bind port")
@click.pass_context
def spider_rpc(ctx, spider_cls, xmlrpc_host, xmlrpc_port):
    """
    spider rpc
    """
    g = ctx.obj
    Spider = load_cls(ctx, None, spider_cls)
    db = g.get('db')
    queue = g.get('queue')
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    attach_storage = g.get('app_config', {}).get('attach_storage', None)
    if attach_storage:
        attach_storage = os.path.realpath(os.path.join(cpath, attach_storage))
    if g.get("debug", False):
        log_level = logging.DEBUG

    spider = Spider(db = db, queue = queue, proxy=proxy, log_level=log_level, attach_storage = attach_storage)
    g['instances'].append(spider)
    spider.xmlrpc_run(xmlrpc_port, xmlrpc_host)

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
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    worker = Worker(db = g.get('db'), queue = g.get('queue'), proxy=proxy, log_level=log_level)
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
    proxy = g.get('proxy', None)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    if mailer:
        mailer = utils.load_mailer(mailer, sender=sender, receiver=receiver)
    worker = Worker(db = db, queue = queue, proxy=proxy, mailer=mailer,
            log_level=log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('-n', '--name', help='tool名')
@click.option('-a', '--arg', multiple=True, help='tool参数')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def tool(ctx, name, arg, no_loop):
    g = ctx.obj
    cls_name = 'cdspider.tools.%s.%s' % (name, name)
    cls = load_cls(ctx, None, cls_name)
    c = cls(g, no_loop)
    c.process(*arg)

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

def main():
    cli()


if __name__ == "__main__":
    main()
