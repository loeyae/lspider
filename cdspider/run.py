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
@click.option('--runtime-dir', default=None, help ='runtime文件夹', show_default=True)
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
    if kwargs['runtime_dir']:
        if not os.path.exists(kwargs['runtime_dir']):
            os.makedirs(kwargs['runtime_dir'])

    app_config = utils.load_config(os.path.join(cpath, "config", "app.json"))

    db_setting = kwargs.get('database')
    db_object = {}
    if db_setting:
        connector = connect_db(ctx, None, db_setting)
        db_object = db_wrapper(connector, db_setting.get('protocol'))
    kwargs['db'] = db_object

    queue_object = {}
    queue_setting = kwargs.get("message_queue")
    if queue_setting:
        queue_setting.setdefault('maxsize', kwargs.get('queue_maxsize'))
        queue_setting.setdefault('queue_prefix', kwargs.get('queue_prefix', ''))
        queue_object = queue_wrapper(ctx, queue_setting)

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
@click.option('--scheduler_cls', default='cdspider.scheduler.Router', callback=load_cls, help='schedule name')
@click.option('--mode', default='project', type=click.Choice(['project', 'site', 'item']), help="分发模式", show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.option('--xmlrpc', default=False, is_flag=True, help='开启xmlrpc', show_default=True)
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=23333, help="xmlrpc bind port")
@click.pass_context
def route(ctx, scheduler_cls, mode, no_loop, xmlrpc, xmlrpc_host, xmlrpc_port, get_object=False):
    """
    路由: 按project、site、item其中一种模式分发计划任务
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    scheduler = Scheduler(db=g.get('db'), queue=g.get('queue'), mode=mode, log_level=log_level)
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
@click.option('--scheduler_cls', default='cdspider.scheduler.PlantaskScheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def plantask_schedule(ctx, scheduler_cls, no_loop,  get_object=False):
    """
    按任务的plantime进行抓取队列入队
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
@click.option('--scheduler_cls', default='cdspider.scheduler.SynctaskScheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def synctask_schedule(ctx, scheduler_cls, no_loop,  get_object=False):
    """
    plantask_schedule之后同步任务的plantime时间
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), log_level=log_level)
    g['instances'].append(scheduler)
    if g.get('testing_mode') or get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        scheduler.run()

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.NewtaskScheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def newtask_schedule(ctx,scheduler_cls, no_loop,  get_object=False):
    """
    根据管理平台添加的url、关键词等生成任务
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    Scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), log_level=log_level)
    g['instances'].append(Scheduler)
    if g.get('testing_mode') or get_object:
        return Scheduler
    if no_loop:
        Scheduler.run_once()
    else:
        Scheduler.run()

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.StatusScheduler', callback=load_cls, help='schedule name')
@click.option('--interval', default=0.1, help='循环间隔', show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def status_schedule(ctx,scheduler_cls, interval, no_loop, get_object=False):
    """
    根据管理平台对project、site、url、keywords等的状态改变，调整相应任务状态
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    Scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), log_level=log_level)
    g['instances'].append(status_schedule)
    if g.get('testing_mode') or get_object:
        return Scheduler
    if no_loop:
        Scheduler.run_once()
    else:
        Scheduler.run()

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.SearchScheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def search_schedule(ctx,scheduler_cls, no_loop, get_object=False):
    """
    根据type为search的站点或关键词组合新的任务
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    rate_map = g.get('rate_map')

    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    Scheduler = Scheduler(db = g.get('db'), queue = g.get('queue'), log_level=log_level)
    g['instances'].append(status_schedule)
    if g.get('testing_mode') or get_object:
        return Scheduler
    if no_loop:
        Scheduler.run_once()
    else:
        Scheduler.run()

@cli.command()
@click.option('--fetch-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.option('--no-sync', default=False, is_flag=True, help='不同步数据给kafka', show_default=True)
@click.pass_context
def fetch(ctx, fetch_cls, no_loop, no_sync, get_object=False, no_input=False):
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

    spider = Spider(db = db, queue = queue, proxy=proxy, no_sync=no_sync, log_level=log_level, attach_storage = attach_storage)
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
@click.option('--worker-cls', default='cdspider.worker.ExcWorker', callback=load_cls, help='worker name')
@click.option('--mailer', default=None, help='mailer name', show_default=True)
@click.option('--sender', default=None, show_default=True, help='发件人设置'
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
    worker = Worker(db = g.get('db'), queue = g.get('queue'), proxy=proxy, mailer=mailer,
            log_level=log_level)
    g['instances'].append(worker)
    if g.get('testing_mode') or get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.SyncKafkaWorker', callback=load_cls, help='worker name')
@click.option('--kafka-cfg', default=None, show_default=True, help='Kafka配置'
              ' default: {host: host, zookeeper_hosts: 27017, topic: topic name, user: guest, password: guest}')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def sync_kafka_work(ctx, worker_cls, kafka_cfg, no_loop,  get_object=False):
    """
    同步数据到kafka
    """
    g=ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    worker = Worker(g.get('db'),g.get('queue'), kafka_cfg, log_level)
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
    """
    工具
    """
    g = ctx.obj
    cls_name = 'cdspider.tools.%s.%s' % (name, name)
    cls = load_cls(ctx, None, cls_name)
    c = cls(g, no_loop)
    c.process(*arg)

@cli.command()
@click.option('--rebot-cls', default='cdspider.robots.WxchatRobots', callback=load_cls, help='schedule name')
@click.option('--aichat-rpc', default='http://127.0.0.1:27777', help='robot rpc server')
@click.option('-u', '--uuid', help='唯一标识')
@click.pass_context
def wechat(ctx, rebot_cls, aichat_rpc, uuid):
    """
    web wechat
    """
    g = ctx.obj
    aichat_rpc = connect_rpc(ctx, None, aichat_rpc)
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(db=g.get('db'), queue=g.get('queue'), uuid=uuid, data_dir=g.get("runtime_dir", None), debug=g.get("debug", False), log_level=log_level)
    reply = lambda m, s: aichat_rpc.reply(m, s)
    def init_aichat(m, s):
        info = m.search_friends()
        k = {
            'name': info['NickName'],
            'age': 18,
            'sex': '男' if info['Sex'] != 1 else '女'
        }
        aichat_rpc.init(k, s)
    init = lambda m, s: init_aichat(m, s)
    robot.add_prepare_reply(init)
    robot.set_reply(reply)
    robot.run()

@cli.command()
@click.option('--rebot-cls', default='cdspider.robots.AichatRobots', callback=load_cls, help='schedule name')
@click.option('-u', '--uuid', default=None, help='唯一标识')
@click.option('-b', '--bot-data', help='AI头脑文件目录')
@click.option('-c', '--commands',  multiple=True, help='commands')
@click.pass_context
def aichat(ctx, rebot_cls, uuid, bot_data, commands):
    """
    Aiml bot
    """
    g = ctx.obj
    log_level = logging.WARN
    if g.get("debug", False):
        log_level = logging.DEBUG
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(db=g.get('db'), queue=g.get('queue'), commands = commands, bot_data = bot_data, data_dir=g.get("runtime_dir", None), debug=g.get("debug", False), log_level=log_level)
    robot.run(uuid)

@cli.command()
@click.option('--rebot-cls', default='cdspider.robots.AichatRobots', callback=load_cls, help='rebot name')
@click.option('-b', '--bot-data', help='AI头脑文件目录')
@click.option('-c', '--commands', default=[], multiple=True, help='commands')
@click.option('-s', '--settings', default=None, multiple=True, help='bot settings: [name, sex, age, company]')
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=27777, help="xmlrpc bind port")
@click.option('--debug', default=False, is_flag=True, help='debug模式', show_default=True)
@click.pass_context
def aichat_rpc(ctx, rebot_cls, bot_data, commands, settings, xmlrpc_host, xmlrpc_port, debug):
    """
    Aiml bot rpc
    """
    g = ctx.obj
    log_level = logging.WARN
    if debug and g.get("debug", False):
        log_level = logging.DEBUG
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(db=g.get('db'), queue=g.get('queue'), commands = commands, bot_data = bot_data, settings = settings, data_dir=g.get("runtime_dir", None), debug= debug and g.get("debug", False), log_level=log_level)
    robot.xmlrpc_run(xmlrpc_port, xmlrpc_host)

@cli.command()
@click.option('--aichat-rpc', default='http://127.0.0.1:27777', help='robot rpc server')
@click.pass_context
def aichat_rpc_hello(ctx, aichat_rpc):
    """
    测试Aiml bot rpc
    """
    g = ctx.obj
    aichat_rpc = connect_rpc(ctx, None, aichat_rpc)
    print(aichat_rpc.hello())

@cli.command()
@click.option('--fetch-num', default=1, help='fetch实例个数')
@click.option('--exc-work-num', default=1, help='exc worker实例个数')
@click.option('--run-in', default='subprocess', type=click.Choice(['subprocess', 'thread']),
              help='运行模式:subprocess or thread')
@click.pass_context
def all(ctx, fetch_num, exc_work_num, run_in):
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
        #route
        route_config = g['config'].get('route', {})
        route_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, route, **route_config))

        #newtask_schedule
        newtask_schedule_config = g['config'].get('newtask_schedule', {})
        threads.append(run_in(ctx.invoke, newtask_schedule, **newtask_schedule_config))

        #plantask_schedule
        plantask_schedule_config = g['config'].get('plantask_schedule', {})
        threads.append(run_in(ctx.invoke, plantask_schedule, **plantask_schedule_config))

        #synctask_schedule
        synctask_schedule_config = g['config'].get('synctask_schedule', {})
        threads.append(run_in(ctx.invoke, synctask_schedule, **synctask_schedule_config))

        #status_schedule
        status_schedule_config = g['config'].get('status_schedule', {})
        threads.append(run_in(ctx.invoke, status_schedule, **status_schedule_config))

        #search_schedule
        search_schedule_config = g['config'].get('search_schedule', {})
        threads.append(run_in(ctx.invoke, search_schedule, **search_schedule_config))

        #spider_rpc
        spider_rpc_config = g['config'].get('spider_rpc', {})
        spider_rpc_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, spider_rpc, **spider_rpc_config))

        #fetch
        fetcher_config = g['config'].get('fetche', {})
        for i in range(fetch_num):
            threads.append(run_in(ctx.invoke, fetch, **fetcher_config))

        #exc worker
        exc_worker_config = g['config'].get('exc_work', {})
        for i in range(exc_work_num):
            threads.append(run_in(ctx.invoke, exc_work, **exc_worker_config))

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
