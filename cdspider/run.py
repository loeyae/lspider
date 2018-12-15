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
              help='命令行配置文件. {"webui": {"port":5001}}', show_default=True)
@click.option('--logging-config', default=os.path.join(cpath, "config", "logging.conf"),
              help="日志配置文件", show_default=True)
@click.option('--app-config', default=os.path.join(cpath, "config", "app.json"),
              help="配置文件", show_default=True)
@click.option('--debug', default=False, is_flag=True, help='debug模式', show_default=True)
@click.option('--db-debug', default=False, is_flag=True, help='debug模式', show_default=True)
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

    app_config = utils.load_config(kwargs['app_config'])

    db_setting = kwargs.get('database')
    db_object = {}
    if db_setting:
        connector = connect_db(ctx, None, db_setting)
        db_object = db_wrapper(connector, db_setting.get('protocol'), log_level=logging.DEBUG if kwargs['db_debug'] else logging.WARN)
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
    ctx.obj['app_path'] = cpath
    ctx.obj.setdefault('rate_map', ctx.obj['app_config'].get('ratemap', {}))
    ctx.obj['instances'] = []
    if ctx.invoked_subcommand is None:
        ctx.invoke(all)
    return ctx

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.Router', callback=load_cls, help='schedule name')
@click.option('--mode', default='project', type=click.Choice(['project', 'site', 'task']), help="分发模式", show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def route(ctx, scheduler_cls, mode, no_loop, get_object=False):
    """
    路由: 按project、site、task其中一种方式分发计划任务
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = Scheduler(ctx, mode=mode)
    g['instances'].append(scheduler)
    if get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        scheduler.run()

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.PlantaskScheduler', callback=load_cls, help='schedule name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def plantask_schedule(ctx, scheduler_cls, no_loop,  get_object=False):
    """
    按任务的plantime进行抓取队列入队
    """
    g=ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = Scheduler(ctx)
    g['instances'].append(scheduler)
    if get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        scheduler.run()


@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.Router', callback=load_cls, help='scheduler name')
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host")
@click.option('--xmlrpc-port', default=23333, help="xmlrpc bind port")
@click.pass_context
def schedule_rpc(ctx, scheduler_cls, xmlrpc_host, xmlrpc_port):
    """
    spider rpc
    """
    g = ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)

    scheduler = Scheduler(ctx)
    g['instances'].append(scheduler)
    scheduler.xmlrpc_run(xmlrpc_port, xmlrpc_host)


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
    spider = Spider(ctx, no_sync=no_sync, no_input = no_input)
    g['instances'].append(spider)
    if get_object:
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

    spider = Spider(ctx, no_input = True)
    g['instances'].append(spider)
    spider.xmlrpc_run(xmlrpc_port, xmlrpc_host)

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.TestWorker', callback=load_cls, help='worker name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def work(ctx, worker_cls, no_loop,  get_object=False):
    """
    同步数据到kafka
    """
    g = ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    worker = Worker(ctx)
    g['instances'].append(worker)
    if get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('-n', '--name', help='tool名')
@click.option('-a', '--arg', multiple=True, help='tool参数')
@click.option('--daemon', default=False, is_flag=True, help='是否作为守护进程', show_default=True)
@click.pass_context
def tool(ctx, name, arg, daemon):
    """
    工具
    """
    cls_name = 'cdspider.tools.%s.%s' % (name, name)
    cls = load_cls(ctx, None, cls_name)
    c = cls(ctx, daemon)
    if daemon:
        c.run(*arg)
    else:
        c.run_once(*arg)

@cli.command()
@click.option('--rebot-cls', default='cdspider.robots.WxchatRobots', callback=load_cls, help='schedule name', show_default=True)
@click.option('--aichat-rpc', default='http://127.0.0.1:27777', help='robot rpc server', show_default=True)
@click.option('-u', '--uuid', help='唯一标识')
@click.pass_context
def wechat(ctx, rebot_cls, aichat_rpc, uuid):
    """
    web wechat
    """
    aichat_rpc = connect_rpc(ctx, None, aichat_rpc)
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(ctx, uuid=uuid)
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
@click.option('--rebot-cls', default='cdspider.robots.AichatRobots', callback=load_cls, help='schedule name', show_default=True)
@click.option('-u', '--uuid', default=None, help='唯一标识', show_default=True)
@click.option('-b', '--bot-data', help='AI头脑文件目录')
@click.option('-c', '--commands',  multiple=True, help='commands')
@click.pass_context
def aichat(ctx, rebot_cls, uuid, bot_data, commands):
    """
    Aiml bot
    """
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(ctx, commands = commands, bot_data = bot_data)
    robot.run(uuid)

@cli.command()
@click.option('--rebot-cls', default='cdspider.robots.AichatRobots', callback=load_cls, help='rebot name', show_default=True)
@click.option('-b', '--bot-data', help='AI头脑文件目录')
@click.option('-c', '--commands', default=[], multiple=True, help='commands')
@click.option('-s', '--settings', default=None, multiple=True, help='bot settings: [name, sex, age, company]', show_default=True)
@click.option('--xmlrpc-host', default='0.0.0.0', help="xmlrpc bind host", show_default=True)
@click.option('--xmlrpc-port', default=27777, help="xmlrpc bind port", show_default=True)
@click.option('--debug', default=False, is_flag=True, help='debug模式', show_default=True)
@click.pass_context
def aichat_rpc(ctx, rebot_cls, bot_data, commands, settings, xmlrpc_host, xmlrpc_port, debug):
    """
    Aiml bot rpc
    """
    rebot_cls = load_cls(ctx, None, rebot_cls)
    robot = rebot_cls(ctx, commands = commands, bot_data = bot_data, settings = settings)
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
@click.option('--scheduler-cls', default='cdspider.scheduler.PlantaskScheduler', callback=load_cls, help='schedule name', show_default=True)
@click.option('-i', '--id', help="task id")
@click.option('-m', '--mode', default='project', help="mode id", show_default=True)
@click.option('-h', '--handler-mode', default='list', help="mode id", show_default=True)
@click.pass_context
def schedule_test(ctx, scheduler_cls, id, mode, handler_mode):
    """
    计划任务测试
    """
    g = ctx.obj
    Scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = Scheduler(ctx)
    task = {
        "item": int(id),
        "mode": mode,
        "h-mode": handler_mode
    }
    scheduler.schedule(task)

@cli.command()
@click.option('--spider-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name', show_default=True)
@click.option('-s', '--setting', callback=load_config, type=click.File(mode='r', encoding='utf-8'),
              help='任务配置json文件', show_default=True)
@click.option('-o', '--output', default=None, help='数据保存的文件', show_default=True)
@click.option( '--no-input/--has-input', default=True, is_flag=True, help='no/has input', show_default=True)
@click.pass_context
def spider_test(ctx, spider_cls, setting, output, no_input):
    """
    抓取流程测试
    """
    Spider = load_cls(ctx, None, spider_cls)
    spider = Spider(ctx, no_sync = True, handler=None, no_input=no_input)
    task = spider.get_task(message = setting, no_check_status = True)
    return_result = spider.fetch(task=task, return_result = setting.get("return_result", False))
    print(return_result)
    if output:
        f = open(output, 'w')
        f.write(json.dumps(return_result))
        f.close()

@cli.command()
@click.option('--spider-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name', show_default=True)
@click.option('-t', '--tid', help="task id")
@click.option('-m', '--mode', default='list', help="mode id", show_default=True)
@click.option('-o', '--output', default=None, help='数据保存的文件', show_default=True)
@click.pass_context
def fetch_task(ctx, spider_cls, tid, mode, output):
    """
    按任务ID抓取数据,测试时使用
    """
    g = ctx.obj
    Spider = load_cls(ctx, None, spider_cls)
    spider = Spider(ctx, no_sync = True, handler=None, no_input=True)
    task = {
        "uuid": int(tid),
        "mode": mode,
    }
    return_result = False
    if output:
        return_result = True
    ret = spider.fetch(task=task, return_result=return_result)
    if output:
        if output == "print":
            print(ret)
        else:
            f = open(output, 'w')
            f.write(json.dumps(ret))
            f.close()

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.ResultWorker', callback=load_cls, help='spider name', show_default=True)
@click.option('-r', '--rid', help="result id")
@click.option('-o', '--output', default=None, help='数据保存的文件', show_default=True)
@click.pass_context
def fetch_result(ctx, worker_cls, rid, output):
    """
    抓取文章测试结果
    """
    g = ctx.obj
    Worker = load_cls(ctx, None, worker_cls)
    worker = Worker(ctx)
    task = {
        "rid": rid
    }
    task['return_result'] = False
    if output:
        task['return_result'] = True
    ret = worker.on_result(message=task)
    if output:
        if output == "print":
            print(ret)
        else:
            f = open(output, 'w')
            f.write(json.dumps(ret))
            f.close()

@cli.command()
@click.option('-r', '--rpc', default='http://127.0.0.1:23333', callback=connect_rpc, help='rpc host', show_default=True)
@click.option('-m', '--method', default='hello', help="rpc method", show_default=True)
@click.option('-p', '--params', callback=load_config, type=click.File(mode='r', encoding='utf-8'),
              help='任务配置json文件', show_default=True)
@click.option('-o', '--output', default=None, help='数据保存的文件', show_default=True)
@click.pass_context
def rpc_test(ctx, rpc, method, params, output):
    import json
    rpc = connect_rpc(ctx, None, rpc)
    ret = None
    if hasattr(rpc, method):
        if params == None:
            data = getattr(rpc, method)()
        else:
            data = getattr(rpc, method)(json.dumps(params))
        ret = json.loads(data)
    if output:
        f = open(output, 'w')
        f.write(json.dumps(ret))
        f.close()
    else:
        print(ret)

@cli.command()
@click.option('--fetch-num', default=1, help='fetch实例个数')
@click.option('--plantask-schedule-num', default=1, help='plantask schedule实例个数')
@click.option('--run-in', default='subprocess', type=click.Choice(['subprocess', 'thread']),
              help='运行模式:subprocess or thread')
@click.pass_context
def all(ctx, fetch_num, plantask_schedule_num, run_in):
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

        #plantask_schedule
        plantask_schedule_config = g['config'].get('plantask_schedule', {})
        for i in range(plantask_schedule_num):
            threads.append(run_in(ctx.invoke, plantask_schedule, **plantask_schedule_config))

        #schedule_rpc
        schedule_rpc_config = g['config'].get('schedule_rpc', {})
        schedule_rpc_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, schedule_rpc, **schedule_config))

        #fetch
        fetcher_config = g['config'].get('fetche', {})
        for i in range(fetch_num):
            threads.append(run_in(ctx.invoke, fetch, **fetcher_config))

        #spider_rpc
        spider_rpc_config = g['config'].get('spider_rpc', {})
        spider_rpc_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, spider_rpc, **spider_rpc_config))

        #work
        work_config = g['config'].get('work', None)
        if isinstance(work_config, (list, tuple)):
            for item in work_config:
                threads.append(run_in(ctx.invoke, work, **item))
        elif work_config:
            threads.append(run_in(ctx.invoke, work, **work_config))

        #tool
        tool_config = g['config'].get('tool', None)
        if isinstance(tool_config, (list, tuple)):
            for item in tool_config:
                threads.append(run_in(ctx.invoke, tool, **item))
        elif tool_config:
            threads.append(run_in(ctx.invoke, tool, **tool_config))

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
