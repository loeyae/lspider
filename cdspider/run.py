#! /usr/bin/python
# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

__author__ = "Zhang Yi <loeyae@gmail.com>"
__date__ = "$2018-1-9 18:04:41$"

import os
import traceback
import logging.config
from cdspider.libs.tools import *

cpath = os.path.dirname(os.path.abspath(__file__))


@click.group(invoke_without_command=True)
@click.option('-c', '--config', default=os.path.join(cpath, "config", "main.json"),
              callback=read_config, type=click.File(mode='r', encoding='utf-8'),
              help='命令行配置文件.配置文件格式: {"webui": {"port":5001}}', show_default=True)
@click.option('--logging-config', default=os.path.join(cpath, "config", "logging.conf"),
              help="日志配置文件", show_default=True)
@click.option('--app-config', default=os.path.join(cpath, "config", "app.json"),
              help="应用配置文件", show_default=True)
@click.option('--debug', default=False, is_flag=True, help='debug模式', show_default=True)
@click.option('--db-debug', default=False, is_flag=True, help='database debug模式', show_default=True)
@click.option('--sdebug', default=False, is_flag=True, help='source debug模式', show_default=True)
@click.option('--runtime-dir', default=None, help ='runtime文件夹', show_default=True)
@click.option('--database', help='数据库设置, default: '
              '{protocol: mongo, host: host, port: 27017, user: guest, password: guest, db: cdspider}')
@click.option('--proxy', default=None, help='代理设置', show_default=True)
@click.option('--queue-maxsize', default=1000, help='queue最大阈值', show_default=True)
@click.option('--queue-prefix', default=None, help='queue的前缀', show_default=True)
@click.option('--message-queue', help='queue设置, default: '
              'message_queue: {protocol: amqp, host: host, port: 5672, user: guest, password: guest}')
@click.option('--add-sys-path/--not-add-sys-path', default=False, is_flag=True,
              help='增加当前文件夹路径到系统路径', show_default=True)
@click.option('--testing-mode', default=False, is_flag=True, help='debug mode', show_default=True)
@click.pass_context
def cli(ctx, **kwargs):
    if kwargs['add_sys_path']:
        sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
    logging.config.fileConfig(kwargs['logging_config'])
    kwargs['logger'] = logging.getLogger("root")
    if kwargs['debug']:
        kwargs['logger'].setLevel(logging.DEBUG)
    if not kwargs['runtime_dir']:
        kwargs['runtime_dir'] = os.path.join(cpath, 'runtime')
    if not os.path.exists(kwargs['runtime_dir']):
        os.makedirs(kwargs['runtime_dir'])

    app_config = utils.load_config(kwargs['app_config'])

    db_setting = kwargs.get('database')
    db_object = {}
    if db_setting:
        connector = connect_db(ctx, None, db_setting)
        db_object = db_wrapper(connector, db_setting.get('protocol'), log_level=logging.DEBUG if kwargs['db_debug'] else logging.WARN)
    else:
        kwargs['testing_mode'] = True
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
@click.option('-m', '--mode', default=None, help="分发模式：handle类型,为空时执行全部handle", multiple=True,  show_default=True)
@click.option('-r', '--rate', default=None, help="分发模式：更新频率,为空时执行全部rate", multiple=True,  show_default=True)
@click.option('-o', '--outqueue', default=None, help='输出的queue', show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def route(ctx, scheduler_cls, mode, rate, outqueue, no_loop, get_object=False):
    """
    路由: 按handle和频率分发任务
    """
    mode = list(set(mode))
    g = ctx.obj
    scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = scheduler(ctx, mode=mode, rate=rate, outqueue=outqueue)
    g['instances'].append(scheduler)
    if get_object:
        return scheduler
    if no_loop:
        scheduler.run_once()
    else:
        scheduler.run()

@cli.command()
@click.option('--scheduler-cls', default='cdspider.scheduler.PlantaskScheduler', callback=load_cls, help='schedule name')
@click.option('-i', '--inqueue', default=None, help='监听的queue', show_default=True)
@click.option('-o', '--outqueue', default=None, help='输出的queue', show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def plantask_schedule(ctx, scheduler_cls, inqueue, outqueue, no_loop,  get_object=False):
    """
    根据路由的分发，将爬虫任务入队
    """
    g=ctx.obj
    scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = scheduler(ctx, inqueue=inqueue, outqueue=outqueue)
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
    调度器rpc接口
    """
    g = ctx.obj
    scheduler = load_cls(ctx, None, scheduler_cls)

    scheduler = scheduler(ctx)
    g['instances'].append(scheduler)
    scheduler.xmlrpc_run(xmlrpc_port, xmlrpc_host)

@cli.command()
@click.option('--fetch-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('-i', '--inqueue', default=None, help='监听的queue', show_default=True)
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.option('--no-sync', default=False, is_flag=True, help='不同步数据给kafka', show_default=True)
@click.pass_context
def fetch(ctx, fetch_cls, inqueue, no_loop, no_sync, get_object=False, no_input=False):
    """
    监听任务队列并执行抓取
    """
    g = ctx.obj
    spider = load_cls(ctx, None, fetch_cls)
    if no_input:
        inqueue = False
    spider = spider(ctx, no_sync=no_sync, inqueue = inqueue)
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
    采集器rpc接口
    """
    g = ctx.obj
    spider = load_cls(ctx, None, spider_cls)

    spider = spider(ctx, inqueue = False)
    g['instances'].append(spider)
    spider.xmlrpc_run(xmlrpc_port, xmlrpc_host)

@cli.command()
@click.option('--worker-cls', default='cdspider.worker.TestWorker', callback=load_cls, help='worker name')
@click.option('--no-loop', default=False, is_flag=True, help='不循环', show_default=True)
@click.pass_context
def work(ctx, worker_cls, no_loop,  get_object=False):
    """
    worker
    """
    g = ctx.obj
    worker = load_cls(ctx, None, worker_cls)
    worker = worker(ctx)
    g['instances'].append(worker)
    if get_object:
        return worker
    if no_loop:
        worker.run_once()
    else:
        worker.run()

@cli.command()
@click.option('-n', '--name', default="test_tool", help='tool名')
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
@click.option('--scheduler-cls', default='cdspider.scheduler.PlantaskScheduler', callback=load_cls, help='schedule name', show_default=True)
@click.option('-m', '--message', help="测试消息,JSON格式", show_default=True)
@click.option('--outqueue', default=None, help='输出的queue', show_default=True)
@click.pass_context
def schedule_test(ctx, scheduler_cls, message, outqueue):
    """
    计划任务测试
    """
    g = ctx.obj
    scheduler = load_cls(ctx, None, scheduler_cls)
    scheduler = scheduler(ctx, outqueue=outqueue)
    task = json.loads(message)
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
    spider = load_cls(ctx, None, spider_cls)
    inqueue = None
    if no_input:
        inqueue = False
    spider = spider(ctx, no_sync = True, handler=None, inqueue=inqueue)
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
    spider = load_cls(ctx, None, spider_cls)
    spider = spider(ctx, no_sync = True, handler=None, inqueue=False)
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
    worker = load_cls(ctx, None, worker_cls)
    worker = worker(ctx)
    task = dict({
        "rid": rid
    })
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
    """
    RPC接口测试
    """
    rpc = connect_rpc(ctx, None, rpc)
    ret = None
    if hasattr(rpc, method):
        if params is None:
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
@click.option('-q', '--queue', help='queue name')
@click.option('-m', '--message', help='message')
@click.pass_context
def send_queue(ctx, queue, message):
    ctx.obj['queue'][queue].put_nowait(json.loads(message))

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
        # route
        route_config = g['config'].get('route', {})
        route_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, route, **route_config))

        # plantask_schedule
        plantask_schedule_config = g['config'].get('plantask_schedule', {})
        for i in range(plantask_schedule_num):
            threads.append(run_in(ctx.invoke, plantask_schedule, **plantask_schedule_config))

        # schedule_rpc
        schedule_rpc_config = g['config'].get('schedule_rpc', {})
        schedule_rpc_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, schedule_rpc, **schedule_rpc_config))

        # fetch
        fetcher_config = g['config'].get('fetche', {})
        for i in range(fetch_num):
            threads.append(run_in(ctx.invoke, fetch, **fetcher_config))

        # spider_rpc
        spider_rpc_config = g['config'].get('spider_rpc', {})
        spider_rpc_config.setdefault('xmlrpc_host', '127.0.0.1')
        threads.append(run_in(ctx.invoke, spider_rpc, **spider_rpc_config))

        # work
        work_config = g['config'].get('work', None)
        if isinstance(work_config, (list, tuple)):
            for item in work_config:
                threads.append(run_in(ctx.invoke, work, **item))
        elif work_config:
            threads.append(run_in(ctx.invoke, work, **work_config))

        # tool
        tool_config = g['config'].get('tool', None)
        if isinstance(tool_config, (list, tuple)):
            for item in tool_config:
                threads.append(run_in(ctx.invoke, tool, **item))
        elif tool_config:
            threads.append(run_in(ctx.invoke, tool, **tool_config))

    except Exception:
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
