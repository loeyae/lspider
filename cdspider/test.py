#! /usr/bin/python
#-*- coding: UTF-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2018-12-12 21:41:35$"

from cdspider.run import *

@cli.command()
@click.option('--spider-cls', default='cdspider.spider.Spider', callback=load_cls, help='spider name')
@click.option('-U', '--url', default='http://2018.ip138.com/ic.asp', help='url')
@click.option('-M', '--mode', default="default", help="mode")
@click.option('-P', '--pid', default="0", help="pid")
@click.option('-S', '--sid', default="0", help="sid")
@click.option('-T', '--tid', default="0", help="tid")
@click.option('-I', '--tier', default="1", help="tier")
@click.option( '--no-input/--has-input', default=True, is_flag=True, help='no/has input')
@click.pass_context
def test(ctx, spider_cls, url, mode, pid, sid, tid, tier, no_input):
    Spider = load_cls(ctx, None, spider_cls)
    spider = Spider(ctx, no_sync = True, handler=None, no_input=no_input)
    task = {
        "url": url,
        "mode": mode,
        "pid": pid,
        "sid": sid,
        "tid": tid,
        "tier": tier,
    }
#    task = None
    task = spider.get_task(message = task, no_check_status = True)
    spider.fetch(task=task, return_result = False)

if __name__ == "__main__":
    main()
