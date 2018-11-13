#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-12 8:56:07
"""
from cdspider.tools import Base

class active_attach_by_pid(Base):
    """
    actvie attack
    """

    def process(self, *args):
        pid = int(self.get_arg(args, 0, 'Pleas input pid'))
        self.broken('Project not exists', pid)
        project = self.g['db']['ProjectsDB'].get_detail(pid)
        self.broken('Project: %s not exists' % pid, project)
        self.notice('Selected Project Info:', project)
        AttachmentDB = self.g['db']['AttachmentDB']
        id = 0
        if len(args) > 1:
            id = int(args[1])
        sum = 0
        while True:
            i = 0
            for item in AttachmentDB.get_new_list_by_pid(id, pid, where={'status': AttachmentDB.STATUS_INIT}):
                d={}
                d['status'] = AttachmentDB.STATUS_ACTIVE
                d['aid'] = item['aid']
                self.info("push status_queue data: %s" %  str(d))
                self.g['queue']['status_queue'].put_nowait(d)
                i += 1
                sum += 1
                if item['aid'] > id:
                    id = item['aid']
            if i < 1:
                self.info("push status_queue data break")
                break
        self.info("push status_queue data total: %s" %  str(sum))
