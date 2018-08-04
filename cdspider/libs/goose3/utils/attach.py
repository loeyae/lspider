#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-7-6 14:01:43
:version: SVN: $Id: attach.py 2316 2018-07-06 09:38:42Z zhangyi $
"""
import hashlib
import os

from cdspider.libs.goose3.utils.encoding import smart_str

class AttachUtils():
    """
    attach utils
    """

    @classmethod
    def store_attach(cls, http_client, link_hash, src, config):
        """\
        Writes an attach src http string to disk as a temporary file
        and returns the local path
        """
        # check for a cache hit already on disk
        local_attach = cls.read_localfile(link_hash, src, config)
        if local_attach:
            return local_attach

        # download the attachment
        data = http_client.fetch(src)
        if data:
            local_path = cls.write_localfile(data, link_hash, src, config)
            if local_path:
                return local_path
        return None

    @classmethod
    def read_localfile(cls, link_hash, src, config):
        local_attach_name = cls.get_localfile_name(link_hash, src, config)
        if os.path.isfile(local_attach_name):
            return local_attach_name
        return None

    @classmethod
    def write_localfile(cls, entity, link_hash, src, config):
        if not os.path.exists(config.local_storage_path):
            os.makedirs(config.local_storage_path)
        local_path = cls.get_localfile_name(link_hash, src, config)
        with open(local_path, 'wb') as fobj:
            fobj.write(entity)
        return local_path

    @classmethod
    def get_localfile_name(cls, link_hash, src, config):
        attach_hash = hashlib.md5(smart_str(src)).hexdigest()
        link_hash = hashlib.md5(smart_str(link_hash)).hexdigest()
        return os.path.join(config.local_storage_path, '%s_%s' % (link_hash, attach_hash)) + os.path.splitext(src)[1]
