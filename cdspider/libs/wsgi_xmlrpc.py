# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-25 18:52:45
"""
from xmlrpc.server import SimpleXMLRPCDispatcher
import logging


class WSGIXMLRPCApplication(object):
    """
    基于WSGI的XMLRPC应用
    """

    def __init__(self, instance=None, methods=None, **kwargs):
        """
        创建xmlrpc dispatcher
        """
        if methods is None:
            methods = []
        self.dispatcher = SimpleXMLRPCDispatcher(allow_none=True, encoding=None)
        if instance is not None:
            self.dispatcher.register_instance(instance)
        for method in methods:
            self.dispatcher.register_function(method)
        self.dispatcher.register_introspection_functions()
        self.logger = kwargs.get('logger', logging.getLogger(__name__))

    def register_instance(self, instance):
        return self.dispatcher.register_instance(instance)

    def register_function(self, function, name=None):
        return self.dispatcher.register_function(function, name)

    def handler(self, environ, start_response):
        """
        处理HTTP访问
        """

        if environ['REQUEST_METHOD'] == 'POST':
            return self.handle_POST(environ, start_response)
        else:
            start_response("400 Bad request", [('Content-Type', 'text/plain')])
            return []

    def handle_POST(self, environ, start_response):
        """
        处理HTTP POST请求
        """

        try:
            length = int(environ['CONTENT_LENGTH'])
            data = environ['wsgi.input'].read(length)

            response = self.dispatcher._marshaled_dispatch(
                data, getattr(self.dispatcher, '_dispatch', None)
            )
            response += b'\n'
        except Exception as e:
            self.logger.exception(e)
            start_response("500 Server error", [('Content-Type', 'text/plain')])
            return []
        else:
            start_response("200 OK", [('Content-Type', 'text/xml'), ('Content-Length', str(len(response)),)])
            return [response]

    def __call__(self, environ, start_response):
        return self.handler(environ, start_response)
