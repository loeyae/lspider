# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

class Base(object):

    STATUS_INIT = 0
    STATUS_ACTIVE = 1
    STATUS_DISABLE = 2
    STATUS_DELETED = -1

from .Articlesdb import ArticlesDB
from .MediaTypesdb import MediaTypesDB
from .Keywordsdb import KeywordsDB
from .Projectsdb import ProjectsDB
from .Sitesdb import SitesDB
from .Urlsdb import UrlsDB
from .Uniquedb import UniqueDB
from .Taskdb import TaskDB
from .SpiderTaskdb import SpiderTaskDB
from .ListRuledb import ListRuleDB
from .ParseRuledb import ParseRuleDB
from .ErrorLogdb import ErrorLogDB
from .CrawlLogdb import CrawlLogDB
