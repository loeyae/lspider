#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

class Base(object):

    STATUS_INIT = 0
    STATUS_ACTIVE = 1
    STATUS_DELETED = -1

from .Articlesdb import ArticlesDB
from .ChannelRulesdb import ChannelRulesDB
from .CrawlLogdb import CrawlLogDB
from .Keywordsdb import KeywordsDB
from .MediaTypesdb import MediaTypesDB
from .ParseRuledb import ParseRuleDB
from .Projectsdb import ProjectsDB
from .Sitesdb import SitesDB
from .Taskdb import TaskDB
from .Uniquedb import UniqueDB
from .Urlsdb import UrlsDB
from .UrlsUniquedb import UrlsUniqueDB
from .SpiderTaskdb import SpiderTaskDB
from .ListRuledb import ListRuleDB
from .ParseRuledb import ParseRuleDB
from .ErrorLogdb import ErrorLogDB
