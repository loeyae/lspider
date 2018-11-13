#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-6 14:16:44
"""
import os
import sys
import types
import time
import json
import aiml
import aiml.Utils
import jieba
import jieba.posseg
import jieba.analyse
import logging
import tempfile
import traceback
import string
import cdspider
import cdspider.parser
import shelve
from cdspider.libs import utils

chinese_punctuation = """，。；‘’“”：？！　、－＝－＋＊（）［］｛｝＜＞《》《》【】｛｝｀～"""

jieba.setLogLevel(logging.WARN)
jieba.initialize()
stopwords_file = os.path.join(os.path.dirname(cdspider.parser.lib.goose3.__file__), 'resources', 'text', 'stopwords-zh.txt')
jieba.analyse.set_stop_words(stopwords_file)
punctuation = chinese_punctuation + string.punctuation
punctuation_tbl = dict.fromkeys([ord(x) for x in punctuation], " ")
def load_stopwords():
    try:
        content = cdspider.parser.lib.goose3.utils.FileHelper.loadResourceFile(stopwords_file)
        word_list = content.splitlines()
    except IOError:
        word_list = []
    return word_list
stopwords_list = load_stopwords()

def extract(msg):
    if punctuation_tbl:
        msg = msg.translate(punctuation_tbl)
    p = None
#    word_list = [i.word for i in jieba.posseg.cut(msg) if i.flag in ('r', 'n', 'ns', 'a', 'vn', 'v')]
#    p = " ".join(word_list) if word_list else None
#    if not p:
#        word_list = jieba.analyse.textrank(msg, topK=20, withWeight=False, allowPOS=('ns', 'n', 'vn', 'v')) or jieba.analyse.extract_tags(msg, topK=20, withWeight=False, allowPOS=())
#        p = " ".join(word_list) if word_list else None
    if not p:
        word_list = [i for i in jieba.cut(msg) if i and i != " "] #[i for i in jieba.cut(msg) if not i in stopwords_list]
        p = " ".join(word_list) if word_list else None
    if not p:
        p = msg
    return p

template = """<?xml version='1.0' encoding='UTF-8' ?>

<aiml version="1.0">
{rules}
</aiml>
"""

category_template = """
<category>
        <pattern>{pattern}</pattern>
        <template>
                {answer}
        </template>
</category>
"""

srai_template = """
<category>
        <pattern>{pattern}</pattern>
        <template><srai>{answer}</srai></template>
</category>
"""

li_template = """
<category>
        <pattern>{pattern}</pattern>
        <template>
                <random>
                </random>
        </template>
</category>
"""

aiml.AimlParser.AimlHandler._validationInfo101.update({
    "arg": ( [], [], True ),
    "train": ( [], [], True ),
    "extract": ( [], ["index"], False ),
    "date": ( [], ["format"], False ),
    "tool": ([], [], True),
    "operate": ([], [], True),
    })

class AichatRobots(cdspider.Component, aiml.Kernel):
    """
    put you comment
    """
    _extractHistory = '_extractHistory'
    _matchedHistory = '_matchedHistory'

    settings = {
        "name": "小Q",
        "sex": "男",
        "age": 1,
        "company": "博彦多彩",
    }

    def __init__(self, db, queue, commands = [], settings = None, learn_file = 'auto-gen.aiml', bot_data = None, data_dir = None, debug = False, log_level = logging.WARN):
        self.db = db
        self.queue = queue
        self.debug_mode = debug
        self.log_level = log_level
        logger = logging.getLogger('robots')
        cdspider.Component.__init__(self, logger, log_level)
        self.temp_dir = data_dir or tempfile.gettempdir()
        self.cache_db = os.path.join(self.temp_dir, 'aichat_auto-gen.db')
        self.startup_file = 'startup.xml'
        self.bot_data = bot_data
        self.default_bot_data = os.path.join(os.path.dirname(cdspider.__file__), 'config/aiml/')
        print(self.bot_data)
        self.learn_file = os.path.join(bot_data, learn_file) if bot_data else os.path.join(self.default_bot_data, learn_file)
        brnfilename = 'bot_brain.brn'
        self.brnfile = os.path.join(self.temp_dir, brnfilename)
        self.mt_time = None
        self.commands = commands
        self.message = None
        self.stop_words = None
        self.punctuation_tbl = None
        if self.debug_mode:
            self.clear_cache()
        aiml.Kernel.__init__(self)
        self._elementProcessors.update({
            "arg": self._processArg,
            "extract": self._processExtract,
            "matched": self._processMatched,
            "train": self._processTrain,
            "tool": self._processTool,
            "operate": self._processOperate,
            })
        self._verboseMode = self.debug_mode
        setting_keys = ['name', 'sex', 'age', 'company']
        if isinstance(settings, (list, tuple)):
            i = 0
            for item in settings:
                self.settings[setting_keys[i]] = item
                i += 1
        elif isinstance(settings, dict):
            self.settings.update(settings)
        elif settings:
            self.settings['name'] = settings
        self.init_aiml()

    def init_aiml(self):
        learnFiles = [self.startup_file]
        bot_data_dir = None
        if os.path.exists(self.bot_data):
            bot_data_dir = os.path.abspath(self.bot_data)
        if not bot_data_dir:
            bot_data_dir = os.path.abspath(self.default_bot_data)
        commands = set()
        #commands.add("load aiml b")
        if self.commands:
            for item in list(self.commands):
                commands.add(item)
        if not commands:
            return
        self.setBotPredicate("bot-data", bot_data_dir)
        #TODO 设置自定义变量
        for k,v in self.settings.items():
            print(k, v)
            self.setBotPredicate(k, v)
        if os.path.isfile(self.brnfile) and not self.debug_mode:
            self.bootstrap(brainFile = self.brnfile)
        else:
            self.bootstrap(learnFiles = learnFiles, commands = commands, chdir = bot_data_dir)
            self.saveBrain(self.brnfile)
        self.mt_time = os.stat(self.brnfile).st_mtime

    def _addSession(self, sessionID):
        """Create a new session with the specified ID string."""
        if sessionID in self._sessions:
            return
        # Create the session.
        self._sessions[sessionID] = {
            # Initialize the special reserved predicates
            self._inputHistory: [],
            self._outputHistory: [],
            self._inputStack: [],
            self._extractHistory: [],
            self._matchedHistory: [],
        }

    def _processDate(self, elem, sessionID):
        week_ = ["星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"]
        format_ = elem[1]['format'] if ("format" in elem[1] and elem[1]['format']) else "%Y年%m月%d日 %H:%i:%s"
        response = time.strftime(format_)
        if format_ == "%w":
            return week[int(response)]
        return response

    def _processArg(self, elem, sessionID):
        response = ""
        for e in elem[2:]:
            response += self._processElement(e, sessionID)
        return response

    def _processExtract(self, elem, sessionID):
        extractHistory = self.getPredicate(self._extractHistory, sessionID)
        try: index = int(elem[1]['index'])
        except: index = 1
        try: return extractHistory[-index]
        except IndexError:
            if self._verboseMode:
                err = "No such extract %d while processing <extract> element.\n" % index
                sys.stderr.write(err)
            return ""

    def _processMatched(self, elem, sessionID):
        matchedHistory = self.getPredicate(self._matchedHistory, sessionID)
        try: index = int(elem[1]['index'])
        except: index = 1
        try: return matchedHistory[-index]
        except IndexError:
            if self._verboseMode:
                err = "No such extract %d while processing <matched> element.\n" % index
                sys.stderr.write(err)
            return ""

    def _processTrain(self, elem, sessionID):
        args = []
        for e in elem[2:]:
            args.append(self._processElement(e, sessionID))
        assert len(args) > 0, "train need more than one args"
        rule = args[0]
        temp = args[1] if len(args) > 1 else None
        append = args[2] if len(args) > 2 else None
        try:
            p = extract(rule)
            db = shelve.open(self.cache_db, "c", writeback=True)
            if not temp:
                temp = '我不明白你的意思，如果你要教我学习，请回复：说错了'
            if append:
                value = db[p]
                if not value:
                    value = []
                if not isinstance(value, (list, tuple)):
                    value = [value]
                value = set(value)
                value.add(temp)
            else:
                value = temp
            db[p] = value
            db.sync()
            self.sync_aiml()
        except:
            self.error(traceback.format_exc())
        return ""

    def _processTool(self, elem, sessionID):
        args = []
        for e in elem[2:]:
            args.append(self._processElement(e, sessionID))
        assert len(args) > 0, "tool need more than one args"
        mode = args[0]
        try:
            if mode == "cut":
                assert len(args) > 2, "tool:cut need more than three args"
                m = "left"
                if len(args) > 3:
                    m = args[3]
                src = args[1]
                sub = args[2]
                if m == 'right+':
                    return src[:(src.find(sub)+len(sub))]
                elif m == 'right':
                    return src[:src.find(sub)]
                elif m == 'left+':
                    return src[src.find(sub):]
                return src[(src.find(sub)+len(sub)):]
            elif mode == "strip":
                assert len(args) > 1, "tool:strip need more than two args"
                m = None
                if len(args) > 2:
                    m = args[2]
                src = args[1]
                if m:
                    return src.strip(m)
                return src.strip()
            elif mode == "lstrip":
                assert len(args) > 1, "tool:lstrip need more than two args"
                m = None
                if len(args) > 2:
                    m = args[2]
                src = args[1]
                if m:
                    return src.lstrip(m)
                return src.lstrip()
            elif mode == "rstrip":
                assert len(args) > 1, "tool:rstrip need more than two args"
                m = None
                if len(args) > 2:
                    m = args[2]
                src = args[1]
                if m:
                    return src.rstrip(m)
                return src.rstrip()
        except:
            self.error(traceback.format_exc())
        return ""

    def _processOperate(self, elem, sessionID):
        args = []
        for e in elem[2:]:
            args.append(self._processElement(e, sessionID))
        assert len(args) > 0, "operate need more than one args"
        mode = args[0]
        try:
            if mode == "":
                pass
        except:
            self.error(traceback.format_exc())
        return ""

    def _processLearn(self, elem, sessionID):
        filename = ""
        for e in elem[2:]:
            filename += self._processElement(e, sessionID)
        if not os.path.isfile(filename):
            filename = self.learn_file
        self.learn(filename)
        return ""

    def _respond(self, input_, sessionID):
        if len(input_) == 0:
            return u""

        inputStack = self.getPredicate(self._inputStack, sessionID)
        if len(inputStack) > self._maxRecursionDepth:
            if self._verboseMode:
                err = u"WARNING: maximum recursion depth exceeded (input='%s')" % self._cod.enc(input_)
                sys.stderr.write(err)
            return u""

        inputStack = self.getPredicate(self._inputStack, sessionID)
        inputStack.append(input_)
        self.setPredicate(self._inputStack, inputStack, sessionID)

        subbedInput = self._subbers['normal'].sub(input_)

        outputHistory = self.getPredicate(self._outputHistory, sessionID)
        try: that = outputHistory[-1]
        except IndexError: that = ""
        subbedThat = self._subbers['normal'].sub(that)

        topic = self.getPredicate("topic", sessionID)
        subbedTopic = self._subbers['normal'].sub(topic)

        response = u""
        elem = self._brain.match(subbedInput, subbedThat, subbedTopic)
        matchedHistory = self.getPredicate(self._matchedHistory, sessionID)
        matchedHistory.append(elem)
        while len(matchedHistory) > self._maxHistorySize:
            matchedHistory.pop(0)
        self.setPredicate(self._matchedHistory, matchedHistory, sessionID)
        if elem is None:
            if self._verboseMode:
                err = "WARNING: No match found for input: %s\n" % self._cod.enc(input_)
                sys.stderr.write(err)
        else:
            response += self._processElement(elem, sessionID).strip()
            response += u" "
        response = response.strip()

        inputStack = self.getPredicate(self._inputStack, sessionID)
        inputStack.pop()
        self.setPredicate(self._inputStack, inputStack, sessionID)

        return response

    def chinese_respond(self, input_, sessionID = None):
        """Return the Kernel's response to the input string."""
        if len(input_) == 0:
            return u""
        if not sessionID:
            sessionID = self._globalSessionID
        # Decode the input (assumed to be an encoded string) into a unicode
        # string. Note that if encoding is False, this will be a no-op
        try: input_ = self._cod.dec(input_)
        except UnicodeError: pass
        except AttributeError: pass

        # prevent other threads from stomping all over us.
        self._respondLock.acquire()

        try:
            # Add the session, if it doesn't already exist
            self._addSession(sessionID)

            # split the input into discrete sentences
            sentences = aiml.Utils.sentences(input_)
            finalResponse = u""
            for s in sentences:
                # Add the input to the history list before fetching the
                # response, so that <input/> tags work properly.
                inputHistory = self.getPredicate(self._inputHistory, sessionID)
                inputHistory.append(s)
                while len(inputHistory) > self._maxHistorySize:
                    inputHistory.pop(0)
                self.setPredicate(self._inputHistory, inputHistory, sessionID)

                # Add the extract result to the history list before fetching the
                # response, so that <extract/> tags work properly.
                p = extract(s)
                extractHistory = self.getPredicate(self._extractHistory, sessionID)
                extractHistory.append(p)
                while len(extractHistory) > self._maxHistorySize:
                    extractHistory.pop(0)
                self.setPredicate(self._extractHistory, extractHistory, sessionID)

                # Fetch the response
                response = self._respond(p, sessionID)

                # add the data from this exchange to the history lists
                outputHistory = self.getPredicate(self._outputHistory, sessionID)
                outputHistory.append(response)
                while len(outputHistory) > self._maxHistorySize:
                    outputHistory.pop(0)
                self.setPredicate(self._outputHistory, outputHistory, sessionID)

                # append this response to the final response.
                finalResponse += (response + u"  ")

            finalResponse = finalResponse.strip()
            #print( "@ASSERT", self.getPredicate(self._inputStack, sessionID))
            assert(len(self.getPredicate(self._inputStack, sessionID)) == 0)

            # and return, encoding the string into the I/O encoding
            return self._cod.enc(finalResponse)

        finally:
            # release the lock
            self._respondLock.release()

    @property
    def has_change(self):
        return self.mt_time != os.stat(self.brnfile).st_mtime

    def sync_aiml(self):
        db = shelve.open(self.cache_db, "c", writeback=True)
        rules = []
        for r in db:
            if isinstance(db[r], (list, tuple)):
                ll = utils.xml_tool(li_template.format(pattern=r))
                for item in db[r]:
                    ll.add_children(ll.create_element('li', item), ll.get_element('random'))
                rules.appen(ll.to_string())
            else:
                rules.append(category_template.format(pattern=r, answer=db[r]))
        content = template.format(rules="\n".join(rules))
        with open(self.learn_file, "w") as fp:
            fp.write(content)
            fp.close()

    def init_bot_info(self, sessionid = None, **kwargs):
        for k,v in kwargs.items():
            self.setPredicate(k, v, sessionid)

    def refresh_brain(self):
        if self.has_change:
            self.loadBrain(self.brnfile)

    def clear_cache(self):
        if os.path.isfile(self.cache_db):
            os.remove(self.cache_db)

    def reply(self, msg, sessionid = None):
        return self.chinese_respond(msg, sessionid)

    def run(self, sessionid = None):
        while True:
            ipt = input("you say: ")
            self.refresh_brain()
            print("robot say: %s" % self.reply(ipt, sessionid))

    def xmlrpc_run(self, port=27777, bind='127.0.0.1'):
        from cdspider.libs import WSGIXMLRPCApplication
        from xmlrpc.client import Binary

        application = WSGIXMLRPCApplication()

        def hello():
            result = {"message": "xmlrpc is running"}
            return json.dumps(result)
        application.register_function(hello, 'hello')

        def learn(rule, temp = None):
#            r_obj = utils.__redirection__()
#            sys.stdout = r_obj
            data = broken_exc = output = status = None
            try:
                self.learn(rule, temp)
                status = 200
            except :
                broken_exc = traceback.format_exc()
                status = 500
#            output = sys.stdout.read()
            result = {"status": status, "broken_exc": broken_exc, "stdout": output, 'data': data}

            return json.dumps(result)
        application.register_function(learn, 'learn')

        def reply(msg, sessionID):
#            r_obj = utils.__redirection__()
#            sys.stdout = r_obj
            data = broken_exc = output = status = None
            try:
                data = self.reply(msg, sessionID)
                status = 200
            except :
                broken_exc = traceback.format_exc()
                status = 500
#            output = sys.stdout.read()
            result = {"status": status, "broken_exc": broken_exc, "stdout": output, 'data': data}

            return json.dumps(result)
        application.register_function(reply, 'reply')

        def init(kwargs, sessionID):
#            r_obj = utils.__redirection__()
#            sys.stdout = r_obj
            data = broken_exc = output = status = None
            try:
                data = self.init_bot_info(sessionID=sessionID, **kwargs)
                status = 200
            except :
                broken_exc = traceback.format_exc()
                status = 500
#            output = sys.stdout.read()
            result = {"status": status, "broken_exc": broken_exc, "stdout": output, 'data': data}

            return json.dumps(result)
        application.register_function(init, 'init')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        self.info('AichatRobots.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()
