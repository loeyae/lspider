#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-25 10:54:35
"""
import os
import traceback
import ssl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from cdspider.mailer import BaseSender
from cdspider.exceptions import *

class SmtpSender(BaseSender):
    """
    mail sender
    """

    def __init__(self, *args, **kwargs):
        super(SmtpSender, self).__init__(*args, **kwargs)
        self.mailer = None
        self._initSMTP()

    def __del__(self):
        if self.mailer:
            self.mailer.quit()

    def _initSMTP(self):
        try:
            assert 'host' in self.sender and self.sender['host'], "invalid sender setting: host"
            assert 'port' in self.sender and self.sender['port'], "invalid sender setting: port"
            assert 'from' in self.sender and self.sender['from'], "invalid sender setting: from"


            if 'secure' in self.sender and self.sender['secure'] == 'ssl':
                context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                context.set_default_verify_paths()
                context.verify_mode = ssl.CERT_REQUIRED
                self.mailer = smtplib.SMTP_SSL(host = self.sender['host'], port = self.sender['port'], context=context)
            else:
                self.mailer = smtplib.SMTP(host = self.sender['host'], port = self.sender['port'])
                code = self.mailer.ehlo()[0]
                uses_esmtp = (200 <= code <= 299)
                if not uses_esmtp:
                    code = connection.helo()[0]
                if not (200 <= code <= 299):
                    raise Exception("Remove server refused HELO; code: %s" % code)
                if self.sender.get('secure') == 'tls' and uses_esmtp and self.mailer.has_extn('starttls'):
                    self.mailer.starttls()
                    code = self.mailer.ehlo()[0]
                    if not (200 <= code <= 299):
                        raise Exception("Couldn't EHLO after STARTTLS")
                    self.logger.info("Using TLS connection.")
                else:
                    self.logger.info("Server does not support TLS; using normal connection.")
            if 'user' in self.sender and self.sender['user']:
                assert 'pass' in self.sender and self.sender['pass'], "invalid sender setting: pass"
                self.mailer.login(self.sender['user'], self.sender['pass'])
        except:
            self.logger.error(traceback.format_exc())

    def send(self, subject, message, type = "plain", to_list = None, attachment=None):
        if not self.mailer:
            raise CDSpiderMailerError("Mailer not Exists")
        to_list = to_list or self.receiver
        if attachment:
            mail = MIMEMultipart('related')
            msg = MIMEText(str(message), type, 'utf-8')
            mail.attach(msg)
            if not isinstance(attachment, list):
                attachment = [attachment]
            for item in attachment:
                att = MIMEApplication(open(item, 'rb').read())
                att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(item))
                mail.attach(att)
        else:
            mail = MIMEText(str(message), type, 'utf-8')
        mail['Subject'] = Header(subject, 'utf-8')
        if 'from_name' in self.sender and self.sender['from_name']:
            f = Header(self.sender['from_name'], 'utf-8')
            f.append("<"+ self.sender['from'] +">", 'utf-8')
            mail['From'] = f
        if isinstance(to_list, list):
            mt = []
            tt = []
            for item in to_list:
                if isinstance(item, dict):
                    assert 'name' in item and item['name'], "invalid receiver setting: name"
                    assert 'mail' in item and item['mail'], "invalid receiver setting: mail"
                    t = Header(item['name'], 'utf-8')
                    t.append("<"+ item['mail'] +">", 'utf-8')
                    mt.append(item['mail'])
                    tt.append(str(t))
                elif isinstance(item, list):
                    if len(item) > 1:
                        t = Header(item[1], 'utf-8')
                        t.append("<"+ item[0] +">", 'utf-8')
                        mt.append(item[0])
                        tt.append(str(t))
                    else:
                        t = Header(item[0], 'utf-8')
                        mt.append(item[0])
                        tt.append(str(t))
                else:
                    t = Header(item, 'utf-8')
                    mt.append(item)
                    tt.append(str(t))
            mail['To'] = ",".join(tt)
            self.mailer.sendmail(self.sender['from'], mt, mail.as_string())
        elif isinstance(to_list, dict):
            assert 'name' in to_list and to_list['name'], "invalid receiver setting: name"
            assert 'mail' in to_list and to_list['mail'], "invalid receiver setting: mail"
            t = Header(to_list['name'], 'utf-8')
            t.append("<"+ to_list['mail'] +">", 'utf-8')
            mail['To'] = t
            self.mailer.sendmail(self.sender['from'], to_list['mail'], mail.as_string())
        else:
            t = Header(to_list, 'utf-8')
            mail['To'] = t
            self.mailer.sendmail(self.sender['from'], to_list, mail.as_string())
