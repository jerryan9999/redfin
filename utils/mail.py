# /usr/bin/python3
# -*- coding:utf-8

from email.mime.text import MIMEText
from email.header import Header
from email.utils import parseaddr, formataddr
import smtplib
import yaml

def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name,'utf-8').encode(), addr))

with open("config.yml") as f:
  config = yaml.load(f)

# Sender's Email Account
smtp_server = "smtp.gmail.com"
from_addr = config['email']['email_account']
password = config['email']['email_password']

# Receiver's Email Account
to_addr = config['email']['to_addr']

def send_email(content,subject):
  msg = MIMEText("<h3 style='text-align:center'>{}</h3> \n{}".format(subject,content),'html', 'utf-8')
  msg['Subject'] = Header(subject,'utf-8').encode()    # Scraper Daily Report
  msg['From'] = _format_addr('Amazon Server <%s>' % from_addr)
  msg['To'] = _format_addr('Administrator <%s>' % to_addr)

  # Mail Transfer Protocol
  server =smtplib.SMTP(smtp_server,587)   # SMTP 协议默认端口
  server.ehlo()
  server.starttls()
  server.set_debuglevel(1)
  server.login(from_addr,password)
  server.sendmail(from_addr, to_addr, msg.as_string())
  server.quit()
