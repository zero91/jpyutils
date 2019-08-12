import smtplib
import random
import ssl
import re
import operator
import email
import email.mime.text
import email.mime.multipart
import mimetypes

_email_server = {
  "smtp": {
    'qq.com': ('smtp.qq.com', (465, 587)),
    'yeah.net': ('smtp.yeah.net', (465,)), #25, 994
    '163.com': ('smtp.163.com', (465,)), #25, 994
    '126.com': ('smtp.126.com', (465,)), #25, 994
    'sina.com': ('smtp.sina.com', (465,)),
    'sohu.com': ('smtp.sohu.com', (465,)),
    'jiehuozhe.com': ('smtp.exmail.qq.com', (465,)),
    'apple.com': ('mail.apple.com', (25,)),
  },
}

def _format_addr(name, addr):
  return email.utils.formataddr(
    (email.header.Header(name, 'utf-8').encode(), addr))


def is_email(email):
  return re.match(
    '^[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+$', email)


def register_server(protocol, domain, host, ports, overwrite=False):
  """Register email server.

  Parameters
  ----------
  protocol: str
    Email protocol, current supported protocol includes 'smtp'.

  domain: str
    The domain of the server, equal to the suffix of email address.

  host: str
    The server address.

  ports: list, tuple, int
    The ports of the host.

  overwrite: bool
    Overwrite the server info if it is already exists.

  """
  if protocol not in _email_server:
    raise ValueError("Unsupported protocol '%s'" % (protocol))

  if domain in _email_server[protocol] and overwrite is False:
    logging.info("Mail domain '%s' for protocol '%s' is already exists",
      domain, protocol)
    return

  if not isinstance(ports, (list, tuple)):
    ports = [ports]
  _email_server[protocol][domain] = (host, ports)


class SMTPClient(object):
  """Create an SMTP client.

  Parameters
  ----------
  sender: str
    The name of the sender.

  sender_email: str
    The email of the sender.

  password: str
    The password of the sender.

  """
  def __init__(self, sender, sender_email, password):
    self._m_sender = sender
    self._m_sender_email = sender_email
    self._m_sender_addr = _format_addr(self._m_sender, self._m_sender_email)

    host, ports = _email_server['smtp'][self._m_sender_email.split('@')[1]]
    port = random.choice(ports)
    try:
      self._m_client = smtplib.SMTP_SSL(host, port)
    except ssl.SSLError as e:
      self._m_client = smtplib.SMTP(host, port)
      self._m_client.ehlo()
      self._m_client.starttls()
      self._m_client.ehlo()
    self._m_client.login(self._m_sender_email, password)

  def __del__(self):
    self._m_client.close()

  def send(self, to_addrs, subject, content, attachments=None,
                                             content_type='plain'):
    """Send an email.

    Parameters
    ----------
    to_addrs: list or str
      A list of receive emails.
      Format as: [(name1, email1), (name2, email2), ...]

    subject: str
      The subject of the email.

    content: str
      The content of the email.

    attachments: list
      A list of attachments.
      Every attachment have an ID equal to list index,
      which can be refer to in the content.

    content_type: str
      'plain' or 'html'.

    Returns
    -------
    mail_num: integer
      The number of receivers.

    """
    msg = email.mime.multipart.MIMEMultipart()
    msg["Subject"] = email.header.Header(subject, charset='utf-8').encode()
    msg["From"] = self._m_sender_addr

    if isinstance(to_addrs, str):
      to_addrs = [to_addrs]

    to_addr_list = []
    for addr in to_addrs:
      if isinstance(addr, str):
        nickname = addr.split('@')[0]
      else:
        nickname = addr[0]
        addr = addr[1]
      to_addr_list.append((nickname, addr))
    msg["To"] = ",".join(map(_format_addr, *zip(*to_addr_list)))

    msg.attach(email.mime.text.MIMEText(content, content_type, 'utf-8'))

    # add attachments
    if attachments is None:
      attachments = []
    for idx, attachment in enumerate(attachments):
      ctype, encoding = mimetypes.guess_type(attachment)
      if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
      maintype, subtype = ctype.split('/', 1)
      with open(attachment, 'rb') as fin:
        att = email.mime.base.MIMEBase(maintype, subtype)
        att.set_payload(fin.read())
        email.encoders.encode_base64(att)
        filename = email.header.Header(fin.name).encode()
        att.add_header('Content-Disposition', 'attachment', filename=filename)
        att.add_header('Content-ID', '<%d>' % (idx))
        msg.attach(att)

    self._m_client.sendmail(
      self._m_sender_email,
      list(map(operator.itemgetter(1), to_addr_list)),
      msg.as_string())
    return len(to_addr_list)

