"""Tools for monitor.
"""
# Author: Donald Cheung <jianzhang9102@gmail.com>

import base64
import subprocess

class Monitor(object):

    @staticmethod
    def mail(receivers, subject, content, encoding='utf-8'):
        """Send mail.

        Parameters
        ----------
        receivers: string/list/tuple/set/dict
            Emails of receivers.

        subject: string
            Email subject.

        content: string
            Email content.

        encoding: string, optional
            Encoding of email subject and content.

        Returns
        -------
        returncode: integer
            Exit code of sending email.

        """
        if isinstance(receivers, str):
            receiver_emails = receivers.replace(",", " ")
        elif isinstance(receivers, (list, tuple, set, dict)):
            receiver_emails = " ".join(receivers)
        else:
            raise ValueError("type of {0} does not supported".format(receivers))

        subject = "{0}\nContent-Transfer-Encoding: base64\n" \
                        "Content-Type: text/html;charset={1}".format(subject, encoding)

        encoded_content = base64.encodestring(content)
        proc = subprocess.Popen(["mail", "-s", subject, receiver_emails], 
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        proc.communicate(input=encoded_content)
        return proc.returncode

