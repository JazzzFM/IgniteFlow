# -*- coding: utf-8 -*-
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import glob
from typing import List, NoReturn
import smtplib

class EmailSender:
    def __init__(self, sender_soeid):
        self.email = f"{sender_soeid}@imcla.lac.nsroot.net"

    def send(self, receivers_soeid, subject, html, file = None):
        """
        This function sends an email from the sender account to the
        receivers.

        ## Parameters
        ----------
            receivers_soeid: List
                List containing the SOEID's of the persons that must
                receive the email
            subject: string
                The subject of the email
            message: string
                Message that the email will contain
            file: string
                Optional path that should be sent
        """
        receivers = [f"{soeid}@imcla.lac.nsroot.net" for soeid in receivers_soeid]

        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = ", ".join(receivers)
        msg['Subject'] = subject

        part_t = MIMEText(html, "html", "utf-8")
        msg.attach(part_t)

        if file:
            filename = os.path.basename(file)
            attachment = open(file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)

        server = smtplib.SMTP('localhost')
        text = msg.as_string()
        server.sendmail(self.email, receivers, text)
        server.quit()
