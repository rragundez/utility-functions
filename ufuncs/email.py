import logging
import smtplib

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def _add_attachment(message, attachment, filetype=None, filename=None):
    """Add attachment to email message.

    Args:
        message (MIMEMultipart): Message object to add attachmetn to.
        attachment (str|bytes): Local path or content as bytes of the file to
            be attached.
        filetype (str): File type of the attachment sent.
        filename (str): Send attachment with this filename.

    Returns:
        MIMEMultipart: Message with the attachment file added to it.
    """
    if not isinstance(attachment, (str, bytes)):
        raise TypeError("attachment expected as str or bytes.")

    if isinstance(attachment, str):
        if not Path(attachment).is_file():
            raise FileNotFoundError("Filepath '{}'' is not a file or "
                                    "doesn't exist".format(attachment))
        else:
            filename = filename if filename else Path(attachment).name
            filetype = filetype if filetype else Path(filename).suffix
            # extract bytes
            with open(attachment, 'rb') as file:
                attachment = file.read()

    if isinstance(attachment, bytes):
        if not (filename and filetype):
            raise TypeError("Missing filename or filetype arguments")

    att = MIMEApplication(attachment, _subtype=filetype)
    att.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(att)
    return message


def send_email(from_email, to_email, smtp_server, port,
               username, password, message, subject=None,
               attachment=None, filetype=None, filename=None):
    """Send email with optional attachtments

    Args:
        from_email (str): Email address to send email from.
        to_email (str): Email address to send email to.
        smtp_server (str): SMTP server to deliver email.
        port (int): Port to use in the SMTP server.
        username (str): Username for authentication in SMTP server.
        password (str): Password for authentication in SMTP server.
        subject (str): Subject of the email.
        message (str): Message body of the email.
        attachment (str|bytes): Local path or content as bytes of the
            file to be attached.
        filetype (str): File type of the attachment sent.
        filename (str): Send attachment with this filename.
    """

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    if attachment:
        msg = _add_attachment(msg, attachment, filetype, filename)

    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls()
        server.login(username, password)
        server.sendmail(from_email, to_email, msg.as_string())
