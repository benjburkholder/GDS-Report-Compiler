# *-* coding:utf-8 *-*
"""
@title: Email Notification Manager
@author: jschroeder
@status: Production
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Gmail-based module for script runtime email notifications.
Useful for failures, audits and confirmations.
"""
import os
import base64
import googleapiclient.discovery
from httplib2 import Http
from oauth2client import file as oauth_file, client, tools
from email.mime.text import MIMEText


class EmailClient:
    """
    Interface for sending emails dynamically
    """

    def __init__(self):
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.sender = 'linkmedia360.reporting@gmail.com'
        self._scopes = [
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/gmail.compose',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        self.service = self._build_service()

    def _build_service(self):
        store = oauth_file.Storage(
            os.path.join(self.path, 'secrets', 'gmail_secrets.json')
        )
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(
                os.path.join(self.path, 'secrets', 'GmailOAuth.json'),
                self._scopes)
            credentials = tools.run_flow(flow, store)
        return googleapiclient.discovery.build('gmail', 'v1', http=credentials.authorize(Http()))

    def create_message(self, to, subject, html):
        """
        Create and return a b64 encoded email object
        """
        message = MIMEText(html, 'html')
        message['to'] = to
        message['from'] = self.sender
        message['subject'] = subject
        return {'raw': base64.urlsafe_b64encode(bytes(message.as_string(), "utf-8")).decode("utf-8")}


    def send_error_email(self, to, script_name, error, client):
        """
        No attachment email with html template for errors
        """
        recipients = ', '.join(to)
        html = """
        <!DOCTYPE html>
            <html>
            <body>
                <h2>{client}: Error Notification</h2>
                <p><i>There has been an error, please see traceback below:</i></p>
                <hr>
                <p>{script_name}</p>
                <br>
                <p>{error}</p>
                </body>
            </html>
        """.format(
            script_name=script_name,
            client=client,
            error=error
        )
        subject = "Error: Uncaught Exception in {script_name} || {client}".format(script_name=script_name, client=client)
        message = self.create_message(to=recipients, subject=subject, html=html)
        message = (self.service.users().messages().send(userId='me', body=message)
                   .execute())
        print('Message Id: {}'.format(message['id']))
        return message

    def send_notification(self, to, html, subject):
        """
        No attachment email with template for general notifications
        """
        recipients = ', '.join(to)
        message = self.create_message(to=recipients, subject=subject, html=html)
        message = (self.service.users().messages().send(userId='me', body=message)
                   .execute())
        print('Message Id: {}'.format(message['id']))
        return message