"""
Gmail Customizer Variant

Platform Customizer - Used for critical system operations
"""
# STANDARD IMPORTS
import base64
import json
from email.mime.text import MIMEText

# INSTALLED IMPORTS
import apiclient
import sqlalchemy
from httplib2 import Http
from oauth2client import client

# LOCAL IMPORTS
from ..core import Customizer


def send_error_email(
        client_name: str,
        script_name: str,
        to: list,
        error: Exception,
        stack_trace: str,
        engine: sqlalchemy.engine
) -> None:
    """
    Imperative wrapper for the OO GmailCustomizer send_error_email workflow
    ====================================================================================================
    :param client_name:
    :param script_name:
    :param to:
    :param error:
    :param stack_trace:
    :param engine:
    :return:
    """
    GmailCustomizer(application_engine=engine).send_error_email(
        client_name=client_name,
        script_name=script_name,
        to=to,
        error=error,
        stack_trace=stack_trace
    )
    return


def get_recipients_from_list(to: list) -> str:
    """
    Combines list of emails into a MIME-compliant string recipient list
    ====================================================================================================
    :param to:
    :return:
    """
    return ', '.join(to)


def get_error_email_html(client_name: str, script_name: str, error: str, stack_trace: str) -> str:
    """
    Generates the error email template's html given the client, script, error and stack trace from the
    call site of the error
    ====================================================================================================
    :param client_name:
    :param script_name:
    :param error:
    :param stack_trace:
    :return:
    """
    return f"""
    <!DOCTYPE html>
        <html>
        <body>
            <h2>{client_name}: Error Notification</h2>
            <p><i>There has been an error, please see traceback below:</i></p>
            <hr>
            <p>{script_name}</p>
            <br>
            <p>{error}</p>
            <br>
            <p>{stack_trace}</p>
            </body>
        </html>
    """


def get_error_email_subject_line(
        script_name: str,
        client_name: str
) -> str:
    """
    Returns the error email template's subject line given client and script names
    ====================================================================================================
    :param script_name:
    :param client_name:
    :return:
    """
    return f"Error: Uncaught Exception in {script_name} || {client_name}"


class GmailCustomizer(Customizer):
    """
    Customizer Instance for Sending Email Alerts and Notifications
    ====================================================================================================
    Use this Customizer instance for alerts regarding errors or other non-standard platform behavior

    :type: Customizer
    """

    credential_name = 'GmailSecrets'

    sender = 'linkmedia360.reporting@gmail.com'

    scopes = [
        'https://www.googleapis.com/auth/gmail.modify',
        'https://www.googleapis.com/auth/gmail.compose',
        'https://www.googleapis.com/auth/gmail.send'
    ]

    api_name = 'gmail'
    api_version = 'v1'

    def __init__(self, application_engine: sqlalchemy.engine):
        """
        GmailCustomizer initializer
        ====================================================================================================
        Provide an application_engine that points at the GRC global application database

        :param application_engine:
        """
        super().__init__()
        self.application_engine = application_engine
        self.service = self._build_service()

    def _build_service(self) -> apiclient.discovery.Resource:
        """
        Build and return a Google API Resource object ready to interact with the Gmail API
        ====================================================================================================
        :return:
        """
        self.get_secrets(include_dat=False)
        credentials = client.Credentials.new_from_json(
            json.dumps(
                self.secrets
            )
        )
        return apiclient.discovery.build(
            self.api_name,
            self.api_version,
            http=credentials.authorize(Http())
        )

    def create_message(self, to: str, subject: str, html: str) -> dict:
        """
        Create and return a b64 encoded email object
        ====================================================================================================
        """
        message = MIMEText(html, 'html')
        message['to'] = to
        message['from'] = self.sender
        message['subject'] = subject
        return {
            'raw': base64.urlsafe_b64encode(
                bytes(
                    message.as_string(),
                    "utf-8"
                )
            ).decode("utf-8")
        }

    def send_error_email(self, to: list, script_name: str, error: Exception, stack_trace: str, client_name: str):
        """
        No attachment email with html template for errors
        ====================================================================================================
        """
        error = str(error)  # normalize into string format for use in email notifications
        recipients = get_recipients_from_list(to=to)
        html = get_error_email_html(
            client_name=client_name,
            script_name=script_name,
            error=str(error),
            stack_trace=stack_trace
        )
        subject = get_error_email_subject_line(
            client_name=client_name,
            script_name=script_name
        )
        message = self.create_message(
            to=recipients,
            subject=subject,
            html=html
        )
        # noinspection PyUnresolvedReferences
        message = self.service.users().messages().send(userId='me', body=message).execute()
        print('Message Id: {}'.format(message['id']))
        return message

    def send_notification(self, to: list, html: str, subject: str) -> None:
        """
        No attachment email with template for general notifications
        ====================================================================================================
        """
        recipients = get_recipients_from_list(
            to=to
        )
        message = self.create_message(
            to=recipients,
            subject=subject,
            html=html
        )
        # noinspection PyUnresolvedReferences
        message = self.service.users().messages().send(userId='me', body=message).execute()
        print('Message Id: {}'.format(message['id']))
        return
