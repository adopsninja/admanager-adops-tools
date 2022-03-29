#!/usr/bin/env python
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

from constants import TODAY, TOKEN_EXPIRY, TOKEN_URI, USER_AGENT
from database import Database

logging.getLogger("googleapiclient").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class NotificationManager:
    def __init__(self, email: str) -> None:
        self.email = email
        self.service = self.mail_service()

    def mail_service(self):
        credentials = Database().get_credentials(self.email)
        return build(
            "gmail",
            "v1",
            credentials=GoogleCredentials(
                credentials.access_token,
                credentials.client_id,
                credentials.client_secret,
                credentials.refresh_token,
                TOKEN_EXPIRY,
                TOKEN_URI,
                USER_AGENT,
            ),
        )

    def mcm_notification_message(self, table):
        if table is None:
            table = "Status domen się nie zmienił."
        html = f"""\
        <html>
            <body>
                <p>Cześć,<br><br>
                poniżej aktualizacja statusów domen Company M:<br>
                <br>
                {table}
                <br>
                <br>
                Możliwe statusy:
                <br>
                <ul>
                    <li>None - domena nie była wcześniej sprawdzana</li>
                    <li>DRAFT - przygotowany szkic domeny</li>
                    <li>UNCHECKED - wysłany do sprawdzenia przez Google</li>
                    <li>APPROVED - zaakceptowany przez Google</li>
                    <li>DISAPPROVED - niezaakceptowany przez Google</li>
                </ul>
                <br>
                <a href='https://docs.google.com/spreadsheets/d/***REMOVED***/' target='_blank'>Spreadsheet: lista domen Company M</a>
                <br>
                <br>
                Pozdrawiam,
                </p>
            </body>
        </html>"""

        return html

    def create_message(self, to, sender, subject, message_body):
        message = MIMEMultipart("alternative")
        message.attach(
            MIMEText(message_body, "html")
        )
        message["to"] = to
        message["from"] = sender
        message["subject"] = TODAY + " - " + subject
        logger.info(f"Created message from: '{sender}' to: '{to}' with subject: '{message['subject']}'.")

        return {
            "raw": base64.urlsafe_b64encode(
                message
                .as_string()
                .encode()
                ).decode()
            }

    def send_message(self, message):
        try:
            message = self.service.users().messages().send(userId="me", body=message).execute()
            logger.info("Message sent.")
            return message
        except HttpError as error:
            logger.error(f"An error occurred: {error}")
