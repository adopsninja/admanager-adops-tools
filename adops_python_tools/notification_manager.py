#!/usr/bin/env python
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import PurePath

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader
from constants import (MCM_MANAGER_PATH, REPORT_MANAGER_PATH, TODAY,
                       TOKEN_EXPIRY, TOKEN_URI, USER_AGENT)
from database import Database
from mcm_manager import MultipleCustomerManagement
from report_manager import (ReportManager, dataframe_to_html,
                            process_adx_fillrate_report)
from spreadsheet_manager import SpreadsheetDataframe, SpreadsheetManager

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

    def adx_fillrate_message(self, table):
        if table is None:
            table = "Status domen się nie zmienił."
        html = f"""\
        <html>
            <body>
                <p>Cześć,<br><br>
                poniżej raport wypełnienia ADX:<br>
                <br>
                {table}
                <br>
                <br>
                Pozdrawiam,
                </p>
            </body>
        </html>"""

        return html

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

def mox_mcm_status_update(env="test"):
    config = ConfigReader(MCM_MANAGER_PATH).read_yaml_config()
    logger.info("YAML configuration loaded.")
    ad_manager = AdOpsAdManagerClient(
        config[env]["email"],
        config[env]["networkCode"]
    )
    logger.info("AdOpsAdManagerClient loaded.")
    spreadsheet_manager = SpreadsheetManager(
        config[env]["email"],
        config[env]["spreadsheetId"],
        config[env]["sheetRange"]
    )
    spreadsheet_dataframe = SpreadsheetDataframe(spreadsheet_manager)
    logger.info("SpreadsheetManager and SpreadsheetDataframe loaded.")
    mcm_manager = MultipleCustomerManagement(ad_manager, spreadsheet_dataframe)
    logger.info("MultipleCustomerManagement loaded.")
    status_dataframe = mcm_manager.status_change()
    status_table = spreadsheet_dataframe.dataframe_to_html(status_dataframe)
    notification_manager = NotificationManager(config[env]["email"])
    logger.info("NotificationManager loaded.")
    message = notification_manager.mcm_notification_message(status_table)
    message = notification_manager.create_message(
        config[env]["notification"]["to"],
        config[env]["notification"]["sender"],
        config[env]["notification"]["subject"],
        message
    )
    notification_manager.send_message(message)

def adx_fillrate_notification():
    admanager_client = AdOpsAdManagerClient("dariusz.siudak***REMOVED***", "***REMOVED***")
    report_path = ReportManager(REPORT_MANAGER_PATH).get_report(admanager_client, "adxFillRateNotification")
    dataframe = process_adx_fillrate_report(PurePath(report_path))
    html_table = dataframe_to_html(dataframe)
    notification_manager = NotificationManager("dariusz.siudak***REMOVED***")
    message = notification_manager.adx_fillrate_message(html_table)
    message = notification_manager.create_message("dariusz.siudak***REMOVED***, ***REMOVED***", "dariusz.siudak***REMOVED***", "ADX fillrate GAM Company Y (***REMOVED***)", message)
    notification_manager.send_message(message)
