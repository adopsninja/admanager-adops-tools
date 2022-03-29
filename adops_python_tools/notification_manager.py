#!/usr/bin/env python
import logging

import pandas as pd
from googleapiclient.discovery import build
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

    def message_body(self):
        pass
    
    def create_message(self):
        pass

    def send_message(self):
        pass
