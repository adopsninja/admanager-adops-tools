#!/usr/bin/env python
import logging
import os

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from constants import TOKEN_URI, TOKEN_EXPIRY, USER_AGENT

from database import Database

logger = logging.getLogger(__name__)


class SpreadsheetManager:
    def __init__(self, email: str, spreadsheet_id: str) -> None:
        self.email = email
        self.spreadsheet_id = spreadsheet_id
        self.service = self.sheets_service()

    def sheets_service(self):
        credentials = Database().get_credentials(self.email)
        return build(
            "sheets",
            "v4",
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

    def read_values(self, sheet_range: str):
        values = (
            self.service
            .spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=sheet_range)
            .execute()
            .get("values", [])
        )
        if not values:
            return None

        return values

    def write_values( self, values: list, sheet_range: str):
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [{
                "values": values,
                "range": sheet_range
            }]
        }
        result = (
            self.service
            .spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
            .execute()
        )
        logger.info("{0} cells updated.".format(result.get("totalUpdatedCells")))
        return result

if __name__ == "__main__":
    spreadsheet = SpreadsheetManager("dariusz.siudak***REMOVED***", "***REMOVED***")
    spreadsheet.read_values("re-approve!A:Z")
