#!/usr/bin/env python
import logging
import os
from datetime import datetime

import pandas as pd
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from constants import TOKEN_EXPIRY, TOKEN_URI, USER_AGENT
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


class SpreadsheetDataframe:
    def __init__(self, values: list) -> None:
        self.values = values

    def build_dataframe(self) -> pd.DataFrame:
        dataframe = pd.DataFrame(self.values)
        dataframe.rename(columns=dataframe.iloc[0], inplace=True)
        dataframe = dataframe[1:]
        dataframe.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return dataframe

    def valid_publishers(self, existing: bool) -> list:
        dataframe = self.build_dataframe()
        necessary_fields = (
            (dataframe["publisher.name"] != "") &
            (dataframe["publisher.email"] != "")
        )
        dataframe = dataframe.loc[necessary_fields]
        if not existing:
            dataframe = dataframe.loc[
                (dataframe["publisher.status"] == "") &
                (dataframe["publisher.accountStatus"] == "")
            ]
        valid_publishers = (
            dataframe[["publisher.name", "publisher.email", "publisher.networkCode"]]
            .drop_duplicates(subset="publisher.name")
            .to_dict("records")
        )

        return valid_publishers

    def update_publishers(self, dataframe: pd.DataFrame, publishers: list) -> pd.DataFrame:
        if publishers:
            for publisher in publishers:
                condition = dataframe["publisher.name"] == publisher["name"]
                dataframe.loc[condition, "publisher.status"] = publisher["childPublisher"]["status"]
                dataframe.loc[condition, "publisher.accountStatus"] = publisher["childPublisher"]["accountStatus"]
                dataframe.loc[condition, "publisher.networkCode"] = publisher["childPublisher"]["childNetworkCode"]

        return dataframe
        
    def dataframe_to_list(self, dataframe: pd.DataFrame) -> list:
        return [dataframe.columns.values.tolist()] + dataframe.values.tolist()


if __name__ == "__main__":
    spreadsheet = SpreadsheetManager("dariusz.siudak***REMOVED***", "***REMOVED***")
    val = spreadsheet.read_values("re-approve!A:Z")
    spreadsheet_df = SpreadsheetDataframe(val)
    print(spreadsheet_df.valid_publishers(True))
