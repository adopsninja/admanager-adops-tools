#!/usr/bin/env python
import logging

import pandas as pd
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from constants import TOKEN_EXPIRY, TOKEN_URI, USER_AGENT
from database import Database

logger = logging.getLogger(__name__)


class SpreadsheetManager:
    def __init__(self, email: str, spreadsheet_id: str, range_name: str) -> None:
        self.email = email
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
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

    def read_values(self):
        values = (
            self.service
            .spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=self.range_name)
            .execute()
            .get("values", [])
        )
        if not values:
            return None

        return values

    def write_values(self, values: list):
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [{
                "values": values,
                "range": self.range_name
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
    def __init__(self, spreadsheet_manager: SpreadsheetManager) -> None:
        self.spreadsheet_manager = spreadsheet_manager
        self.values = self.spreadsheet_manager.read_values()

    def build_dataframe(self) -> pd.DataFrame:
        dataframe = pd.DataFrame(self.values)
        dataframe.rename(columns=dataframe.iloc[0], inplace=True)
        dataframe = dataframe[1:]
        dataframe.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return dataframe

    def valid_publishers(self, dataframe: pd.DataFrame, exists: bool) -> list:
        necessary_fields = (
            (dataframe["publisher.name"] != "") &
            (dataframe["publisher.email"] != "")
        )
        dataframe = dataframe.loc[necessary_fields]
        if not exists:
            dataframe = dataframe.loc[
                (dataframe["publisher.status"] == "") &
                (dataframe["publisher.accountStatus"] == "")
            ]

        return (
            dataframe[["publisher.name", "publisher.email", "publisher.networkCode"]]
            .drop_duplicates(subset="publisher.name")
            .to_dict("records")
        )

    def update_publishers(self, dataframe: pd.DataFrame, publishers: list) -> pd.DataFrame:
        if publishers:
            for publisher in publishers:
                condition = dataframe["publisher.name"] == publisher["name"]
                dataframe.loc[condition, "publisher.status"] = publisher["childPublisher"]["status"]
                dataframe.loc[condition, "publisher.accountStatus"] = publisher["childPublisher"]["accountStatus"]
                dataframe.loc[condition, "publisher.networkCode"] = publisher["childPublisher"]["childNetworkCode"]

        return dataframe

    def valid_sites(self, dataframe: pd.DataFrame, exists: bool) -> list:
        necessary_fields = (
            (dataframe["publisher.accountStatus"] == "APPROVED") &
            (dataframe["publisher.status"] == "APPROVED")
        )
        dataframe = dataframe.loc[necessary_fields]
        if not exists:
            condition = dataframe["site.approvalStatus"].isin(["", None])
            dataframe = dataframe.loc[condition]

        return (
            dataframe[["publisher.networkCode", "site.url"]]
            .to_dict("records"))

    def update_sites(self, dataframe: pd.DataFrame, sites: list) -> pd.DataFrame:
        if sites:
            for site in sites:
                condition = (
                    (dataframe["site.url"] == site["url"]) &
                    (dataframe["publisher.networkCode"] == site["childNetworkCode"])
                )
                dataframe.loc[condition, "site.approvalStatus"] = site["approvalStatus"]
        
        return dataframe
        
    def dataframe_to_list(self, dataframe: pd.DataFrame) -> list:
        return [dataframe.columns.values.tolist()] + dataframe.values.tolist()

    def refresh_values(self):
        self.values = self.spreadsheet_manager.read_values()
