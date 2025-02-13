#!/usr/bin/env python
import logging

import pandas as pd
import pandas.io.formats.style
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from constants import TOKEN_EXPIRY, TOKEN_URI, USER_AGENT
from database import Database

logging.getLogger("googleapiclient").setLevel(logging.ERROR)
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
        dataframe.fillna("", inplace=True)
        dataframe.rename(columns=dataframe.iloc[0], inplace=True)  # type: ignore
        dataframe = dataframe[1:]
        dataframe["site.url"] = dataframe["site.url"].str.lower()
        for column in dataframe.columns:
            dataframe[column] = dataframe[column].str.strip()

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

    def site_status(self):
        dataframe = self.build_dataframe()[["site.url", "site.approvalStatus"]]
        dataframe.set_index("site.url", inplace=True)
        
        return dataframe

    def compare_site_statuses(self, dataframe_before, dataframe_after):
        result = dataframe_before.compare(dataframe_after)
        result = result.droplevel(level=0, axis=1)
        result = result.reset_index().drop_duplicates().reset_index(drop=True)
        result.index += 1
        result.rename(
            columns={
                "self": "site.approvalStatus.before",
                "other": "site.approvalStatus.after"
            }, inplace=True)

        return result

    def dataframe_to_html(self, dataframe):
        if dataframe is None:
            return None
        result = """<html><head><style>
        h2 {text-align: center;font-family: Helvetica, Arial, sans-serif;}
        table, th, td {border: 1px solid black;border-collapse: collapse;}
        th, td {padding: 1px;text-align: left;font-family: Helvetica, Arial, sans-serif;font-size: 90%;}
        table tbody tr:hover {background-color: #dddddd;}
        .wide {width: 35%;}
        </style></head><body>"""

        if type(dataframe) == pandas.io.formats.style.Styler:
            result += dataframe.render()
        else:
            result += dataframe.to_html(classes="wide", escape=False)
        result += """</body></html>"""

        return result.replace("\n", "")
