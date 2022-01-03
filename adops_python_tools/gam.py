#!/usr/bin/env python
import datetime
import logging
import os
import tempfile
from pathlib import PurePath

import yaml
from googleads import errors
from googleads.ad_manager import AdManagerClient, StatementBuilder
from googleads.oauth2 import GoogleRefreshTokenClient

from constants import API_VERSION, PLACEMENT_MANAGER_PATH, REPORT_MANAGER_PATH
from database import Database

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

class AdOpsTools:
    def __init__(self) -> None:
        self.admanager = None
        # report_manager = ReportManager()


class AdOpsAdManagerClient:
    _API_VERSION = API_VERSION

    def __init__(self, application_name, client_id, client_secret, email, refresh_token, network_code=None) -> None:
        self.refresh_token_client = GoogleRefreshTokenClient(client_id, client_secret, refresh_token)
        self.client = AdManagerClient(self.refresh_token_client, application_name, network_code)
        self.network_service = self.client.GetService("NetworkService", version=self._API_VERSION)
        self.placement_service = self.client.GetService("PlacementService", version=self._API_VERSION)
        self.report_downloader = self.client.GetDataDownloader(version=self._API_VERSION)
        self.currency = self.network_service.getCurrentNetwork()["currencyCode"]

    def main(self) -> None:
        print(self.network_service.getAllNetworks())


class PlacementManager:
    _PLACEMENT_MANAGER_PATH = PLACEMENT_MANAGER_PATH
    def __init__(self, client=None) -> None:
        self.client = client
        self.config = ConfigReader(self._PLACEMENT_MANAGER_PATH).read_config()


class ReportManager:
    _REPORT_MANAGER_PATH = REPORT_MANAGER_PATH
    def __init__(self, client=None) -> None:
        self.config = ConfigReader(self._REPORT_MANAGER_PATH).read_config()
        self.client = self.implement_client(client)

        self.update_config()

    def set_report_job(self, adops_job="placement") -> dict:
        return {
            "reportQuery": {
                "statement": self.config["report"][adops_job]["statement"],
                "dimensions": self.config["report"][adops_job]["dimensions"],
                "columns": self.config["report"][adops_job]["columns"],
                "adUnitView": "FLAT",
                "dateRangeType": "CUSTOM_DATE",
                "startDate": self.config["report"][adops_job]["startDate"],
                "endDate": self.config["report"][adops_job]["endDate"],
                "timeZoneType": self.config["report"][adops_job]["timeZoneType"],
            }
        }

    def get_report(self, adops_job="placement"):
        output_path = PurePath(
            self.config["report"]["outputFolderPath"], 
            datetime.datetime.now().strftime("%d%m%Y_%H%M")
        )
        self.create_directory(output_path)
        report_path = PurePath(
            output_path, 
            f"{self.config['report'][adops_job]['networkCode']}_{self.client.currency}_{self.config['report'][adops_job]['reportType']}_{self.config['report'][adops_job]['startDate']}_{self.config['report'][adops_job]['endDate']}.csv.gz"
        )
        report_job = self.set_report_job(adops_job)

        try:
            report_job_id = self.client.report_downloader.WaitForReport(report_job)
            with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False, dir=PurePath(output_path),) as report_file:
                self.client.report_downloader.DownloadReportToFile(report_job_id, "CSV_DUMP", report_file)
                temp_file_path = report_file.name
            try:
                os.rename(temp_file_path, report_path)
            except FileExistsError:
                logger.info(f"Report already exist. Deleting old report {report_path}")
                os.remove(report_path)
                os.rename(temp_file_path, report_path)
            finally:
                logger.info(f"Report job with id {report_job_id} downloaded to: {report_path}")
        except errors.AdManagerReportError as e:
            logger.error(f"Failed to generate report. Error was: {e}")

        return report_path


    def update_config(self) -> None:
        if not all(key in self.config["report"]["placement"] for key in ["startDate", "endDate"]):
            date_range = {
                "startDate": datetime.date.today() - datetime.timedelta(30),
                "endDate": datetime.date.today() - datetime.timedelta(1),
            }
            self.config["report"]["placement"].update(date_range)

    def implement_client(self, client):
        if not client:
            credentials = Database().get_credentials(self.config["report"]["placement"]["email"])
            return AdOpsAdManagerClient(*credentials, self.config["report"]["placement"]["networkCode"])
        else:
            return client

    @staticmethod
    def create_directory(directory) -> str:
        if not os.path.exists(directory):
            logger.info(f"Path {directory} does not exist. Trying to create.")
            os.makedirs(directory)
            return directory

    def __str__(self) -> str:
        return f"\nConfiguration: {self.config}\n\nCurrent network: {self.client.network_service.getCurrentNetwork()}"


class ConfigReader:
    def __init__(self, path_to_configuration_file) -> None:
        self.path_to_configuration_file = path_to_configuration_file

    def read_config(self) -> dict:
        with open(PurePath(self.path_to_configuration_file), "r") as config_file:
            config = yaml.safe_load(config_file)
        return config

def main():
    ReportManager().get_report()


if __name__ == "__main__":
    main()
