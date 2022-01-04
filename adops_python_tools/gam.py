#!/usr/bin/env python
import datetime
import logging
import os
import tempfile
from pathlib import PurePath

import pandas as pd
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
        self.report_manager = ReportManager(REPORT_MANAGER_PATH)
        self.placement_manager = PlacementManager(PLACEMENT_MANAGER_PATH)

    def set_admanager_client(self, email=None, network_code=None):
        credentials = Database().get_credentials(email)
        return AdOpsAdManagerClient(*credentials, network_code)

    def update_performance_placements(self, publisher="Company Y") -> None:
        placement_config = self.placement_manager.config
        ad_manager_client = self.set_admanager_client(placement_config[publisher]["email"], placement_config[publisher]["networkCode"])
        report_path = self.report_manager.get_report(ad_manager_client, "placementPerformance")
        dataframe = self.placement_manager.cleanup_report(report_path)

        for placement in placement_config[publisher]["placements"]:
            ad_units = self.placement_manager.filter_ad_units(dataframe, placement)
            self.placement_manager.update_placement(ad_manager_client, placement["id"], ad_units)


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
    def __init__(self, config_path) -> None:
        self.config = ConfigReader(config_path).read_config()

    def cleanup_report(self, report: str) -> pd.DataFrame:
        dataframe = pd.read_csv(report, compression="gzip")
        dataframe["Column.AD_EXCHANGE_AD_REQUEST_ECPM"] /= 1000000
        dataframe["Column.AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE"] *= 100
        dataframe["Column.AD_EXCHANGE_AD_REQUEST_CTR"] *= 100

        return dataframe

    def filter_ad_units(self, dataframe: pd.DataFrame, config: dict) -> list:
        dataframe = dataframe.loc[
            (dataframe[config["column"]] >= config["minn"])
            & (dataframe[config["column"]] <= config["maxn"])
            & (dataframe["Column.AD_EXCHANGE_AD_REQUESTS"] > config["minAdRequests"])
        ]
        return list(set(dataframe["Dimension.AD_EXCHANGE_DFP_AD_UNIT_ID"]))

    def update_placement(self, client, placement_id: str, ad_unit_list: list) -> str:
        statement = (
            StatementBuilder(version=client._API_VERSION)
            .Where("id = :id")
            .OrderBy("id", ascending=True)
            .Limit(1)
            .WithBindVariable("id", placement_id)
        )
        response = client.placement_service.getPlacementsByStatement(statement.ToStatement())
        if "results" in response and len(response["results"]):
            placement = response["results"][0]
            placement["targetedAdUnitIds"] = ad_unit_list
            try:
                updated_placements = client.placement_service.updatePlacements([placement])
                for placement in updated_placements:
                    logging.info(f"Placement with id: 123456789zz{placement['id']} and name {placement['name']} was updated.")
            except errors.GoogleAdsServerFault as e:
                logging.error(
                    f"Placement {placement['name']} couldn't be updated because of {e.errors[0]['reason']}."
                )

        return placement_id


class ReportManager:
    def __init__(self, config_path) -> None:
        self.config = ConfigReader(config_path).read_config()

    def set_report_job(self, report_type="placementPerformance") -> dict:
        if self.config[report_type]["dateRangeType"] == "CUSTOM_DATE":
            default_date_range = {
                "startDate": datetime.date.today() - datetime.timedelta(30),
                "endDate": datetime.date.today() - datetime.timedelta(1),
            }
            self.config[report_type].update(default_date_range)
        
        query = {
            "reportQuery": self.config[report_type]
        }
        return query

    def get_report(self, client, report_type="placementPerformance") -> str:
        output_path = PurePath(
            self.config["outputFolderPath"], datetime.datetime.now().strftime("%d%m%Y_%H%M")
        )
        self.create_directory(output_path)
        report_job = self.set_report_job(report_type)
        report_path = PurePath(
            output_path,
            f"{client.network_service.getCurrentNetwork()['networkCode']}_{report_type}_{self.config[report_type]['startDate']}_{self.config[report_type]['endDate']}.csv.gz",
        )

        try:
            report_job_id = client.report_downloader.WaitForReport(report_job)
            with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False, dir=PurePath(output_path),) as report_file:
                client.report_downloader.DownloadReportToFile(report_job_id, "CSV_DUMP", report_file)
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

    @staticmethod
    def create_directory(directory: str) -> str:
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
    AdOpsTools().update_performance_placements("Company E")
    AdOpsTools().update_performance_placements("Company I")
    AdOpsTools().update_performance_placements("Company p")

if __name__ == "__main__":
    main()
