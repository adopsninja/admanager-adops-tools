#!/usr/bin/env python
import datetime
import logging
from pathlib import PurePath

import yaml
from googleads.ad_manager import AdManagerClient, StatementBuilder
from googleads.oauth2 import GoogleRefreshTokenClient

from constants import API_VERSION, PLACEMENT_MANAGER_PATH, REPORT_MANAGER_PATH
from database import Database

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class AdOpsAdManagerClient:
    _API_VERSION = API_VERSION

    def __init__(self, application_name, client_id, client_secret, email, refresh_token, network_code=None) -> None:
        self.refresh_token_client = GoogleRefreshTokenClient(client_id, client_secret, refresh_token)
        self.client = AdManagerClient(self.refresh_token_client, application_name, network_code)
        self.network_service = self.client.GetService("NetworkService", version=self._API_VERSION)
        self.placement_service = self.client.GetService("PlacementService", version=self._API_VERSION)

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

    def update_config(self) -> None:
        if not all(key in self.config["report"]["placement"] for key in ["startDate", "endDate"]):
            date_range = {
                "startDate": datetime.date.today() - datetime.timedelta(1),
                "endDate": datetime.date.today() - datetime.timedelta(30),
            }
            self.config["report"]["placement"].update(date_range)

    def implement_client(self, client):
        if not client:
            credentials = Database().get_credentials(self.config["report"]["placement"]["email"])
            return AdOpsAdManagerClient(*credentials, self.config["report"]["placement"]["networkCode"])
        else:
            return client

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
    print(ReportManager())


if __name__ == "__main__":
    main()
