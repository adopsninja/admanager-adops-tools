#!/usr/bin/env python
import logging

from googleads.ad_manager import AdManagerClient
from googleads.oauth2 import GoogleRefreshTokenClient

from constants import API_VERSION, PLACEMENT_MANAGER_PATH, REPORT_MANAGER_PATH

logger = logging.getLogger(__name__)

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
