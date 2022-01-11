import logging

from googleads.ad_manager import AdManagerClient
from googleads.oauth2 import GoogleRefreshTokenClient

from constants import API_VERSION
from database import Database

logger = logging.getLogger(__name__)

class AdOpsAdManagerClient:
    _API_VERSION = API_VERSION

    def __init__(self, email, network_code=None) -> None:
        self.email = email
        self.client = self.set_admanager_client(network_code)
        self.network_service = self.client.GetService("NetworkService", version=self._API_VERSION)
        self.placement_service = self.client.GetService("PlacementService", version=self._API_VERSION)
        self.report_downloader = self.client.GetDataDownloader(version=self._API_VERSION)

    def set_admanager_client(self, network_code: str=None) -> AdManagerClient:
        credentials = Database().get_credentials(self.email)
        refresh_token_client = GoogleRefreshTokenClient(
            credentials.client_id, 
            credentials.client_secret, 
            credentials.refresh_token
        )
        if network_code:
            return AdManagerClient(refresh_token_client, credentials.app_name, network_code)
        else:
            client = AdManagerClient(refresh_token_client, credentials.app_name)
            all_networks = client.GetService("NetworkService", version=self._API_VERSION).getAllNetworks()
            print("Available Ad manager networks:")
            network_codes = {}
            for index, network in enumerate(all_networks):
                print(f"{index}: {network['networkCode']} {network['displayName']}")
                network_codes.update({f"{index}": network["networkCode"]})
            if not network_code:
                network_code = network_codes.get(input("Pick number: "))
                return AdManagerClient(refresh_token_client, credentials.app_name, network_code)
