#!/usr/bin/env python
import logging

from googleads.ad_manager import AdManagerClient, StatementBuilder
from googleads.oauth2 import GoogleRefreshTokenClient

from database import Database
from constants import API_VERSION

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class AdOpsAdManagerClient:
    _API_VERSION = API_VERSION
    
    def __init__(self, application_name, client_id, client_secret, email, refresh_token, network_code=None,):
        self.refresh_token_client = GoogleRefreshTokenClient(client_id, client_secret, refresh_token)
        self.client = AdManagerClient(self.refresh_token_client, application_name, email, network_code)
        self.network_service = self.client.GetService("NetworkService", version=self.API_VERSION)

    def main(self):
        print(self.refresh_token_client)
        print(self.network_service.getAllNetworks())

def main():
    credentials = Database().get_credentials()
    print(AdOpsAdManagerClient(*credentials).main())


if __name__ == "__main__":
    main()