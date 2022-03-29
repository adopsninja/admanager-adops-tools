import logging

from googleads.ad_manager import AdManagerClient
from googleads.oauth2 import GoogleRefreshTokenClient
from googleads.ad_manager import StatementBuilder

from constants import API_VERSION
from database import Database

logger = logging.getLogger(__name__)

class AdOpsAdManagerClient:
    def __init__(self, email, network_code=None) -> None:
        self.email = email
        self.client = self.set_admanager_client(network_code)
        self.network_service = self.client.GetService("NetworkService", version=API_VERSION)
        self.placement_service = self.client.GetService("PlacementService", version=API_VERSION)
        self.inventory_service = self.client.GetService("InventoryService", version=API_VERSION)
        self.site_service = self.client.GetService("SiteService", version=API_VERSION)
        self.company_service = self.client.GetService("CompanyService", version=API_VERSION)
        self.report_downloader = self.client.GetDataDownloader(version=API_VERSION)

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
            all_networks = client.GetService("NetworkService", version=API_VERSION).getAllNetworks()
            print("Available Ad manager networks:")
            network_codes = {}
            for index, network in enumerate(all_networks):
                print(f"{index}: {network['networkCode']} {network['displayName']}")
                network_codes.update({f"{index}": network["networkCode"]})
            if not network_code:
                network_code = network_codes.get(input("Pick number: "))
                return AdManagerClient(refresh_token_client, credentials.app_name, network_code)

    def build_statement(self, key, value, limit=500, contains=False):
        statement = (
            StatementBuilder(version=API_VERSION)
            .OrderBy(f"{key}", ascending=True)
            .Limit(limit)
        )
        if isinstance(key, list):
            statement = (statement
            .Where(f"{key} IN (:{key})")
            .WithBindVariable(f"{key}", ",".join(value))
            )
        elif contains:
            statement = statement.Where(f"{key} LIKE '%{value}%'")
        else:
            statement = (statement
                .Where(f"{key} = :{key}")
                .WithBindVariable(f"{key}", value)
            )

        return statement
    
    def get_items_by_statement(self, statement: StatementBuilder, callback):
        logger.info(f"Statement query: {statement.ToStatement()['query']}")
        items = []
        while True:
            response = callback(statement.ToStatement())
            logger.debug(f"startIndex: {response['startIndex']}, totalResultSetSize: {response['totalResultSetSize']}")
            if "results" in response and response["results"]:
                items.extend(response["results"])
                statement.offset += statement.limit
            else:
                break

        return items
