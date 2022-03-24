#!/usr/bin/env python
import logging
import functools

from googleads.ad_manager import StatementBuilder
from googleads.errors import GoogleAdsServerFault

from adops_ad_manager import AdOpsAdManagerClient
from constants import API_VERSION, MCM_MANAGER_PATH
from config_reader import ConfigReader
from spreadsheet_manager import SpreadsheetDataframe, SpreadsheetManager

logger = logging.getLogger(__name__)


class MultipleCustomerManagement:
    def __init__(self, ad_manager: AdOpsAdManagerClient, spreadsheet_dataframe: SpreadsheetDataframe) -> None:
        self.ad_manager = ad_manager
        self.spreadsheet_dataframe = spreadsheet_dataframe

    def create_publishers(self, publishers: list):
        if not publishers:
            return None
        child_publishers = [{
            "name": publisher["publisher.name"],
            "email": publisher["publisher.email"],
            "type": "CHILD_PUBLISHER",
            "childPublisher": {
                "proposedDelegationType": "MANAGE_INVENTORY",
                "childNetworkCode": publisher["publisher.networkCode"],
            }
        } for publisher in publishers]
        child_publishers = self.ad_manager.company_service.createCompanies(child_publishers)

        return child_publishers

    def create_sites(self, sites: list):
        if not sites:
            return None
        sites = self.ad_manager.site_service.createSites([{
            "url": site["site.url"],
            "childNetworkCode": site["publisher.networkCode"]
        } for site in sites])

        return sites

    def submit_for_approval(self):
        statement = (
            StatementBuilder(version=API_VERSION)
            .Where("approvalStatus = 'DRAFT' AND childNetworkCode != ''")
            .OrderBy("url", ascending=True)
            .Limit(500)
        )
        sites = self.ad_manager.site_service.performSiteAction({"xsi_type": "SubmitSiteForApproval"}, statement.ToStatement())

        return sites

    def update_publishers(self, dataframe, *args, **kwargs):
        valid_publishers = self.spreadsheet_dataframe.valid_publishers(dataframe, *args, **kwargs)
        if kwargs.get("exists", True) == False:
            self.create_publishers(valid_publishers)

        valid_publishers = [publisher["publisher.name"] for publisher in valid_publishers]
        statement = self.ad_manager.build_statement('name', valid_publishers)
        status = self.ad_manager.get_items_by_statement(statement, self.ad_manager.company_service.getCompaniesByStatement)
        updated_dataframe = self.spreadsheet_dataframe.update_publishers(dataframe, status)

        return updated_dataframe

    def update_sites(self):
        pass

    def update_status(self, func, *args, **kwargs):
        dataframe = self.spreadsheet_dataframe.build_dataframe()
        updated_dataframe = func(dataframe, *args, **kwargs)

        updated_values = self.spreadsheet_dataframe.dataframe_to_list(updated_dataframe)
        self.spreadsheet_dataframe.spreadsheet_manager.write_values(updated_values)

    def update_mcm(self):
        self.update_status(self.update_publishers, exists=True)
        self.update_status(self.update_publishers, exists=False)


if __name__ == "__main__":
    config = ConfigReader(MCM_MANAGER_PATH).read_yaml_config()
    ad_manager = AdOpsAdManagerClient(
        config["test"]["email"],
        config["test"]["networkCode"]
    )
    spreadsheet_manager = SpreadsheetManager(
        config["test"]["email"],
        config["test"]["spreadsheetId"],
        config["test"]["sheetRange"]
    )
    spreadsheet_dataframe = SpreadsheetDataframe(spreadsheet_manager)
    mcm_manager = MultipleCustomerManagement(ad_manager, spreadsheet_dataframe)
    mcm_manager.update_mcm()
