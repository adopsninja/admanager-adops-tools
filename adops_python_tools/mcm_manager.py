#!/usr/bin/env python
import logging

import pandas as pd
from googleads.ad_manager import StatementBuilder
from googleads.errors import GoogleAdsServerFault

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader
from constants import API_VERSION, MCM_MANAGER_PATH
from spreadsheet_manager import SpreadsheetDataframe, SpreadsheetManager
from notification_manager import NotificationManager

logging.getLogger("googleads").setLevel(logging.WARNING)
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
        try:
            sites = self.ad_manager.site_service.performSiteAction(
                {"xsi_type": "SubmitSiteForApproval"}, 
                statement.ToStatement()
            )
            return sites
        except GoogleAdsServerFault as e:
            logger.error(f"Couldn't submit sites for approval. Error was: {e}")
            return None

    def update_publishers(self, dataframe, *args, **kwargs):
        valid_publishers = self.spreadsheet_dataframe.valid_publishers(dataframe, *args, **kwargs)
        if kwargs.get("exists", True) == False:
            self.create_publishers(valid_publishers)

        valid_publishers = [publisher["publisher.name"] for publisher in valid_publishers]
        statement = self.ad_manager.build_statement('name', valid_publishers)
        status = self.ad_manager.get_items_by_statement(statement, self.ad_manager.company_service.getCompaniesByStatement)

        return self.spreadsheet_dataframe.update_publishers(dataframe, status)

    def update_sites(self, dataframe, *args, **kwargs):
        valid_sites = self.spreadsheet_dataframe.valid_sites(dataframe, *args, **kwargs)
        if not valid_sites:
            return dataframe
        if kwargs.get("exists", True) == False:
            unique_sites = list({site["site.url"]:site for site in valid_sites}.values())
            self.create_sites(unique_sites)

        valid_sites = list(set([site["site.url"] for site in valid_sites]))
        statement = self.ad_manager.build_statement('url', valid_sites)
        status = self.ad_manager.get_items_by_statement(statement, self.ad_manager.site_service.getSitesByStatement)

        return self.spreadsheet_dataframe.update_sites(dataframe, status)

    def update_status(self, func, *args, **kwargs):
        dataframe = self.spreadsheet_dataframe.build_dataframe()
        updated_dataframe = func(dataframe, *args, **kwargs)

        updated_values = self.spreadsheet_dataframe.dataframe_to_list(updated_dataframe)
        self.spreadsheet_dataframe.spreadsheet_manager.write_values(updated_values)
        self.spreadsheet_dataframe.refresh_values()


    def update_mcm(self):
        self.update_status(self.update_publishers, exists=True)
        self.update_status(self.update_publishers, exists=False)
        logger.info("Publishers statuses checked.")
        self.update_status(self.update_sites, exists=True)
        self.update_status(self.update_sites, exists=False)
        logger.info("Domains statuses checked. Submit for approval.")
        self.submit_for_approval()
        self.update_status(self.update_sites, exists=True)
        logger.info("Update domains statuses after submission.")

    def status_change(self):
        before = self.spreadsheet_dataframe.site_status()
        self.update_mcm()
        after = self.spreadsheet_dataframe.site_status()
        result = self.spreadsheet_dataframe.compare_site_statuses(before, after)
        logger.info("Domains statuses before and after compared. Result:")

        if result.empty:
            logger.info("No status change for Company M sites.")
            return None

        logger.info(f"Status update:\n{result}")

        return result

def mox_mcm_status_update(env="test"):
    config = ConfigReader(MCM_MANAGER_PATH).read_yaml_config()
    logger.info("YAML configuration loaded.")
    ad_manager = AdOpsAdManagerClient(
        config[env]["email"],
        config[env]["networkCode"]
    )
    logger.info("AdOpsAdManagerClient loaded.")
    spreadsheet_manager = SpreadsheetManager(
        config[env]["email"],
        config[env]["spreadsheetId"],
        config[env]["sheetRange"]
    )
    spreadsheet_dataframe = SpreadsheetDataframe(spreadsheet_manager)
    logger.info("SpreadsheetManager and SpreadsheetDataframe loaded.")
    mcm_manager = MultipleCustomerManagement(ad_manager, spreadsheet_dataframe)
    logger.info("MultipleCustomerManagement loaded.")
    status_dataframe = mcm_manager.status_change()
    status_table = spreadsheet_dataframe.dataframe_to_html(status_dataframe)
    notification_manager = NotificationManager(config[env]["email"])
    logger.info("NotificationManager loaded.")
    message = notification_manager.mcm_notification_message(status_table)
    message = notification_manager.create_message(
        config[env]["notification"]["to"],
        config[env]["notification"]["sender"],
        config[env]["notification"]["subject"],
        message
    )
    notification_manager.send_message(message)
