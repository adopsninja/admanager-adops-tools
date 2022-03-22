#!/usr/bin/env python
import logging

from adops_ad_manager import AdOpsAdManagerClient
from googleads.ad_manager import StatementBuilder
from googleads.errors import GoogleAdsServerFault
from spreadsheet_manager import SpreadsheetDataframe, SpreadsheetManager

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

    
