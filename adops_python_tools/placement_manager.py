import logging
from pathlib import PurePath
import re

import pandas as pd
from googleads import errors
from googleads.ad_manager import StatementBuilder

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader
from constants import API_VERSION, REPORT_MANAGER_PATH
from report_manager import ReportManager

logger = logging.getLogger(__name__)

class PlacementManager:
    def __init__(self, config_path) -> None:
        self.report_manager = ReportManager(REPORT_MANAGER_PATH)
        self.config_reader = ConfigReader(config_path)
        self.config = self.config_reader.read_yaml_config()

    def clean_up_report(self, report: PurePath) -> pd.DataFrame:
        dataframe = pd.read_csv(report, compression="gzip")
        dataframe["Column.AD_EXCHANGE_AD_REQUEST_ECPM"] /= 1000000
        dataframe["Column.AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE"] *= 100
        dataframe["Column.AD_EXCHANGE_AD_REQUEST_CTR"] *= 100
        dataframe["Dimension.AD_EXCHANGE_DFP_AD_UNIT_ID"] = dataframe["Dimension.AD_EXCHANGE_DFP_AD_UNIT_ID"].astype(str)

        return dataframe

    def filter_by_label_sign(self, dataframe: pd.DataFrame, pattern: str, is_positive: bool=True) -> pd.DataFrame:
        ad_unit_label = dataframe["Dimension.AD_EXCHANGE_DFP_AD_UNIT_ID"].str.contains(pattern, regex=True, flags=re.IGNORECASE, case=False)
        url_label = dataframe["Dimension.AD_EXCHANGE_URL"].str.contains(pattern, regex=True, flags=re.IGNORECASE, case=False)
        if is_positive:
            dataframe = dataframe.loc[(ad_unit_label) | (url_label)]
        else:
            dataframe = dataframe.loc[~((ad_unit_label) | (url_label))]
        return dataframe

    def filter_by_performance(self, dataframe: pd.DataFrame, config: dict) -> pd.DataFrame:
        dataframe = dataframe.loc[
            (dataframe[config["column"]] >= config["minn"])
            & (dataframe[config["column"]] <= config["maxn"])
            & (dataframe["Column.AD_EXCHANGE_AD_REQUESTS"] > config["minAdRequests"])
        ]
        return dataframe

    def filter_pattern(self, items: list) -> str:
        return "|".join(list(
            set(
                [item.split(".")[0].strip().lower() for item in items]
            )
        ))

    def filter_by_list_type(self, dataframe: pd.DataFrame, config: dict) -> pd.DataFrame:
        if "whitelist" in config:
            items = self.config_reader.read_txt_config(config["whitelist"])
            dataframe = self.filter_by_label_sign(dataframe, self.filter_pattern(items), True)
            logger.info("Number of ad units after placement whitelist check: %s", len(dataframe))
        if  "blacklist" in config:
            items = self.config_reader.read_txt_config(config["blacklist"])
            dataframe = self.filter_by_label_sign(dataframe, self.filter_pattern(items), False)
            logger.info("Number of ad units after placement blacklist check: %s", len(dataframe))
        return dataframe

    def filter_by_general_blacklist(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        blacklisted_ad_units = self.config_reader.read_txt_config(self.config["generalBlacklist"])
        dataframe = self.filter_by_label_sign(dataframe, self.filter_pattern(blacklisted_ad_units), False)
        logger.info("Number of ad units after general blacklist check: %s", len(dataframe))
        return dataframe

    def filter_ad_units(self, dataframe: pd.DataFrame, config: dict) -> list:
        dataframe = self.filter_by_performance(dataframe, config)
        dataframe = self.filter_by_list_type(dataframe, config)
        dataframe = self.filter_by_general_blacklist(dataframe)
        logger.info("Number of ad units after filtering: %s", len(dataframe))
        return list(set(dataframe["Dimension.AD_EXCHANGE_DFP_AD_UNIT_ID"]))

    def get_placement_by_id(self, client: AdOpsAdManagerClient, placement_id: str) -> dict:
        statement = (
            StatementBuilder(version=API_VERSION)
            .Where("id = :id")
            .OrderBy("id", ascending=True)
            .Limit(1)
            .WithBindVariable("id", placement_id)
        )
        logger.debug("Statement to retrieve placement: %s", statement.ToStatement())
        return statement.ToStatement()

    def update_placement(self, client: AdOpsAdManagerClient, placement_id: str, ad_unit_list: list) -> str:
        response = client.placement_service.getPlacementsByStatement(self.get_placement_by_id(client, placement_id))
        if "results" in response and len(response["results"]):
            placement = response["results"][0]
            placement["targetedAdUnitIds"] = ad_unit_list
            try:
                updated_placements = client.placement_service.updatePlacements([placement])
                for placement in updated_placements:
                    logger.info(f"Placement with id: 123456789zz{placement['id']} and name {placement['name']} was updated.")
            except errors.GoogleAdsServerFault as e:
                logger.error(
                    f"Placement {placement['name']} couldn't be updated because of {e.errors[0]['errorString']}."
                )

        return placement_id

    def update_performance_placements(self, publisher="Company Y") -> None:
        ad_manager_client = AdOpsAdManagerClient(
            self.config[publisher]["email"],
            self.config[publisher]["networkCode"]
            )
        report_path = self.report_manager.get_report(ad_manager_client, "placementPerformance")
        dataframe = self.clean_up_report(report_path)

        for placement in self.config[publisher]["placements"]:
            ad_units = self.filter_ad_units(dataframe, placement)
            self.update_placement(ad_manager_client, placement["id"], ad_units)
