import logging

import pandas as pd
from googleads import errors
from googleads.ad_manager import StatementBuilder

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader

logger = logging.getLogger(__name__)

class PlacementManager:
    def __init__(self, config_path) -> None:
        self.config = ConfigReader(config_path).read_yaml_config()

    def clean_up_report(self, report: str) -> pd.DataFrame:
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

    def get_placement_by_id(self, client: AdOpsAdManagerClient, placement_id: str) -> dict:
        statement = (
            StatementBuilder(version=client._API_VERSION)
            .Where("id = :id")
            .OrderBy("id", ascending=True)
            .Limit(1)
            .WithBindVariable("id", placement_id)
        )
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
                    f"Placement {placement['name']} couldn't be updated because of {e.errors[0]['reason']}."
                )

        return placement_id
