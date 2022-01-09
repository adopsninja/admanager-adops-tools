import logging
import yaml
from pathlib import PurePath

from constants import PLACEMENT_MANAGER_PATH, REPORT_MANAGER_PATH
from database import Database
from adops_ad_manager import AdOpsAdManagerClient
from placement_manager import PlacementManager
from report_manager import ReportManager

logger = logging.getLogger(__name__)


class AdOpsTools:
    def __init__(self) -> None:
        self.report_manager = ReportManager(REPORT_MANAGER_PATH)
        self.placement_manager = PlacementManager(PLACEMENT_MANAGER_PATH)

    def set_admanager_client(self, email=None, network_code=None):
        credentials = Database().get_credentials(email)
        return AdOpsAdManagerClient(*credentials, network_code)

    def update_performance_placements(self, publisher="Company Y") -> None:
        placement_config = self.placement_manager.config
        ad_manager_client = self.set_admanager_client(
            placement_config[publisher]["email"], 
            placement_config[publisher]["networkCode"]
        )
        report_path = self.report_manager.get_report(ad_manager_client, "placementPerformance")
        dataframe = self.placement_manager.clean_up_report(report_path)

        for placement in placement_config[publisher]["placements"]:
            ad_units = self.placement_manager.filter_ad_units(dataframe, placement)
            self.placement_manager.update_placement(ad_manager_client, placement["id"], ad_units)
