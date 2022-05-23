#!/usr/bin/env python3
import logging

from constants import PLACEMENT_MANAGER_PATH
from placement_manager import PlacementManager

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("adops_ad_manager").setLevel(logging.DEBUG)
logging.getLogger("placement_manager").setLevel(logging.DEBUG)
logging.getLogger("report_manager").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    PlacementManager(PLACEMENT_MANAGER_PATH).update_performance_placements("Company Y")
    PlacementManager(PLACEMENT_MANAGER_PATH).update_performance_placements("Company A")
    PlacementManager(PLACEMENT_MANAGER_PATH).update_performance_placements("Company E")
    PlacementManager(PLACEMENT_MANAGER_PATH).update_performance_placements("Company I")
    PlacementManager(PLACEMENT_MANAGER_PATH).update_performance_placements("Company p")

if __name__ == "__main__":
    main()
