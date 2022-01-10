#!/usr/bin/env python3
import logging

from adops_tools import AdOpsTools

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("placement_manager").setLevel(logging.DEBUG)
logging.getLogger("report_manager").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    AdOpsTools().update_performance_placements("Company Y")
    AdOpsTools().update_performance_placements("Company A")
    AdOpsTools().update_performance_placements("Company E")
    AdOpsTools().update_performance_placements("Company I")
    AdOpsTools().update_performance_placements("Company p")

if __name__ == "__main__":
    main()
