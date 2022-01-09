#!/usr/bin/env python
import logging

from adops_tools import AdOpsTools

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

def main():
    AdOpsTools().update_performance_placements("Company Y")
    # AdOpsTools().update_performance_placements("Company E")
    # AdOpsTools().update_performance_placements("Company I")
    # AdOpsTools().update_performance_placements("Company p")

if __name__ == "__main__":
    main()
