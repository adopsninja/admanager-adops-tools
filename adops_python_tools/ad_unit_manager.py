#!/usr/bin/env python3
import logging

from googleads.ad_manager import StatementBuilder

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader
from constants import AD_UNIT_MANAGER_PATH

logger = logging.getLogger(__name__)

class AdUnitManager():
    def __init__(self) -> None:
        pass

    def log_archived_ad_units(self, path_to_log_file, archived_ad_units) -> None:
        with open(path_to_log_file, "a") as log_file:
            log_file.writelines(item + "\n" for item in archived_ad_units)

    def check_if_exist(self, client: AdOpsAdManagerClient) -> None:
        config_reader = ConfigReader(AD_UNIT_MANAGER_PATH)
        config = config_reader.read_yaml_config()
        archived = config_reader.read_txt_config(config["archived"])
        archived2 = config_reader.read_txt_config(config["archived2"])

        recheck = list(set(archived).symmetric_difference(set(archived2)))
        statement = (StatementBuilder(version=client._API_VERSION)
                    .Where(f"id IN ({','.join(recheck)})")
                    )
        while True:
            response = client.inventory_service.getAdUnitsByStatement(
                statement.ToStatement())
            if "results" in response and len(response["results"]):
                for item in response["results"]:
                    logger.info(f"{item['name']} {item['status']} {item['id']}")
                # self.log_archived_ad_units(config["archived"], [item["id"] for item in response["results"]])
                statement.offset += statement.limit
            else:
                break

    def ad_unit_status(self, client: AdOpsAdManagerClient) -> None:
        ad_units_archived = 0
        config_reader = ConfigReader(AD_UNIT_MANAGER_PATH)
        config = config_reader.read_yaml_config()

        statement = (StatementBuilder(version=client._API_VERSION).Where(f"status = 'ARCHIVED'"))
        while True:
            response = client.inventory_service.getAdUnitsByStatement(
                statement.ToStatement())
            if "results" in response and len(response["results"]):
                for item in response["results"]:
                    logger.info(f"{item['name']} {item['status']} {item['id']} {'/'.join(i['adUnitCode'] for i in item['parentPath'])}")
                self.log_archived_ad_units(config["archived"], [item["id"] for item in response["results"]])
                statement.offset += statement.limit
                if ad_units_archived > 0:
                    logger.info(f"Number of archived ad units: {len(response['results'])}")
            else:
                break

    def activate_ad_units(self, client: AdOpsAdManagerClient) -> None:
        ad_units_activated = 0
        config_reader = ConfigReader(AD_UNIT_MANAGER_PATH)
        config = ConfigReader(AD_UNIT_MANAGER_PATH).read_yaml_config()
        all_ad_units = config_reader.read_txt_config(config["toActivate"])
        ad_units_to_activate = sorted([item.strip() for item in list(set(all_ad_units))])
        logger.info(f"Number of all ad units to activate: {len(ad_units_to_activate)}")

        statement = (StatementBuilder(version=client._API_VERSION)
                    .Where(f"id IN ({','.join(ad_units_to_activate)})")
                    )
        while True:
            response = client.inventory_service.getAdUnitsByStatement(
                statement.ToStatement())
            if "results" in response and len(response["results"]):
                result = client.inventory_service.performAdUnitAction(
                    {"xsi_type": "ActivateAdUnits"}, statement.ToStatement())
                if result and int(result["numChanges"]) > 0:
                    ad_units_activated += int(result["numChanges"])
                statement.offset += statement.limit
            else:
                break

        if ad_units_activated > 0:
            logger.info(f"Number of ad units activated: {ad_units_activated}")
        else:
            logger.info("No ad units were activated.")

    def archive_ad_units(self, client: AdOpsAdManagerClient) -> None:
        ad_units_archived = 0
        config_reader = ConfigReader(AD_UNIT_MANAGER_PATH)
        config = ConfigReader(AD_UNIT_MANAGER_PATH).read_yaml_config()
        all_ad_units = config_reader.read_txt_config(config["allAdUnits"])

        active_ad_units = config_reader.read_txt_config(config["active"])
        ad_units_to_archive = sorted([item.strip() for item in list(set(all_ad_units).difference(active_ad_units))])
        
        logger.info(f"Number of all ad units: {len(all_ad_units)}")
        logger.info(f"Number of active ad units: {len(active_ad_units)}")
        logger.info(f"Number of ad units to archive: {len(ad_units_to_archive)}")

        statement = (StatementBuilder(version=client._API_VERSION)
                    .Where(f"id IN ({','.join(ad_units_to_archive)})")
                    )
        while True:
            response = client.inventory_service.getAdUnitsByStatement(
                statement.ToStatement())
            if "results" in response and len(response["results"]):
                self.log_archived_ad_units(config["archived"], [ad_unit['id'] for ad_unit in response["results"]])
                result = client.inventory_service.performAdUnitAction(
                    {"xsi_type": "ArchiveAdUnits"}, statement.ToStatement())
                if result and int(result["numChanges"]) > 0:
                    ad_units_archived += int(result["numChanges"])
                statement.offset += statement.limit
            else:
                break

        if ad_units_archived > 0:
            logger.info(f"Number of ad units archived: {ad_units_archived}")
        else:
            logger.info("No ad units were archived.")

    @staticmethod
    def chunks(list, number):
        for item in range(0, len(list), number):
            yield list[item : item + number]

if __name__ == "__main__":
    client = AdOpsAdManagerClient("dariusz.siudak***REMOVED***", "***REMOVED***")
    # AdUnitManager().archive_ad_units(client)
    # AdUnitManager().activate_ad_units(client)
    # AdUnitManager().ad_unit_status(client)
    AdUnitManager().check_if_exist(client)