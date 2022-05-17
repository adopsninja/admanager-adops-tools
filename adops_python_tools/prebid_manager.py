from distutils.command.config import config
import logging
from collections import namedtuple
from typing import Union, Dict, List
from pathlib import PurePath
from googleads.errors import GoogleAdsServerFault
import datetime

import pytz
import yaml

from adops_ad_manager import AdOpsAdManagerClient
from config_reader import ConfigReader
from helpers import item_chunks, random_id

logger = logging.getLogger(__name__)

class PrebidManager:
    def __init__(self, config_path: str) -> None:
        self.config = ConfigReader(config_path).read_yaml_config()

    def size_converter(self, sizes: str, service: str = "li") -> List[Dict]:
        Size = namedtuple("Size", ["width", "height"])
        creative_placeholders = sizes.split(";")
        creative_placeholders = [Size(size.split("x")[0], size.split("x")[1]) for size in creative_placeholders]

        if service == "licas":
            return [{"width": size.width, "height": size.height} for size in creative_placeholders]

        return [{"size": {"width": size.width, "height": size.height}} for size in creative_placeholders]

    def create_order(self, client: AdOpsAdManagerClient, start: float, step: float, ammount: int):
        advertiser_id = self.config["advertiserId"]
        user_id = client.user_service.getCurrentUser()["id"]
        order_object = {
            "name": f"{self.config.get('name')} {start + step:.2f} - {start + (step * ammount):.2f} {self.config.get('currency')}",
            "advertiserId": advertiser_id,
            "salespersonId": user_id,
            "traffickerId": user_id,
        }
        order = None
        try:
            order_list = client.order_service.createOrders(order_object)
            for order in order_list:
                logger.info(f"Order with name {order['name']} and id {order['id']} have been created")
        except GoogleAdsServerFault as GoogleError:
            for error in GoogleError.errors:
                if error["errorString"] == "UniqueError.NOT_UNIQUE":
                    logger.info(f"Order with name {error['trigger']} already exists")
                    statement = client.build_statement("name", error["trigger"], 1)
                    return next(iter(client.get_items_by_statement(statement, client.order_service.getOrdersByStatement)))["id"]
        else:
            return next(iter(order_list))["id"]


    def prepare_line_items(self, client: AdOpsAdManagerClient, start: float, step: float, ammount: int, order_id: int) -> List[Dict]:
        network = client.network_service.getCurrentNetwork()
        timezone = network["timeZone"]
        sdate = datetime.datetime.strptime("05/11/2019", "%d/%m/%Y")
        edate = datetime.datetime.strptime("05/11/2019", "%d/%m/%Y")

        existing_li_statement = client.build_statement("orderId", order_id)
        existing_li = client.get_items_by_statement(existing_li_statement, client.line_item_service.getLineItemsByStatement)
        existing_li = [li["name"] for li in existing_li]
        logger.info(f"Existing line items: ({len(existing_li)})")
        logger.debug(existing_li)
        key_values = self.get_key_values(client)

        cpm = start
        todo_line_items = []

        for _ in range(ammount):
            cpm = float(cpm)
            cpm += float(step)
            cpm = round(cpm, 2)
            line_item = {
                "orderId": order_id,
                "name": f"{cpm:.2f} {self.config.get('currency')} {self.config.get('name')}",
                "lineItemType": "PRICE_PRIORITY",
                "creativePlaceholders": self.size_converter(self.config.get("creativePlaceholders")),
                "targeting": {
                    "inventoryTargeting": {"targetedAdUnits": {"adUnitId": network["effectiveRootAdUnitId"]}},
                    "customTargeting": self.set_custom_targeting(key_values, f"{cpm:.2f}", self.config.get("hbFormat")),
                },
                "startDateTimeType": "IMMEDIATELY",
                "startDateTime": datetime.datetime(sdate.year, sdate.month, sdate.day, tzinfo=pytz.timezone(timezone)),
                "endDateTime": datetime.datetime(edate.year, edate.month, edate.day, hour=23, minute=59, tzinfo=pytz.timezone(timezone)),
                "unlimitedEndDateTime": True,
                "costType": "CPM",
                "costPerUnit": {"currencyCode": self.config.get("currency"), "microAmount": int(cpm * 1000000),},
                "primaryGoal": {"goalType": "NONE", "units": "100", "unitType": "IMPRESSIONS",},
                "creativeRotationType": "OPTIMIZED",
                "discountType": "PERCENTAGE",
                "allowOverbook": "true",
            }
            if "native" in self.config.get("hbFormat", ["banner", "video"]):
                line_item["creativePlaceholders"] = {
                    "size": {"width": "1", "height": "1"},
                    "creativeTemplateId": self.config.get("templateId"),
                    "creativeSizeType": "NATIVE",
                }

            cpm = f"{cpm:.2f}"
            todo_line_items.append(line_item)

        logger.info(f"Requested line items: ({len(todo_line_items)})")
        todo_line_items = [line_item for line_item in todo_line_items if line_item["name"] not in existing_li]
        logger.info(f"Line items to create: ({len(todo_line_items)})")

        if (li_ammount := len(existing_li) + len(todo_line_items)) > 450:
            raise Exception(
                f"Number of existing + requested = {li_ammount} line items exceeds maximum of 450 line items per order. Aborting."
            )
        
        return todo_line_items

    def create_line_items(self, client: AdOpsAdManagerClient, todo_line_items: List[Dict]) -> None:
        line_items_chunks = item_chunks(todo_line_items, 200)
        for chunk in line_items_chunks:
            attempts = 0
            while attempts < 5:
                try:
                    lis = client.line_item_service.createLineItems(chunk)
                    logger.info(f"Created line items: ({len(lis)})")
                    break
                except GoogleAdsServerFault as error:
                    attempts += 1
                    logger.error(error)
                    for e in error.errors:
                        if e["errorString"] == "UniqueError.NOT_UNIQUE":
                            print("UniqueError.NOT_UNIQUE")

    def creative_template(self, client: AdOpsAdManagerClient) -> List[int]:
        today = datetime.datetime.utcnow().strftime("%H%M%S_%d%m%Y")
        creatives = []
        for _ in range(8):
            creative = {
                "xsi_type": "TemplateCreative",
                "name": f"{self.config.get('name')} creative T-{today} ID-{random_id()}",
                "advertiserId": self.config.get("advertiserId"),
                "size": {"width": "1", "height": "1"},
                "isSafeFrameCompatible": False,
                "creativeTemplateId": self.config.get("templateId"),
            }
            if "native" in self.config.get("hbFormat", ["banner", "video"]):
                creative["destinationUrl"] = "https://dummy.pl"

            creatives.append(creative)

        creatives = client.creative_service.createCreatives(creatives)
        creative_ids = [creative["id"] for creative in creatives]
        logger.info(f"Created creatives: ({len(creative_ids)})")
        for creative in creatives:
            logger.debug(f"Creative {creative['name']} with id {creative['id']} has been created.")

        return creative_ids

    def create_creatives(self, client: AdOpsAdManagerClient, config_path: str) -> Dict:
        with open(PurePath(config_path), "r") as config_file:
            config = yaml.safe_load(config_file)

        with open(PurePath(config_path), "w") as config_file:
            if not config.get("creativeIds", []):
                config["creativeIds"] = self.creative_template(client)
            yaml.safe_dump(config, config_file)
            self.config = config

        return config

    def create_licas(self, client: AdOpsAdManagerClient, line_item_ids: List, creative_id: str):
        """ Associates creatives with line items. For given order.    
        """
        sizes = self.size_converter(self.config.get("creativePlaceholders"), "licas")
        statement = client.build_statement("creativeId", creative_id)
        existing_licas = [
            lica["lineItemId"]
            for lica in client.get_items_by_statement(statement, client.lica_service.getLineItemCreativeAssociationsByStatement)
        ]
        logger.info(f"Number of requested line items to associate with creative {creative_id}: ({len(line_item_ids)})")
        logger.info(f"Number of existing LICA for creative {creative_id}: ({len(existing_licas)})")

        line_item_ids = list(set(line_item_ids).difference(set(existing_licas)))
        logger.info(f"Number of line items to associate with creative {creative_id}: ({len(line_item_ids)})")

        line_item_ids = list(item_chunks(line_item_ids, 200))

        for line_item_chunked_id in line_item_ids:
            attempts = 0
            lica_ammount = 0
            while attempts < 2:
                try:
                    if "native" in self.config.get("hbFormat", ["banner", "video"]):
                        licas = [
                            {"creativeId": creative_id, "lineItemId": line_item} for line_item in line_item_chunked_id
                        ]
                    else:
                        licas = [
                            {"creativeId": creative_id, "lineItemId": line_item, "sizes": sizes,}
                            for line_item in line_item_chunked_id
                        ]
                    licas = client.lica_service.createLineItemCreativeAssociations(licas)
                    if licas:
                        lica_ammount += len(licas)
                        logger.info(f"Number of line items associated with creative {creative_id}: ({lica_ammount})")
                        for lica in licas:
                            logger.debug(
                                f'LICA with line item id {lica["lineItemId"]}, creative id {lica["creativeId"]} and status {lica["status"]} was created.'
                            )
                    break
                except GoogleAdsServerFault as error:
                    attempts += 1
                    logger.error(error)
            continue

    def set_custom_targeting(self, key_values: List[Dict], hb_pb: str, hb_format: List[str]) -> Dict:
        def filter_keys(key_name: str) -> Dict:
            return [key for key in key_values if key["name"] == key_name][0]

        hb_pb_key: Dict = filter_keys("hb_pb") 
        hb_format_key: Dict = filter_keys("hb_format")

        custom_targeting = {
            "xsi_type": "CustomCriteriaSet",
            "logicalOperator": "AND",
            "children": [{
                "xsi_type": "CustomCriteria",
                "keyId": hb_pb_key["id"],
                "valueIds": hb_pb_key["values"].get(hb_pb),
                "operator": "IS",
            },
            {
                "xsi_type": "CustomCriteria",
                "keyId": hb_format_key["id"],
                "valueIds": [hb_format_key["values"].get(key) for key in hb_format if key in hb_format_key["values"]],
                "operator": "IS",
            }],
        }

        return custom_targeting

    def get_key_values(self, client: AdOpsAdManagerClient) -> List[Dict]:
        key_values: list[str] = self.config.get("keyValues", ["hb_format", "hb_pb"])
        statement = client.build_statement("name", key_values)
        keys = client.get_items_by_statement(statement, client.custom_targeting_service.getCustomTargetingKeysByStatement)
        for key in keys:
            v_statement = client.build_statement("customTargetingKeyId", key["id"])
            key["values"] = client.get_items_by_statement(v_statement, client.custom_targeting_service.getCustomTargetingValuesByStatement)
            key["values"] = {value["name"]: value["id"] for value in key["values"]}
        print(keys[0])
        return keys

def build_prebid_setup(start: float, step: float, ammount: int) -> None:
    config_path = "/data/prebid_manager.yaml"
    prebid_manager = PrebidManager(config_path)
    client = AdOpsAdManagerClient(prebid_manager.config.get("email"), prebid_manager.config.get("networkCode"))
    prebid_manager.create_creatives(client, config_path)

    order_id = prebid_manager.create_order(client, start, step, ammount)
    todo_line_items = prebid_manager.prepare_line_items(client, start, step, ammount, order_id)
    prebid_manager.create_line_items(client, todo_line_items)
    statement = client.build_statement("orderId", order_id)
    line_item_ids = client.get_items_by_statement(statement, client.line_item_service.getLineItemsByStatement)
    line_item_ids = [item["id"] for item in line_item_ids]
    creative_ids = prebid_manager.config.get("creativeIds", [])

    for creative_id in creative_ids:
        prebid_manager.create_licas(client, line_item_ids, creative_id)

def main():
    build_prebid_setup(0.00, 0.01, 450)
    build_prebid_setup(4.50, 0.01, 450)
    build_prebid_setup(9.00, 0.01, 450)
    build_prebid_setup(13.50, 0.01, 450)
    build_prebid_setup(18.00, 0.01, 200)
    build_prebid_setup(20.00, 1.00, 80)

if __name__ == "__main__":
    main()
