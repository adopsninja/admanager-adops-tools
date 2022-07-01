#!/usr/bin/env python3
import logging
from typing import List
from adops_ad_manager import AdOpsAdManagerClient
from googleads.ad_manager import StatementBuilder

from constants import API_VERSION
from helpers import item_chunks
from notification_manager import mox_mcm_status_update


logger = logging.getLogger(__name__)

def update_line_item_targeting(order_list: List):
    client = AdOpsAdManagerClient("***REMOVED***", "***REMOVED***")
    statement = client.build_statement("orderId", order_list)
    li = []
    for order in order_list:
        li.extend(client.get_items_by_statement(statement, client.line_item_service.getLineItemsByStatement))
    
    for item in li:
        item["targeting"]["requestPlatformTargeting"] = {
            "targetedRequestPlatforms": [
                "MOBILE_APP"
            ]
        }
    
    chunk_nr = 1
    for items in item_chunks(li, 50):
        size = len(li)
        print(items[-1]["name"], items[-1]["id"])
        client.line_item_service.updateLineItems(items)
        print(f"Chunk nr {chunk_nr}, total_nr of chunks {int(size/50)}")
        chunk_nr += 1


if __name__ == "__main__":
    update_line_item_targeting(["3046438043", "3047416506", "3047417406", "3046434161", "3047410035"])
