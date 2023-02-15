import logging

from googleads import ad_manager

from adops_ad_manager import AdOpsAdManagerClient
from constants import API_VERSION
from helpers import item_chunks

logger = logging.getLogger(__name__)


def update_line_items(client, order_id):
    line_item_service = client.line_item_service

    statement = (
        ad_manager.StatementBuilder(version=API_VERSION)
        .Where(("orderId = :orderId"))
        .WithBindVariable("orderId", int(order_id))
        .Limit(500)
    )
    response = line_item_service.getLineItemsByStatement(statement.ToStatement())

    if "results" in response and len(response["results"]):
        updated_line_items = []
        for line_item in response["results"]:
            if not line_item["isArchived"]:
                line_item["creativePlaceholders"].extend(
                    [
                        {"size": {"width": 360, "height": 300}},
                        {"size": {"width": 345, "height": 345}},
                        {"size": {"width": 360, "height": 100}},
                        {"size": {"width": 336, "height": 250}},
                    ]
                )
                updated_line_items.append(line_item)

        for items in item_chunks(updated_line_items, 100):
            line_items = line_item_service.updateLineItems(items)

        if line_items:
            for line_item in line_items:
                print(
                    'Line item with id "%s", belonging to order id "%s", named '
                    '"%s"'
                    % (
                        line_item["id"],
                        line_item["orderId"],
                        line_item["name"],
                    )
                )
        else:
            print("No line items were updated.")
    else:
        print("No line items found to update.")


def update_licas(client, creative_id):
    lica_service = client.lica_service

    statement = (
        ad_manager.StatementBuilder(version=API_VERSION)
        .Where(("creativeId = :creativeId"))
        .WithBindVariable("creativeId", int(creative_id))
        .Limit(500)
    )

    while True:
        response = lica_service.getLineItemCreativeAssociationsByStatement(
            statement.ToStatement()
        )

        if "results" in response and len(response["results"]):
            updated_licas = []
            for lica in response["results"]:
                lica["sizes"].extend(
                    [
                        {"width": 360, "height": 300, "isAspectRatio": False},
                        {"width": 345, "height": 345, "isAspectRatio": False},
                        {"width": 360, "height": 100, "isAspectRatio": False},
                        {"width": 336, "height": 250, "isAspectRatio": False},
                    ]
                )
                updated_licas.append(lica)

            for items in item_chunks(updated_licas, 100):
                licas = lica_service.updateLineItemCreativeAssociations(items)

            for lica in licas:
                print(
                    'LICA with line item id "%s", creative id "%s", and status '
                    '"%s" was updated.'
                    % (lica["lineItemId"], lica["creativeId"], lica["status"])
                )
            statement.offset += statement.limit
        else:
            break

    if response["totalResultSetSize"] == 0:
        print("No LICAs found to update.")


if __name__ == "__main__":
    gam = AdOpsAdManagerClient("dariusz.siudak***REMOVED***", "***REMOVED***")
    for order in [
        3028495157,
        3028616815,
        3028620952,
        3028620436,
        3028496846,
        3029409492,
    ]:
        update_line_items(gam, order)
    for creative in [
        138392519164,
        138392519122,
        138392519125,
        138392519119,
        138392519134,
        138392519137,
        138392519131,
        138392519128,
    ]:
        update_licas(gam, creative)
