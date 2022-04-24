#!/usr/bin/env python3
import logging
from adops_ad_manager import AdOpsAdManagerClient
from googleads.ad_manager import StatementBuilder

from constants import API_VERSION


logger = logging.getLogger(__name__)

if __name__ == "__main__":
  pass
  # client = AdOpsAdManagerClient("dariusz.siudak***REMOVED***", "***REMOVED***")
  # statement = client.build_statement("id", "138342099954", 1, False)
  # statement = StatementBuilder(version=API_VERSION)
  # pub = client.get_adform_vast(statement, client.creative_service.getCreativesByStatement)
  # pub = client.get_items_by_statement(statement, client.creative_service.getCreativesByStatement)
  # print(pub)
  # print(type(pub[0]))
  # print(pub)